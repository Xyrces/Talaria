// SPDX-License-Identifier: Apache-2.0

using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Reqnroll;
using Talaria.Core;
using Talaria.Core.Abstractions;
using Talaria.Core.Registration;
using Talaria.Specs.Tests;
using Talaria.Transports.InMemory;

namespace Talaria.Specs.Steps;

[Binding]
public sealed class SagaTimeoutSteps : IAsyncDisposable
{
    public sealed record StartOrderPayment(string OrderId);
    public sealed record CompleteOrderPayment(string OrderId);
    public sealed record AuthorizeOrderPayment(string OrderId);
    public sealed record OrderPaymentTimeoutExpired(string CorrelationId);

    public sealed class PaymentSagaState
    {
        public string OrderId { get; set; } = string.Empty;
        public string Status { get; set; } = string.Empty;
    }

    private readonly InMemoryTransport _transport = new();
    private IHost? _host;
    private bool _hostStarted;
    private readonly ConcurrentQueue<string> _timeoutInvocations = new();

    [Given(@"a payment saga configured with a (\d+) ms payment timeout")]
    public void GivenAPaymentSagaConfiguredWithAPaymentTimeout(int timeoutMs)
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Logging.ClearProviders();
        builder.Services.AddTalaria(opts =>
        {
            opts.ApplicationName = "saga-timeout-bdd-app";
            opts.OutboxRelayInterval = TimeSpan.FromMilliseconds(20);
            opts.DeferralBackoff = TimeSpan.FromMilliseconds(20);
        })
        .UseInMemoryTransport(_transport)
        .UseInMemoryIdempotencyStore()
        .UseInMemoryDeferralStore()
        .UseInMemoryStateStore();

        _host = builder.Build();

        _host.Services.MapSaga<PaymentSagaState>(saga =>
        {
            saga.StartedBy<StartOrderPayment>(
                "payment-saga.start",
                correlateBy: m => m.OrderId,
                handler: (msg, ctx) =>
                {
                    ctx.RequestTimeout(
                        new OrderPaymentTimeoutExpired(msg.OrderId),
                        TimeSpan.FromMilliseconds(timeoutMs));
                    return Task.FromResult(ctx.Transition(new PaymentSagaState
                    {
                        OrderId = msg.OrderId,
                        Status = "AwaitingPayment"
                    }));
                });

            saga.On<CompleteOrderPayment>(
                "payment-saga.complete",
                correlateBy: m => m.OrderId,
                handler: (_, _, ctx) => Task.FromResult(ctx.Complete()));

            saga.On<AuthorizeOrderPayment>(
                "payment-saga.authorize",
                correlateBy: m => m.OrderId,
                handler: (state, _, ctx) =>
                {
                    state.Status = "PaymentAuthorized";
                    ctx.CancelTimeout<OrderPaymentTimeoutExpired>();
                    return Task.FromResult(ctx.Transition(state));
                });

            saga.OnTimeout<OrderPaymentTimeoutExpired>(
                correlateBy: m => m.CorrelationId,
                handler: (_, timeout, ctx) =>
                {
                    _timeoutInvocations.Enqueue(timeout.CorrelationId);
                    return Task.FromResult(ctx.Complete());
                });
        });
    }

    private async Task EnsureHostStartedAsync()
    {
        if (!_hostStarted && _host is not null)
        {
            await _host.StartAsync();
            _hostStarted = true;
        }
    }

    [When(@"an order ""([^""]*)"" starts the payment saga")]
    public async Task WhenAnOrderStartsThePaymentSaga(string orderId)
    {
        await EnsureHostStartedAsync();
        await using var producer = await _transport.CreateProducerAsync<StartOrderPayment>("payment-saga.start", new ProducerOptions());
        await producer.ProduceAsync(new StartOrderPayment(orderId));

        var store = _host!.Services.GetRequiredService<IStateStore<PaymentSagaState>>();
        var created = await TestAsyncHelpers.PollUntilAsync(
            async () => await store.GetAsync(orderId) is not null,
            TimeSpan.FromSeconds(5));
        Assert.True(created, $"Expected payment saga state for '{orderId}' to be created.");
    }

    [When(@"payment is completed for order ""([^""]*)"" before the timeout is due")]
    public async Task WhenPaymentIsCompletedForOrderBeforeTheTimeoutIsDue(string orderId)
    {
        await EnsureHostStartedAsync();
        await using var producer = await _transport.CreateProducerAsync<CompleteOrderPayment>("payment-saga.complete", new ProducerOptions());
        await producer.ProduceAsync(new CompleteOrderPayment(orderId));
    }

    [When(@"payment is authorized for order ""([^""]*)"" cancelling the payment timeout")]
    public async Task WhenPaymentIsAuthorizedForOrderCancellingThePaymentTimeout(string orderId)
    {
        await EnsureHostStartedAsync();
        await using var producer = await _transport.CreateProducerAsync<AuthorizeOrderPayment>("payment-saga.authorize", new ProducerOptions());
        await producer.ProduceAsync(new AuthorizeOrderPayment(orderId));
    }

    [Then(@"the payment saga for ""([^""]*)"" should be in state ""([^""]*)""")]
    [Then(@"the payment saga for ""([^""]*)"" should remain in state ""([^""]*)""")]
    public async Task ThenThePaymentSagaForShouldBeInState(string orderId, string expectedStatus)
    {
        var store = _host!.Services.GetRequiredService<IStateStore<PaymentSagaState>>();
        var matched = await TestAsyncHelpers.PollUntilAsync(
            async () => (await store.GetAsync(orderId))?.Status == expectedStatus,
            TimeSpan.FromSeconds(5));
        Assert.True(matched, $"Expected payment saga '{orderId}' to have status '{expectedStatus}'.");
    }

    [Then(@"once the payment timeout is due the saga for ""([^""]*)"" should be completed")]
    [Then(@"the payment saga for ""([^""]*)"" should be completed")]
    public async Task ThenTheSagaForShouldBeCompleted(string orderId)
    {
        var store = _host!.Services.GetRequiredService<IStateStore<PaymentSagaState>>();
        var completed = await TestAsyncHelpers.PollUntilAsync(
            async () => await store.GetAsync(orderId) is null,
            TimeSpan.FromSeconds(5));
        Assert.True(completed, $"Expected payment saga '{orderId}' to complete and purge its state.");
    }

    [Then(@"a payment timeout notification for ""([^""]*)"" should have been recorded")]
    public async Task ThenAPaymentTimeoutNotificationShouldHaveBeenRecorded(string orderId)
    {
        var recorded = await TestAsyncHelpers.PollUntilAsync(
            () => Task.FromResult(_timeoutInvocations.Contains(orderId)),
            TimeSpan.FromSeconds(5));
        Assert.True(recorded, $"Expected timeout handler to be invoked for '{orderId}'.");
    }

    [Then(@"waiting (\d+) ms past the timeout deadline should not invoke the timeout handler for ""([^""]*)""")]
    public async Task ThenWaitingPastTheTimeoutDeadlineShouldNotInvokeTheTimeoutHandler(int waitMs, string orderId)
    {
        var stayedQuiet = await TestAsyncHelpers.PollStableAsync(
            () => Task.FromResult(!_timeoutInvocations.Contains(orderId)),
            TimeSpan.FromMilliseconds(waitMs));
        Assert.True(stayedQuiet, $"Expected timeout handler for '{orderId}' to be suppressed, but it ran.");
    }

    public async ValueTask DisposeAsync()
    {
        if (_host is not null)
        {
            try { await _host.StopAsync(TimeSpan.FromSeconds(2)); } catch { }
            _host.Dispose();
        }
    }
}
