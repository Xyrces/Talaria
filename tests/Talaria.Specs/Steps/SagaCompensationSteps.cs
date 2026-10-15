// SPDX-License-Identifier: Apache-2.0

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Reqnroll;
using Talaria.Core;
using Talaria.Core.Abstractions;
using Talaria.Core.Registration;
using Talaria.Core.Sagas;
using Talaria.Specs.Tests;
using Talaria.Transports.InMemory;

namespace Talaria.Specs.Steps;

[Binding]
public sealed class SagaCompensationSteps : IAsyncDisposable
{
    public sealed record OrderPlaced(string OrderId, string ReservationId);
    public sealed record PaymentSucceeded(string OrderId, string ChargeId);
    public sealed record ShipmentBookingFailed(string OrderId, string Reason);

    public sealed record ReleaseInventory(string OrderId, string ReservationId);
    public sealed record InventoryReleased(string OrderId);

    public sealed record RefundPayment(string OrderId, string ChargeId);
    public sealed record PaymentRefunded(string OrderId);

    public sealed record CancelShipment(string OrderId, string ShipmentId);
    public sealed record ShipmentCanceled(string OrderId);

    public sealed record OrderFailed(string OrderId, string Status);

    public sealed class FulfillmentSagaState : ICompensatableState
    {
        public string OrderId { get; set; } = string.Empty;
        public bool InventoryReserved { get; set; }
        public string? ReservationId { get; set; }
        public bool PaymentCharged { get; set; }
        public string? ChargeId { get; set; }
        public bool ShipmentBooked { get; set; }
        public string? ShipmentId { get; set; }
        public string Status { get; set; } = "Started";
        public SagaCompensationState Compensation { get; set; } = new();
    }

    private readonly InMemoryTransport _transport = new();
    private IHost? _host;
    private bool _hostStarted;

    private string ReleaseInventoryTopic => MessageNames.Destination(MessageNames.Contract(typeof(ReleaseInventory)));
    private string InventoryReleasedTopic => MessageNames.Destination(MessageNames.Contract(typeof(InventoryReleased)));
    private string RefundPaymentTopic => MessageNames.Destination(MessageNames.Contract(typeof(RefundPayment)));
    private string PaymentRefundedTopic => MessageNames.Destination(MessageNames.Contract(typeof(PaymentRefunded)));
    private const string OrderFailedTopic = "comp-bdd.orders.failed";

    [Given(@"an order fulfillment saga configured with sequential compensation")]
    public void GivenAnOrderFulfillmentSagaConfiguredWithSequentialCompensation()
    {
        ConfigureHost(batchMode: false, includeLatePaymentHook: false);
    }

    [Given(@"an order fulfillment saga configured with single-phase batch compensation")]
    public void GivenAnOrderFulfillmentSagaConfiguredWithSinglePhaseBatchCompensation()
    {
        ConfigureHost(batchMode: true, includeLatePaymentHook: false);
    }

    [Given(@"an order fulfillment saga configured with sequential compensation and late payment arrival handling")]
    public void GivenAnOrderFulfillmentSagaConfiguredWithSequentialCompensationAndLatePaymentArrivalHandling()
    {
        ConfigureHost(batchMode: false, includeLatePaymentHook: true);
    }

    private void ConfigureHost(bool batchMode, bool includeLatePaymentHook)
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Logging.ClearProviders();
        builder.Services.AddTalaria(opts =>
        {
            opts.ApplicationName = batchMode ? "saga-comp-bdd-batch" : "saga-comp-bdd-seq";
            opts.OutboxRelayInterval = TimeSpan.FromMilliseconds(15);
            opts.DeferralBackoff = TimeSpan.FromMilliseconds(20);
        })
        .UseInMemoryTransport(_transport)
        .UseInMemoryIdempotencyStore()
        .UseInMemoryDeferralStore()
        .UseInMemoryStateStore();

        _host = builder.Build();

        _host.Services.MapSaga<FulfillmentSagaState>(saga =>
        {
            if (batchMode)
            {
                saga.CompensateWith<ReleaseInventory>(
                        when: s => s.InventoryReserved,
                        create: s => new ReleaseInventory(s.OrderId, s.ReservationId!))
                    .CompensateWith<RefundPayment>(
                        when: s => s.PaymentCharged,
                        create: s => new RefundPayment(s.OrderId, s.ChargeId!))
                    .CompensateWith<CancelShipment>(
                        when: s => s.ShipmentBooked,
                        create: s => new CancelShipment(s.OrderId, s.ShipmentId!));
            }
            else
            {
                saga.CompensateStep<ReleaseInventory, InventoryReleased>(
                        when: s => s.InventoryReserved,
                        create: s => new ReleaseInventory(s.OrderId, s.ReservationId!),
                        onCompensated: (s, _) => s.InventoryReserved = false,
                        correlateBy: a => a.OrderId)
                    .CompensateStep<RefundPayment, PaymentRefunded>(
                        when: s => s.PaymentCharged,
                        create: s => new RefundPayment(s.OrderId, s.ChargeId!),
                        onCompensated: (s, _) => s.PaymentCharged = false,
                        correlateBy: a => a.OrderId)
                    .CompensateStep<CancelShipment, ShipmentCanceled>(
                        when: s => s.ShipmentBooked,
                        create: s => new CancelShipment(s.OrderId, s.ShipmentId!),
                        onCompensated: (s, _) => s.ShipmentBooked = false,
                        correlateBy: a => a.OrderId)
                    .OnCompensationCompleted((state, ctx) =>
                    {
                        state.Status = "Compensated";
                        return Task.FromResult(ctx
                            .Dispatch(new OrderFailed(state.OrderId, "Compensated"))
                            .Complete());
                    });

                if (includeLatePaymentHook)
                {
                    saga.OnLateArrival<PaymentSucceeded>((state, msg) =>
                    {
                        state.PaymentCharged = true;
                        state.ChargeId = msg.ChargeId;
                    });
                }
            }

            saga.DispatchTo<OrderFailed>(OrderFailedTopic)
                .StartedBy<OrderPlaced>(
                    "comp-bdd.orders.placed",
                    (msg, ctx) => Task.FromResult(ctx.Transition(new FulfillmentSagaState
                    {
                        OrderId = msg.OrderId,
                        InventoryReserved = true,
                        ReservationId = msg.ReservationId,
                        Status = "InventoryReserved"
                    })),
                    correlateBy: m => m.OrderId)
                .On<PaymentSucceeded>(
                    "comp-bdd.payments.succeeded",
                    (state, msg, ctx) =>
                    {
                        state.PaymentCharged = true;
                        state.ChargeId = msg.ChargeId;
                        state.Status = "PaymentCharged";
                        return Task.FromResult(ctx.Transition(state));
                    },
                    correlateBy: m => m.OrderId)
                .On<ShipmentBookingFailed>(
                    "comp-bdd.shipping.failed",
                    (state, _, ctx) =>
                    {
                        state.Status = "Compensating";
                        if (batchMode)
                        {
                            return Task.FromResult(ctx
                                .Dispatch(new OrderFailed(state.OrderId, "Compensated"))
                                .CompensateAndComplete(state));
                        }

                        return Task.FromResult(ctx.Compensate(state));
                    },
                    correlateBy: m => m.OrderId);
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

    [When(@"order ""([^""]*)"" reserves inventory ""([^""]*)"" and charges payment ""([^""]*)""")]
    public async Task WhenOrderReservesInventoryAndChargesPayment(string orderId, string reservationId, string chargeId)
    {
        await EnsureHostStartedAsync();
        var store = _host!.Services.GetRequiredService<IStateStore<FulfillmentSagaState>>();

        await using var startProducer = await _transport.CreateProducerAsync<OrderPlaced>("comp-bdd.orders.placed", new ProducerOptions());
        await startProducer.ProduceAsync(new OrderPlaced(orderId, reservationId));

        Assert.True(await TestAsyncHelpers.PollUntilAsync(
            async () => (await store.GetAsync(orderId))?.InventoryReserved == true,
            TimeSpan.FromSeconds(5)));

        await using var payProducer = await _transport.CreateProducerAsync<PaymentSucceeded>("comp-bdd.payments.succeeded", new ProducerOptions());
        await payProducer.ProduceAsync(new PaymentSucceeded(orderId, chargeId));

        Assert.True(await TestAsyncHelpers.PollUntilAsync(
            async () => (await store.GetAsync(orderId))?.PaymentCharged == true,
            TimeSpan.FromSeconds(5)));
    }

    [When(@"order ""([^""]*)"" reserves inventory ""([^""]*)""")]
    public async Task WhenOrderReservesInventory(string orderId, string reservationId)
    {
        await EnsureHostStartedAsync();
        var store = _host!.Services.GetRequiredService<IStateStore<FulfillmentSagaState>>();

        await using var startProducer = await _transport.CreateProducerAsync<OrderPlaced>("comp-bdd.orders.placed", new ProducerOptions());
        await startProducer.ProduceAsync(new OrderPlaced(orderId, reservationId));

        Assert.True(await TestAsyncHelpers.PollUntilAsync(
            async () => (await store.GetAsync(orderId))?.InventoryReserved == true,
            TimeSpan.FromSeconds(5)));
    }

    [When(@"shipment booking fails for order ""([^""]*)""")]
    [When(@"shipment booking fails triggering batch compensation for order ""([^""]*)""")]
    public async Task WhenShipmentBookingFailsForOrder(string orderId)
    {
        await EnsureHostStartedAsync();
        await using var failProducer = await _transport.CreateProducerAsync<ShipmentBookingFailed>("comp-bdd.shipping.failed", new ProducerOptions());
        await failProducer.ProduceAsync(new ShipmentBookingFailed(orderId, "NoCarrierCapacity"));
    }

    [When(@"a late ""PaymentSucceeded"" event with charge ""([^""]*)"" arrives for order ""([^""]*)"" while compensating")]
    public async Task WhenALatePaymentSucceededEventArrivesWhileCompensating(string chargeId, string orderId)
    {
        await EnsureHostStartedAsync();
        var store = _host!.Services.GetRequiredService<IStateStore<FulfillmentSagaState>>();

        await using var payProducer = await _transport.CreateProducerAsync<PaymentSucceeded>("comp-bdd.payments.succeeded", new ProducerOptions());
        await payProducer.ProduceAsync(new PaymentSucceeded(orderId, chargeId));

        Assert.True(await TestAsyncHelpers.PollUntilAsync(
            async () => (await store.GetAsync(orderId))?.PaymentCharged == true,
            TimeSpan.FromSeconds(5)));
    }

    [When(@"""PaymentRefunded"" acknowledgment arrives for order ""([^""]*)""")]
    public async Task WhenPaymentRefundedAcknowledgmentArrives(string orderId)
    {
        await EnsureHostStartedAsync();
        await using var ackProducer = await _transport.CreateProducerAsync<PaymentRefunded>(PaymentRefundedTopic, new ProducerOptions());
        await ackProducer.ProduceAsync(new PaymentRefunded(orderId));
    }

    [When(@"""InventoryReleased"" acknowledgment arrives for order ""([^""]*)""")]
    public async Task WhenInventoryReleasedAcknowledgmentArrives(string orderId)
    {
        await EnsureHostStartedAsync();
        await using var ackProducer = await _transport.CreateProducerAsync<InventoryReleased>(InventoryReleasedTopic, new ProducerOptions());
        await ackProducer.ProduceAsync(new InventoryReleased(orderId));
    }

    [Then(@"only ""RefundPayment"" should have been dispatched for order ""([^""]*)""")]
    [Then(@"""RefundPayment"" should be dispatched for order ""([^""]*)""")]
    public async Task ThenRefundPaymentShouldBeDispatched(string orderId)
    {
        var dispatched = await TestAsyncHelpers.PollUntilAsync(
            async () => (await _transport.ReadAllFromTopicAsync<RefundPayment>(RefundPaymentTopic)).Any(m => m.Payload.OrderId == orderId),
            TimeSpan.FromSeconds(5));
        Assert.True(dispatched, $"Expected RefundPayment to be dispatched for '{orderId}'.");
    }

    [Then(@"only ""ReleaseInventory"" should have been dispatched for order ""([^""]*)""")]
    [Then(@"""ReleaseInventory"" should be dispatched for order ""([^""]*)""")]
    public async Task ThenReleaseInventoryShouldBeDispatched(string orderId)
    {
        var dispatched = await TestAsyncHelpers.PollUntilAsync(
            async () => (await _transport.ReadAllFromTopicAsync<ReleaseInventory>(ReleaseInventoryTopic)).Any(m => m.Payload.OrderId == orderId),
            TimeSpan.FromSeconds(5));
        Assert.True(dispatched, $"Expected ReleaseInventory to be dispatched for '{orderId}'.");
    }

    [Then(@"""ReleaseInventory"" should not yet be dispatched for order ""([^""]*)""")]
    public async Task ThenReleaseInventoryShouldNotYetBeDispatched(string orderId)
    {
        var messages = await _transport.ReadAllFromTopicAsync<ReleaseInventory>(ReleaseInventoryTopic);
        Assert.DoesNotContain(messages, m => m.Payload.OrderId == orderId);
    }

    [Then(@"""OrderFailed"" should not yet be published for order ""([^""]*)""")]
    public async Task ThenOrderFailedShouldNotYetBePublished(string orderId)
    {
        var messages = await _transport.ReadAllFromTopicAsync<OrderFailed>(OrderFailedTopic);
        Assert.DoesNotContain(messages, m => m.Payload.OrderId == orderId);
    }

    [Then(@"""OrderFailed"" should be published for order ""([^""]*)""")]
    public async Task ThenOrderFailedShouldBePublished(string orderId)
    {
        var published = await TestAsyncHelpers.PollUntilAsync(
            async () => (await _transport.ReadAllFromTopicAsync<OrderFailed>(OrderFailedTopic)).Any(m => m.Payload.OrderId == orderId),
            TimeSpan.FromSeconds(5));
        Assert.True(published, $"Expected OrderFailed to be published for '{orderId}'.");
    }

    [Then(@"both ""RefundPayment"" and ""ReleaseInventory"" and ""OrderFailed"" should be dispatched for order ""([^""]*)""")]
    public async Task ThenBothRefundAndReleaseAndOrderFailedShouldBeDispatched(string orderId)
    {
        var allDispatched = await TestAsyncHelpers.PollUntilAsync(async () =>
        {
            var refunds = await _transport.ReadAllFromTopicAsync<RefundPayment>(RefundPaymentTopic);
            var releases = await _transport.ReadAllFromTopicAsync<ReleaseInventory>(ReleaseInventoryTopic);
            var failed = await _transport.ReadAllFromTopicAsync<OrderFailed>(OrderFailedTopic);
            return refunds.Any(m => m.Payload.OrderId == orderId)
                && releases.Any(m => m.Payload.OrderId == orderId)
                && failed.Any(m => m.Payload.OrderId == orderId);
        }, TimeSpan.FromSeconds(5));
        Assert.True(allDispatched, $"Expected RefundPayment, ReleaseInventory, and OrderFailed to be dispatched for '{orderId}'.");
    }

    [Then(@"the order fulfillment saga state for ""([^""]*)"" should be completed and purged")]
    public async Task ThenTheOrderFulfillmentSagaStateShouldBeCompletedAndPurged(string orderId)
    {
        var store = _host!.Services.GetRequiredService<IStateStore<FulfillmentSagaState>>();
        var completed = await TestAsyncHelpers.PollUntilAsync(
            async () => await store.GetAsync(orderId) is null,
            TimeSpan.FromSeconds(5));
        Assert.True(completed, $"Expected saga state for '{orderId}' to be completed and purged.");
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
