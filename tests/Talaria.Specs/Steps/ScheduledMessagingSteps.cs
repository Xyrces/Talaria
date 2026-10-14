// SPDX-License-Identifier: Apache-2.0

using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Reqnroll;
using Talaria.Core;
using Talaria.Core.Abstractions;
using Talaria.Core.Registration;
using Talaria.Specs.Messages;
using Talaria.Specs.Tests;
using Talaria.Transports.InMemory;

namespace Talaria.Specs.Steps;

[Binding]
public sealed class ScheduledMessagingSteps : IAsyncDisposable
{
    private readonly InMemoryTransport _transport = new();
    private IHost? _host;
    private bool _hostStarted;
    private bool _mapShipOrderCommand;
    private bool _mapOrderPlacedEvent;
    private readonly ConcurrentQueue<ShipOrder> _receivedCommands = new();
    private readonly ConcurrentQueue<OrderPlaced> _receivedEvents = new();

    [Given(@"a Talaria scheduled messaging host with an in-memory deferral store")]
    public void GivenATalariaScheduledMessagingHostWithAnInMemoryDeferralStore()
    {
        // Host configuration deferred until handlers are known.
    }

    [Given(@"a command handler registered for ShipOrder")]
    public void GivenACommandHandlerRegisteredForShipOrder()
    {
        _mapShipOrderCommand = true;
    }

    [Given(@"an event handler registered for OrderPlaced")]
    public void GivenAnEventHandlerRegisteredForOrderPlaced()
    {
        _mapOrderPlacedEvent = true;
    }

    private async Task EnsureHostStartedAsync()
    {
        if (_host is null)
        {
            var builder = Host.CreateApplicationBuilder();
            builder.Logging.ClearProviders();
            builder.Services.AddTalaria(opts =>
            {
                opts.ApplicationName = "scheduled-bdd-app";
                opts.DeferralBackoff = TimeSpan.FromMilliseconds(20);
                opts.OutboxRelayInterval = TimeSpan.FromMilliseconds(20);
            })
            .UseInMemoryTransport(_transport)
            .UseInMemoryIdempotencyStore()
            .UseInMemoryDeferralStore();

            _host = builder.Build();

            if (_mapShipOrderCommand)
            {
                _host.MapCommand<ShipOrder>((ShipOrder cmd) =>
                {
                    _receivedCommands.Enqueue(cmd);
                    return Task.CompletedTask;
                });
            }

            if (_mapOrderPlacedEvent)
            {
                _host.MapEvent<OrderPlaced>((OrderPlaced evt) =>
                {
                    _receivedEvents.Enqueue(evt);
                    return Task.CompletedTask;
                });
            }
        }

        if (!_hostStarted)
        {
            await _host.StartAsync();
            _hostStarted = true;
        }
    }

    [When(@"I schedule a ShipOrder command for order ""(.*)"" with a delay of (\d+) ms")]
    public async Task WhenIScheduleAShipOrderCommandWithADelay(string orderId, int delayMs)
    {
        await EnsureHostStartedAsync();
        await using var scope = _host!.Services.CreateAsyncScope();
        var bus = scope.ServiceProvider.GetRequiredService<IMessageBus>();
        await bus.ScheduleSendAsync(new ShipOrder(orderId), TimeSpan.FromMilliseconds(delayMs));
    }

    [When(@"I schedule an OrderPlaced event for order ""(.*)"" due in (\d+) ms")]
    public async Task WhenIScheduleAnOrderPlacedEventDueIn(string orderId, int delayMs)
    {
        await EnsureHostStartedAsync();
        await using var scope = _host!.Services.CreateAsyncScope();
        var bus = scope.ServiceProvider.GetRequiredService<IMessageBus>();
        var dueAt = DateTimeOffset.UtcNow.AddMilliseconds(delayMs);
        await bus.SchedulePublishAsync(new OrderPlaced(orderId, 42.0m), dueAt);
    }

    [Then(@"the ShipOrder command handler should not have received order ""(.*)"" within (\d+) ms")]
    public async Task ThenTheShipOrderCommandHandlerShouldNotHaveReceivedOrderWithin(string orderId, int windowMs)
    {
        var stayedEmpty = await TestAsyncHelpers.PollStableAsync(
            () => Task.FromResult(!_receivedCommands.Any(c => c.OrderId == orderId)),
            TimeSpan.FromMilliseconds(windowMs));
        Assert.True(stayedEmpty, $"Expected ShipOrder '{orderId}' not to arrive within {windowMs} ms.");
    }

    [Then(@"the ShipOrder command handler should receive order ""(.*)"" once due")]
    public async Task ThenTheShipOrderCommandHandlerShouldReceiveOrderOnceDue(string orderId)
    {
        var arrived = await TestAsyncHelpers.PollUntilAsync(
            () => Task.FromResult(_receivedCommands.Any(c => c.OrderId == orderId)),
            TimeSpan.FromSeconds(5));
        Assert.True(arrived, $"Expected ShipOrder '{orderId}' to be delivered once due.");
    }

    [Then(@"the OrderPlaced event handler should receive order ""(.*)"" once due")]
    public async Task ThenTheOrderPlacedEventHandlerShouldReceiveOrderOnceDue(string orderId)
    {
        var arrived = await TestAsyncHelpers.PollUntilAsync(
            () => Task.FromResult(_receivedEvents.Any(e => e.OrderId == orderId)),
            TimeSpan.FromSeconds(5));
        Assert.True(arrived, $"Expected OrderPlaced '{orderId}' to be delivered once due.");
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
