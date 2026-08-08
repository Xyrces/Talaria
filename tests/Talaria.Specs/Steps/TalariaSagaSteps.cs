using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Reqnroll;
using Talaria.Core;
using Talaria.Core.Registration;
using Talaria.Core.Abstractions;
using Talaria.Specs.Messages;
using Talaria.Transports.InMemory;

namespace Talaria.Specs.Steps;

[Binding]
public class TalariaSagaSteps : IAsyncDisposable
{
    private InMemoryTransport _transport = new();
    private IHost? _host;
    private bool _hostStarted;
    
    [Given(@"a configured saga ""OrderSaga""")]
    public void GivenAConfiguredSaga()
    {
        var builder = Host.CreateDefaultBuilder();
        builder.ConfigureServices(services =>
        {
            services.AddTalaria(opts =>
            {
                opts.MaxHopCount = 32;
                opts.ApplicationName = "test-saga-app";
                opts.MaxDeferralAttempts = 10;
                opts.DeferralBackoff = TimeSpan.FromMilliseconds(50);
            }).UseInMemoryTransport(_transport)
              .UseInMemoryDeferralStore();

        });

        _host = builder.Build();

        _host.Services.MapSaga<OrderSagaState>(saga =>
        {
            // Starter
            saga.StartedBy<OrderPlacedSaga>(
                "order-placed",
                correlateBy: m => m.CorrelationId,
                handler: async (msg, ctx) =>
                {
                    var state = new OrderSagaState { Id = msg.CorrelationId, Placed = true };
                    if (state.Billed)
                    {
                        ctx.Dispatch(new OrderCompletedSaga { Id = state.Id });
                        return ctx.Complete();
                    }
                    return ctx.Transition(state);
                });

            // Transition
            saga.On<OrderBilledSaga>(
                "order-billed",
                correlateBy: m => m.OrderId,
                handler: async (state, msg, ctx) =>
                {
                    state.Billed = true;
                    if (state.Placed)
                    {
                        ctx.Dispatch(new OrderCompletedSaga { Id = state.Id });
                        return ctx.Complete();
                    }
                    return ctx.Transition(state);
                });

            // Explicit dispatch route (was previously derived from the CLR type name).
            saga.DispatchTo<OrderCompletedSaga>(typeof(OrderCompletedSaga).Name.ToLowerInvariant());
        });
    }

    private static readonly System.Diagnostics.ActivityListener _listener = new()
    {
        ShouldListenTo = source => source.Name == "Talaria.Core",
        Sample = (ref System.Diagnostics.ActivityCreationOptions<System.Diagnostics.ActivityContext> _) => System.Diagnostics.ActivitySamplingResult.AllData
    };

    static TalariaSagaSteps()
    {
        System.Diagnostics.ActivitySource.AddActivityListener(_listener);
    }

    private async Task EnsureHostStarted()
    {
        if (!_hostStarted && _host != null)
        {
            await _host.StartAsync();
            _hostStarted = true;
        }
    }

    [When(@"^I publish an ""([^""]*)"" message with correlation ID ""([^""]*)""$")]
    public async Task WhenIPublishAMessageWithCorrelationID(string messageType, string correlationId)
    {
        await EnsureHostStarted();
        
        switch (messageType)
        {
            case "OrderPlaced":
                var p1 = await _transport.CreateProducerAsync<OrderPlacedSaga>("order-placed", new ProducerOptions());
                await p1.ProduceAsync(new OrderPlacedSaga { CorrelationId = correlationId });
                break;
            case "OrderBilled":
                var p2 = await _transport.CreateProducerAsync<OrderBilledSaga>("order-billed", new ProducerOptions());
                await p2.ProduceAsync(new OrderBilledSaga { OrderId = correlationId });
                break;
        }
    }

    [When(@"^wait (\d+) ms$")]
    [Then(@"^wait (\d+) ms$")]
    public async Task WhenWaitMs(int ms)
    {
        await Task.Delay(ms);
    }

    [Then(@"^the saga state for ""([^""]*)"" should exist$")]
    public async Task ThenTheSagaStateForShouldExist(string id)
    {
        var store = _host!.Services.GetRequiredService<IStateStore<OrderSagaState>>();
        OrderSagaState? state = null;
        for (int i = 0; i < 50; i++)
        {
            state = await store.GetAsync(id);
            if (state != null) break;
            await Task.Delay(50);
        }
        Assert.NotNull(state);
    }

    [Then(@"^the saga state for ""([^""]*)"" should not exist$")]
    [Then(@"^the saga state for ""([^""]*)"" should no longer exist$")]
    public async Task ThenTheSagaStateForShouldNotExistOrShouldNoLongerExist(string id)
    {
        var store = _host!.Services.GetRequiredService<IStateStore<OrderSagaState>>();
        OrderSagaState? state = null;
        for (int i = 0; i < 50; i++)
        {
            state = await store.GetAsync(id);
            if (state == null) break;
            await Task.Delay(50);
        }
        Assert.Null(state);
    }

    [Then(@"^an ""([^""]*)"" message should be dispatched$")]
    public async Task ThenAnMessageShouldBeDispatched(string messageType)
    {
        if (messageType == "OrderCompleted")
        {
            var topic = typeof(OrderCompletedSaga).Name.ToLowerInvariant();
            for (int i = 0; i < 50; i++)
            {
                var messages = await _transport.ReadAllFromTopicAsync<OrderCompletedSaga>(topic);
                if (messages.Count > 0) return;
                await Task.Delay(50);
            }
            Assert.Fail("Message was not dispatched.");
        }
    }

    [When(@"^I publish an ""([^""]*)"" message with correlation ID ""([^""]*)"" and traceparent ""([^""]*)""$")]
    public async Task WhenIPublishAMessageWithCorrelationIDAndTraceParent(string messageType, string correlationId, string traceparent)
    {
        await EnsureHostStarted();
        var headers = new MessageHeaders { TraceParent = traceparent };
        
        switch (messageType)
        {
            case "OrderBilled":
                var p2 = await _transport.CreateProducerAsync<OrderBilledSaga>("order-billed", new ProducerOptions());
                await p2.ProduceAsync(new OrderBilledSaga { OrderId = correlationId }, headers);
                break;
        }
    }

    [Then(@"^an ""(.*)"" message should be dispatched with traceparent ""(.*)""$")]
    public async Task ThenAnMessageShouldBeDispatchedWithTraceParent(string messageType, string expectedParent)
    {
        if (messageType == "OrderCompleted")
        {
            var topic = typeof(OrderCompletedSaga).Name.ToLowerInvariant();

            // The outbox relay publishes asynchronously — poll until the message lands.
            List<Core.Abstractions.MessageEnvelope<OrderCompletedSaga>> envelopes = [];
            for (int i = 0; i < 50; i++)
            {
                envelopes.AddRange(await _transport.ReadAllFromTopicAsync<OrderCompletedSaga>(topic));
                if (envelopes.Count > 0) break;
                await Task.Delay(50);
            }

            var envelope = Assert.Single(envelopes);
            
            var expectedTraceId = expectedParent.Split('-')[1];
            Assert.NotNull(envelope.Headers.TraceParent);
            var generatedTraceId = envelope.Headers.TraceParent!.Split('-')[1];
            
            Assert.Equal(expectedTraceId, generatedTraceId);
        }
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
