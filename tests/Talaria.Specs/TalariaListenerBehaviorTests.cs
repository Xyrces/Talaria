using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Talaria.Core;
using Talaria.Core.Abstractions;
using Talaria.Core.Hosting;
using Talaria.Core.Registration;
using Talaria.Core.Sagas;
using Talaria.Transports.InMemory;
using Xunit;

namespace Talaria.Specs.Tests;

/// <summary>
/// Behavior tests driven purely through a manually-started <see cref="TalariaListener"/>
/// without a Generic Host.
/// </summary>
public class TalariaListenerBehaviorTests
{
    private class TopicMessage { public string Id { get; set; } = ""; }

    [Fact]
    public async Task Manual_Topic_Listener_Processes_And_Commits_Message()
    {
        var transport = new InMemoryTransport();
        var received = new List<string>();

        var topicReg = new TopicRegistry();
        topicReg.MapTopic<TopicMessage>("listener.topic", (msg, ct) =>
        {
            received.Add(msg.Id);
            return Task.CompletedTask;
        });

        var listener = new TalariaListener(
            transport,
            topicReg,
            new SagaRegistry(),
            new TalariaOptions { ApplicationName = "manual-app" },
            NullLogger<TalariaListener>.Instance);

        await listener.StartAsync();

        var producer = await transport.CreateProducerAsync<TopicMessage>("listener.topic", new ProducerOptions());
        await producer.ProduceAsync(new TopicMessage { Id = "manual-1" });

        await TestAsyncHelpers.PollUntilAsync(() => Task.FromResult(received.Count == 1), TimeSpan.FromSeconds(5));
        Assert.Equal("manual-1", received[0]);

        await listener.StopAsync();
    }

    private class SagaState { public string Id { get; set; } = ""; public bool Completed { get; set; } }
    private class StartSaga { public string Id { get; set; } = ""; }
    private class CompleteSaga { public string Id { get; set; } = ""; }
    private class SagaCompleted { public string Id { get; set; } = ""; }

    [Fact]
    public async Task Manual_Saga_Listener_Completes_EndToEnd()
    {
        var transport = new InMemoryTransport();

        var registry = new SagaRegistry();
        var config = new SagaConfigurator<SagaState>(registry);
        config.StartedBy<StartSaga>("listener.saga.start",
            (msg, ctx) => Task.FromResult(ctx.Transition(new SagaState { Id = msg.Id })),
            m => m.Id);
        config.On<CompleteSaga>("listener.saga.complete",
            (state, msg, ctx) =>
            {
                state.Completed = true;
                ctx.Dispatch(new SagaCompleted { Id = msg.Id });
                return Task.FromResult(ctx.Complete());
            },
            m => m.Id);
        config.DispatchTo<SagaCompleted>("listener.saga.completed");
        config.Complete();

        var services = new ServiceCollection()
            .AddSingleton<ITransport>(transport)
            .AddSingleton(typeof(IStateStore<>), typeof(InMemoryStateStore<>))
            .BuildServiceProvider();

        var listener = new TalariaListener(
            transport,
            new TopicRegistry(),
            registry,
            new TalariaOptions
            {
                ApplicationName = "manual-app",
                DeferralBackoff = TimeSpan.FromMilliseconds(50),
                OutboxRelayInterval = TimeSpan.FromMilliseconds(50)
            },
            NullLogger<TalariaListener>.Instance,
            services);

        await listener.StartAsync();

        var startProducer = await transport.CreateProducerAsync<StartSaga>("listener.saga.start", new ProducerOptions());
        await startProducer.ProduceAsync(new StartSaga { Id = "end-1" });

        var store = services.GetRequiredService<IStateStore<SagaState>>();
        var started = await TestAsyncHelpers.PollUntilAsync(async () => await store.GetAsync("end-1") is not null, TimeSpan.FromSeconds(5));
        Assert.True(started, "Saga starter never created state.");

        var completeProducer = await transport.CreateProducerAsync<CompleteSaga>("listener.saga.complete", new ProducerOptions());
        await completeProducer.ProduceAsync(new CompleteSaga { Id = "end-1" });

        var dispatched = await TestAsyncHelpers.ReadUntilAsync<SagaCompleted>(transport, "listener.saga.completed", 1);
        var envelope = Assert.Single(dispatched);
        Assert.Equal("end-1", envelope.Payload!.Id);

        await listener.StopAsync();
    }

}
