using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Talaria.Core.Abstractions;
using Talaria.Core.Hosting;
using Talaria.Core.Registration;
using Talaria.Core.Sagas;
using Talaria.Transports.InMemory;
using Xunit;

namespace Talaria.Core.Tests;

/// <summary>
/// Lifecycle and host-agnostic processing coverage for <see cref="TalariaListener"/>.
/// </summary>
public class TalariaListenerTests
{
    private class DummyMessage { public string Id { get; set; } = ""; }

    [Fact]
    public async Task StartAsync_Seals_Registries()
    {
        var transport = new InMemoryTransport();
        var topicReg = new TopicRegistry();
        var sagaReg = new SagaRegistry();

        var listener = new TalariaListener(
            transport,
            topicReg,
            sagaReg,
            new TalariaOptions { ApplicationName = "test-app" },
            NullLogger<TalariaListener>.Instance);

        Assert.False(topicReg.IsSealed);
        Assert.False(sagaReg.IsSealed);

        await listener.StartAsync();

        Assert.True(topicReg.IsSealed);
        Assert.True(sagaReg.IsSealed);

        await listener.StopAsync();
    }

    [Fact]
    public async Task StartAsync_Is_Idempotent_While_Running()
    {
        var transport = new InMemoryTransport();
        var topicReg = new TopicRegistry();
        topicReg.Add(new TopicRegistration
        {
            TopicName = "idle-topic",
            MessageType = typeof(DummyMessage),
            Handler = (msg, headers, ct) => Task.CompletedTask
        });

        var listener = new TalariaListener(
            transport,
            topicReg,
            new SagaRegistry(),
            new TalariaOptions { ApplicationName = "test-app" },
            NullLogger<TalariaListener>.Instance);

        var first = listener.StartAsync();
        var second = listener.StartAsync();

        Assert.True(first.IsCompleted);
        Assert.True(second.IsCompleted);
        Assert.True(listener.IsRunning);

        await listener.StopAsync();
    }

    [Fact]
    public async Task StopAsync_Is_Idempotent_After_Stop()
    {
        var transport = new InMemoryTransport();
        var listener = new TalariaListener(
            transport,
            new TopicRegistry(),
            new SagaRegistry(),
            new TalariaOptions { ApplicationName = "test-app" },
            NullLogger<TalariaListener>.Instance);

        await listener.StartAsync();
        await listener.StopAsync();
        await listener.StopAsync(); // must not throw
    }

    [Fact]
    public async Task StartAsync_After_Stop_Throws_InvalidOperationException()
    {
        var transport = new InMemoryTransport();
        var listener = new TalariaListener(
            transport,
            new TopicRegistry(),
            new SagaRegistry(),
            new TalariaOptions { ApplicationName = "test-app" },
            NullLogger<TalariaListener>.Instance);

        await listener.StartAsync();
        await listener.StopAsync();

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => listener.StartAsync());
        Assert.Contains("single-cycle", ex.Message);
    }

    [Fact]
    public async Task DisposeAsync_Stops_If_Running()
    {
        var transport = new InMemoryTransport();
        var listener = new TalariaListener(
            transport,
            new TopicRegistry(),
            new SagaRegistry(),
            new TalariaOptions { ApplicationName = "test-app" },
            NullLogger<TalariaListener>.Instance);

        await listener.StartAsync();
        await listener.DisposeAsync();

        Assert.False(listener.IsRunning);
    }

    [Fact]
    public async Task Topic_Only_Processing_Without_Host()
    {
        var transport = new InMemoryTransport();
        var received = new List<string>();

        var topicReg = new TopicRegistry();
        topicReg.MapTopic<DummyMessage>("manual.topic", (msg, ct) =>
        {
            received.Add(msg.Id);
            return Task.CompletedTask;
        });

        var listener = new TalariaListener(
            transport,
            topicReg,
            new SagaRegistry(),
            new TalariaOptions { ApplicationName = "test-app" },
            NullLogger<TalariaListener>.Instance);

        await listener.StartAsync();
        await Task.Delay(500); // let the consumer spin up

        var producer = await transport.CreateProducerAsync<DummyMessage>("manual.topic", new ProducerOptions());
        await producer.ProduceAsync(new DummyMessage { Id = "MSG-1" });

        var processed = await PollUntilAsync(() => Task.FromResult(received.Count == 1), TimeSpan.FromSeconds(1));
        Assert.True(processed, "Message was not processed.");
        Assert.Equal("MSG-1", received[0]);

        await listener.StopAsync();
    }

    [Fact]
    public async Task HopGuard_Routes_To_DLQ_Without_Host()
    {
        var transport = new InMemoryTransport();
        var received = new List<string>();

        var topicReg = new TopicRegistry();
        topicReg.MapTopic<DummyMessage>("hop.topic", (msg, ct) =>
        {
            received.Add(msg.Id);
            return Task.CompletedTask;
        });

        var listener = new TalariaListener(
            transport,
            topicReg,
            new SagaRegistry(),
            new TalariaOptions { ApplicationName = "test-app", MaxHopCount = 2 },
            NullLogger<TalariaListener>.Instance);

        await listener.StartAsync();

        var producer = await transport.CreateProducerAsync<DummyMessage>("hop.topic", new ProducerOptions());
        await producer.ProduceAsync(new DummyMessage { Id = "HOP" }, new MessageHeaders { HopCount = 2 });

        var dlq = await ReadUntilAsync<DummyMessage>(transport, "hop.topic.dlq", 1);
        Assert.Single(dlq);
        Assert.Equal("HOP", dlq[0].Payload!.Id);
        Assert.Equal("max_hops_exceeded", dlq[0].Headers.DlqReason);
        Assert.Empty(received);

        await listener.StopAsync();
    }

    [Fact]
    public async Task Deferral_Sweeper_Runs_Without_Host()
    {
        var transport = new InMemoryTransport();
        var deferralStore = new InMemoryDeferralStore();
        var received = new List<string>();

        var topicReg = new TopicRegistry();
        topicReg.MapTopic<DummyMessage>("defer.topic", (msg, ct) =>
        {
            received.Add(msg.Id);
            return Task.CompletedTask;
        });

        var listener = new TalariaListener(
            transport,
            topicReg,
            new SagaRegistry(),
            new TalariaOptions
            {
                ApplicationName = "test-app",
                DeferralBackoff = TimeSpan.FromMilliseconds(50)
            },
            NullLogger<TalariaListener>.Instance,
            stores: new TalariaListenerStores(DeferralStore: deferralStore));

        await listener.StartAsync();

        var deferred = new DeferredMessage(
            Guid.NewGuid(),
            "defer.topic",
            typeof(DummyMessage).AssemblyQualifiedName!,
            "{\"Id\":\"DEF\"}",
            new MessageHeaders(),
            null,
            1,
            DateTimeOffset.UtcNow.AddMilliseconds(-1),
            null);

        await deferralStore.EnqueueAsync(deferred);

        await WaitUntilAsync(() => received.Count == 1);
        Assert.Equal("DEF", received[0]);

        await listener.StopAsync();
    }

    /// <summary>
    /// Sagas require an <see cref="IServiceProvider"/> because the listener resolves
    /// <see cref="IStateStore{TState}"/> from a DI scope for each message. The provider
    /// must have a registration for the closed generic state-store type.
    /// </summary>
    [Fact]
    public async Task Saga_Processing_With_Minimal_ServiceCollection()
    {
        var transport = new InMemoryTransport();

        var registry = new SagaRegistry();
        var config = new SagaConfigurator<ManualSagaState>(registry);
        config.StartedBy<ManualSagaStart>("manual.saga.start",
            (msg, ctx) => Task.FromResult(ctx.Transition(new ManualSagaState { Id = msg.Id })),
            m => m.Id);
        config.Complete();

        var services = new ServiceCollection()
            .AddSingleton<ITransport>(transport)
            .AddSingleton(typeof(IStateStore<>), typeof(InMemoryStateStore<>))
            .BuildServiceProvider();

        var listener = new TalariaListener(
            transport,
            new TopicRegistry(),
            registry,
            new TalariaOptions { ApplicationName = "test-app" },
            NullLogger<TalariaListener>.Instance,
            services);

        await listener.StartAsync();

        var producer = await transport.CreateProducerAsync<ManualSagaStart>("manual.saga.start", new ProducerOptions());
        await producer.ProduceAsync(new ManualSagaStart { Id = "saga-1" });

        var store = services.GetRequiredService<IStateStore<ManualSagaState>>();
        await WaitUntilAsync(async () => await store.GetAsync("saga-1") is not null);

        var state = await store.GetAsync("saga-1");
        Assert.NotNull(state);
        Assert.Equal("saga-1", state.Id);

        await listener.StopAsync();
    }

    [Fact]
    public async Task Saga_Registered_Without_ServiceProvider_Throws_InvalidOperationException()
    {
        var transport = new InMemoryTransport();

        var registry = new SagaRegistry();
        var config = new SagaConfigurator<ManualSagaState>(registry);
        config.StartedBy<ManualSagaStart>("manual.saga.start",
            (msg, ctx) => Task.FromResult(ctx.Transition(new ManualSagaState { Id = msg.Id })),
            m => m.Id);
        config.Complete();

        var listener = new TalariaListener(
            transport,
            new TopicRegistry(),
            registry,
            new TalariaOptions { ApplicationName = "test-app" },
            NullLogger<TalariaListener>.Instance);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => listener.StartAsync());
        Assert.Contains("IServiceProvider", ex.Message);
    }

    private class ManualSagaState
    {
        public string Id { get; set; } = "";
    }

    private class ManualSagaStart
    {
        public string Id { get; set; } = "";
    }

    private static async Task<List<MessageEnvelope<T>>> ReadUntilAsync<T>(
        InMemoryTransport transport, string topic, int expectedCount)
    {
        var deadline = DateTime.UtcNow.AddSeconds(10);
        List<MessageEnvelope<T>> messages;
        do
        {
            messages = await transport.ReadAllFromTopicAsync<T>(topic);
            if (messages.Count >= expectedCount)
            {
                break;
            }

            await Task.Delay(50);
        }
        while (DateTime.UtcNow < deadline);

        return messages;
    }

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow.AddSeconds(10);
        while (DateTime.UtcNow < deadline)
        {
            if (condition())
            {
                return;
            }

            await Task.Delay(25);
        }

        Assert.True(condition(), "Condition was not met within the timeout.");
    }

    private static async Task WaitUntilAsync(Func<Task<bool>> condition)
    {
        var deadline = DateTime.UtcNow.AddSeconds(10);
        while (DateTime.UtcNow < deadline)
        {
            if (await condition())
            {
                return;
            }

            await Task.Delay(25);
        }

        Assert.True(await condition(), "Condition was not met within the timeout.");
    }

    private static async Task<bool> PollUntilAsync(Func<Task<bool>> condition, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (await condition())
            {
                return true;
            }

            await Task.Delay(25);
        }

        return await condition();
    }
}
