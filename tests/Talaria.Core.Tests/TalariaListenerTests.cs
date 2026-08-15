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
            Handler = (msg, headers, _, ct) => Task.CompletedTask
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
        var topicReg = new TopicRegistry();
        var handlerEntered = new TaskCompletionSource();
        var handlerExited = new TaskCompletionSource();

        topicReg.MapTopic<DummyMessage>("dispose.topic", async (msg, ct) =>
        {
            handlerEntered.TrySetResult();
            try
            {
                await Task.Delay(Timeout.Infinite, ct);
            }
            finally
            {
                handlerExited.TrySetResult();
            }
        });

        var listener = new TalariaListener(
            transport,
            topicReg,
            new SagaRegistry(),
            new TalariaOptions { ApplicationName = "test-app" },
            NullLogger<TalariaListener>.Instance);

        await listener.StartAsync();

        var producer = await transport.CreateProducerAsync<DummyMessage>("dispose.topic", new ProducerOptions());
        await producer.ProduceAsync(new DummyMessage { Id = "DISPOSE-1" });

        await handlerEntered.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await listener.DisposeAsync();

        await handlerExited.Task.WaitAsync(TimeSpan.FromSeconds(5));
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

        await TestAsyncHelpers.WaitUntilAsync(() => received.Count == 1, TimeSpan.FromSeconds(1));
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

        var dlq = await TestAsyncHelpers.ReadUntilAsync<DummyMessage>(transport, "hop.topic.dlq", 1);
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

        await TestAsyncHelpers.WaitUntilAsync(() => received.Count == 1);
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
        await TestAsyncHelpers.WaitUntilAsync(async () => await store.GetAsync("saga-1") is not null);

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

    private class ClassConsumerMessage { public string Id { get; set; } = ""; }
    private class DummyClassConsumer : ITopicConsumer<ClassConsumerMessage>
    {
        public Task ConsumeAsync(ConsumeContext<ClassConsumerMessage> context) => Task.CompletedTask;
    }

    [Fact]
    public async Task ClassConsumer_Registered_Without_ServiceProvider_Throws_InvalidOperationException()
    {
        var transport = new InMemoryTransport();

        var topicReg = new TopicRegistry();
        topicReg.MapTopic<ClassConsumerMessage, DummyClassConsumer>("class-consumer.topic");

        var listener = new TalariaListener(
            transport,
            topicReg,
            new SagaRegistry(),
            new TalariaOptions { ApplicationName = "test-app" },
            NullLogger<TalariaListener>.Instance);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => listener.StartAsync());
        Assert.Contains("IServiceProvider", ex.Message);
    }

    [Fact]
    public async Task StartAsync_Throws_When_RunAsync_FaultsSynchronously()
    {
        var transport = new FaultingTransport();

        var registry = new SagaRegistry();
        var config = new SagaConfigurator<FaultSagaState>(registry);
        config.StartedBy<FaultSagaStart>("fault.saga.start",
            (msg, ctx) => Task.FromResult(ctx.Transition(new FaultSagaState { Id = msg.Id })),
            m => m.Id);
        config.Complete();

        var services = new ServiceCollection()
            .AddSingleton(typeof(IStateStore<>), typeof(InMemoryStateStore<>))
            .BuildServiceProvider();

        var listener = new TalariaListener(
            transport,
            new TopicRegistry(),
            registry,
            new TalariaOptions { ApplicationName = "test-app" },
            NullLogger<TalariaListener>.Instance,
            services);

        // SagaConsumerEngine pre-creates producers before starting supervised loops,
        // so a synchronously faulting CreateProducerAsync faults RunAsync itself.
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => listener.StartAsync());
        Assert.Contains("transport fault", ex.Message);

        // A synchronously faulted start moves the listener to the stopped state,
        // so subsequent starts are rejected as single-cycle.
        var later = await Assert.ThrowsAsync<InvalidOperationException>(() => listener.StartAsync());
        Assert.Contains("single-cycle", later.Message);
    }

    [Fact]
    public async Task StopAsync_Unblocks_Blocked_Handler()
    {
        var transport = new InMemoryTransport();
        var topicReg = new TopicRegistry();
        var handlerEntered = new TaskCompletionSource();
        var handlerExited = new TaskCompletionSource();

        topicReg.MapTopic<DummyMessage>("stop-block.topic", async (msg, ct) =>
        {
            handlerEntered.TrySetResult();
            try
            {
                await Task.Delay(Timeout.Infinite, ct);
            }
            finally
            {
                handlerExited.TrySetResult();
            }
        });

        var listener = new TalariaListener(
            transport,
            topicReg,
            new SagaRegistry(),
            new TalariaOptions { ApplicationName = "test-app" },
            NullLogger<TalariaListener>.Instance);

        await listener.StartAsync();

        var producer = await transport.CreateProducerAsync<DummyMessage>("stop-block.topic", new ProducerOptions());
        await producer.ProduceAsync(new DummyMessage { Id = "STOP-BLOCK" });

        await handlerEntered.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await listener.StopAsync();

        await handlerExited.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.False(listener.IsRunning);
    }

    [Fact]
    public async Task DisposeAsync_Does_Not_Dispose_Caller_Owned_Transport()
    {
        var transport = new TrackableTransport();
        var listener = new TalariaListener(
            transport,
            new TopicRegistry(),
            new SagaRegistry(),
            new TalariaOptions { ApplicationName = "test-app" },
            NullLogger<TalariaListener>.Instance);

        await listener.StartAsync();
        await listener.DisposeAsync();

        Assert.False(transport.Disposed);
    }

    [Fact]
    public async Task Explicit_Stores_Precede_ServiceProvider_Resolved_Stores()
    {
        var transport = new InMemoryTransport();
        var received = new List<string>();

        var topicReg = new TopicRegistry();
        topicReg.MapTopic<DummyMessage>("precedence.topic", (msg, ct) =>
        {
            received.Add(msg.Id);
            return Task.CompletedTask;
        });

        var serviceProviderStore = new InMemoryIdempotencyStore();
        var explicitStore = new InMemoryIdempotencyStore();

        var services = new ServiceCollection()
            .AddSingleton<IIdempotencyStore>(serviceProviderStore)
            .BuildServiceProvider();

        var listener = new TalariaListener(
            transport,
            topicReg,
            new SagaRegistry(),
            new TalariaOptions { ApplicationName = "test-app" },
            NullLogger<TalariaListener>.Instance,
            services,
            new TalariaListenerStores(IdempotencyStore: explicitStore));

        var producer = await transport.CreateProducerAsync<DummyMessage>("precedence.topic", new ProducerOptions());
        var messageId = "precedence-msg";
        await producer.ProduceAsync(new DummyMessage { Id = "PREC" }, new MessageHeaders { MessageId = messageId });

        // Mark the message complete in the service-provider-resolved store.
        var lck = await serviceProviderStore.TryAcquireLockAsync(messageId, "test-app.precedence.topic", TimeSpan.FromMinutes(1));
        Assert.NotNull(lck);
        await serviceProviderStore.MarkCompleteAsync(lck);

        await listener.StartAsync();
        await TestAsyncHelpers.WaitUntilAsync(() => received.Count == 1);

        Assert.Single(received);
        Assert.Equal("PREC", received[0]);

        await listener.StopAsync();
    }

    private class ManualSagaState
    {
        public string Id { get; set; } = "";
    }

    private class ManualSagaStart
    {
        public string Id { get; set; } = "";
    }

    private class FaultSagaState
    {
        public string Id { get; set; } = "";
    }

    private class FaultSagaStart
    {
        public string Id { get; set; } = "";
    }

    private class FaultingTransport : ITransport
    {
        public string Name => "Faulting";

        public Task<IConsumer<T>> CreateConsumerAsync<T>(string topic, ConsumerOptions options, CancellationToken ct = default)
            => Task.FromResult<IConsumer<T>>(new FakeConsumer<T>());

        public Task<IProducer<T>> CreateProducerAsync<T>(string topic, ProducerOptions options, CancellationToken ct = default)
            => throw new InvalidOperationException("transport fault");

        public Task<ITransactionalSession> BeginTransactionAsync(
            string? consumerGroup = null,
            TransactionOffsetSource? offsetSource = null,
            CancellationToken ct = default)
            => Task.FromResult<ITransactionalSession>(new FakeSession());
    }

    private sealed class TrackableTransport : ITransport, IAsyncDisposable
    {
        public bool Disposed { get; private set; }

        public string Name => "Trackable";

        public Task<IConsumer<T>> CreateConsumerAsync<T>(string topic, ConsumerOptions options, CancellationToken ct = default)
            => Task.FromResult<IConsumer<T>>(new FakeConsumer<T>());

        public Task<IProducer<T>> CreateProducerAsync<T>(string topic, ProducerOptions options, CancellationToken ct = default)
            => Task.FromResult<IProducer<T>>(new FakeProducer<T>());

        public Task<ITransactionalSession> BeginTransactionAsync(
            string? consumerGroup = null,
            TransactionOffsetSource? offsetSource = null,
            CancellationToken ct = default)
            => Task.FromResult<ITransactionalSession>(new FakeSession());

        public ValueTask DisposeAsync()
        {
            Disposed = true;
            return default;
        }
    }

    private sealed class FakeConsumer<T> : IConsumer<T>
    {
        public IAsyncEnumerable<MessageEnvelope<T>> ConsumeAsync(CancellationToken ct = default)
            => EmptyAsync();

        private static async IAsyncEnumerable<MessageEnvelope<T>> EmptyAsync()
        {
            await Task.Yield();
            yield break;
        }

        public Task CommitAsync(MessageEnvelope<T> message, CancellationToken ct = default)
            => Task.CompletedTask;

        public Task NackAsync(MessageEnvelope<T> message, CancellationToken ct = default)
            => Task.CompletedTask;

        public ValueTask DisposeAsync() => default;
    }

    private sealed class FakeProducer<T> : IProducer<T>
    {
        public Task ProduceAsync(T message, MessageHeaders? headers = null, string? partitionKey = null, CancellationToken ct = default)
            => Task.CompletedTask;

        public ValueTask DisposeAsync() => default;
    }

    private sealed class FakeSession : ITransactionalSession
    {
        public Task<IProducer<T>> GetProducerAsync<T>(string topic, CancellationToken ct = default)
            => Task.FromResult<IProducer<T>>(new FakeProducer<T>());

        public Task CommitAsync(CancellationToken ct = default) => Task.CompletedTask;
        public Task AbortAsync(CancellationToken ct = default) => Task.CompletedTask;
        public ValueTask DisposeAsync() => default;
    }
}
