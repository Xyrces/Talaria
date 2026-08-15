// SPDX-License-Identifier: Apache-2.0

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Talaria.Core.Abstractions;
using Talaria.Core.Hosting;
using Talaria.Core.Registration;
using Talaria.Core.Sagas;
using Talaria.Transports.InMemory;

namespace Talaria.Core.Tests;

public class TopicConsumerScopeTests
{
    private class ScopeMessage { public string Id { get; set; } = ""; }

    private class ScopedDependency
    {
        public Guid InstanceId { get; } = Guid.NewGuid();
    }

    private class DisposableTracker : IAsyncDisposable
    {
        private readonly Action _onDispose;

        public DisposableTracker(Action onDispose)
        {
            _onDispose = onDispose;
        }

        public ValueTask DisposeAsync()
        {
            _onDispose();
            return ValueTask.CompletedTask;
        }
    }

    private class CapturingConsumer : ITopicConsumer<ScopeMessage>
    {
        private readonly ScopedDependency _dependency;
        private readonly List<Guid> _capturedIds;

        public CapturingConsumer(ScopedDependency dependency, List<Guid> capturedIds)
        {
            _dependency = dependency;
            _capturedIds = capturedIds;
        }

        public Task ConsumeAsync(ConsumeContext<ScopeMessage> context)
        {
            _capturedIds.Add(_dependency.InstanceId);
            return Task.CompletedTask;
        }
    }

    private class ContextServicesConsumer : ITopicConsumer<ScopeMessage>
    {
        private readonly List<Guid> _capturedIds;

        public ContextServicesConsumer(List<Guid> capturedIds)
        {
            _capturedIds = capturedIds;
        }

        public Task ConsumeAsync(ConsumeContext<ScopeMessage> context)
        {
            var dependency = context.Services.GetRequiredService<ScopedDependency>();
            _capturedIds.Add(dependency.InstanceId);
            return Task.CompletedTask;
        }
    }

    private class ScopeDisposingConsumer : ITopicConsumer<ScopeMessage>
    {
        private readonly DisposableTracker _tracker;
        private readonly List<DisposableTracker> _trackers;

        public ScopeDisposingConsumer(DisposableTracker tracker, List<DisposableTracker> trackers)
        {
            _tracker = tracker;
            _trackers = trackers;
        }

        public Task ConsumeAsync(ConsumeContext<ScopeMessage> context)
        {
            _trackers.Add(_tracker);
            return Task.CompletedTask;
        }
    }

    private class ThrowingConstructorConsumer : ITopicConsumer<ScopeMessage>
    {
        public ThrowingConstructorConsumer()
        {
            throw new InvalidOperationException("constructor failure");
        }

        public Task ConsumeAsync(ConsumeContext<ScopeMessage> context) => Task.CompletedTask;
    }

    private class ThrowingConsumer : ITopicConsumer<ScopeMessage>
    {
        public Task ConsumeAsync(ConsumeContext<ScopeMessage> context)
        {
            throw new InvalidOperationException("handler failure");
        }
    }

    private class ThrowingDisposableTracker : IAsyncDisposable
    {
        public bool Disposed { get; private set; }

        public ValueTask DisposeAsync()
        {
            Disposed = true;
            throw new InvalidOperationException("disposal failure");
        }
    }

    private class AsyncDisposableTracker : IAsyncDisposable
    {
        private readonly Action _onDispose;

        public AsyncDisposableTracker(Action onDispose)
        {
            _onDispose = onDispose;
        }

        public ValueTask DisposeAsync()
        {
            _onDispose();
            return ValueTask.CompletedTask;
        }
    }

    private class ScopeDisposalTrackingConsumer : ITopicConsumer<ScopeMessage>
    {
        private readonly ThrowingDisposableTracker _tracker;
        private readonly List<string> _observedOrder;

        public ScopeDisposalTrackingConsumer(ThrowingDisposableTracker tracker, List<string> observedOrder)
        {
            _tracker = tracker;
            _observedOrder = observedOrder;
        }

        public Task ConsumeAsync(ConsumeContext<ScopeMessage> context)
        {
            _observedOrder.Add("handler-start");
            throw new InvalidOperationException("handler failure");
        }
    }

    private class OrderTrackingConsumer : ITopicConsumer<ScopeMessage>
    {
        private readonly AsyncDisposableTracker _tracker;

        public OrderTrackingConsumer(AsyncDisposableTracker tracker)
        {
            _tracker = tracker;
        }

        public Task ConsumeAsync(ConsumeContext<ScopeMessage> context)
        {
            throw new InvalidOperationException("handler failure");
        }
    }

    private class InstanceCounter
    {
        public int InstanceId { get; }

        public InstanceCounter()
        {
            InstanceId = Interlocked.Increment(ref _nextId);
        }

        private static int _nextId;
    }

    private class InstanceCapturingConsumer : ITopicConsumer<ScopeMessage>
    {
        private readonly InstanceCounter _counter;
        private readonly List<int> _capturedIds;

        public InstanceCapturingConsumer(InstanceCounter counter, List<int> capturedIds)
        {
            _counter = counter;
            _capturedIds = capturedIds;
        }

        public Task ConsumeAsync(ConsumeContext<ScopeMessage> context)
        {
            _capturedIds.Add(_counter.InstanceId);
            throw new InvalidOperationException("handler failure");
        }
    }

    private class ConstructionCountingConsumer : ITopicConsumer<ScopeMessage>
    {
        public static int ConstructionCount;
        public static int HandlerInvocationCount;

        public ConstructionCountingConsumer()
        {
            Interlocked.Increment(ref ConstructionCount);
        }

        public Task ConsumeAsync(ConsumeContext<ScopeMessage> context)
        {
            Interlocked.Increment(ref HandlerInvocationCount);
            return Task.CompletedTask;
        }
    }

    private class ThrowingDisposeDependency : IAsyncDisposable
    {
        public ValueTask DisposeAsync()
        {
            throw new InvalidOperationException("scoped disposal failure");
        }
    }

    private class SucceedingWithDisposableConsumer : ITopicConsumer<ScopeMessage>
    {
        private readonly ThrowingDisposeDependency _dependency;
        private readonly Action _onHandled;

        public SucceedingWithDisposableConsumer(ThrowingDisposeDependency dependency, Action onHandled)
        {
            _dependency = dependency;
            _onHandled = onHandled;
        }

        public Task ConsumeAsync(ConsumeContext<ScopeMessage> context)
        {
            _ = _dependency;
            _onHandled();
            return Task.CompletedTask;
        }
    }

    private class CountingIdempotencyStore : IIdempotencyStore
    {
        private readonly IIdempotencyStore _inner;
        private int _acquireCount;

        public CountingIdempotencyStore(IIdempotencyStore inner)
        {
            _inner = inner;
        }

        public int AcquireCount => _acquireCount;

        public async Task<IdempotencyLock?> TryAcquireLockAsync(
            string messageId,
            string consumerQueue,
            TimeSpan expiration,
            CancellationToken ct = default)
        {
            Interlocked.Increment(ref _acquireCount);
            return await _inner.TryAcquireLockAsync(messageId, consumerQueue, expiration, ct);
        }

        public Task MarkCompleteAsync(IdempotencyLock @lock, CancellationToken ct = default)
            => _inner.MarkCompleteAsync(@lock, ct);

        public Task ReleaseLockAsync(IdempotencyLock @lock, CancellationToken ct = default)
            => _inner.ReleaseLockAsync(@lock, ct);
    }

    private class NoIsServiceProvider : IServiceProvider
    {
        private readonly TopicRegistry _registry;

        public NoIsServiceProvider(TopicRegistry registry)
        {
            _registry = registry;
        }

        public object? GetService(Type serviceType)
        {
            if (serviceType == typeof(TopicRegistry))
            {
                return _registry;
            }

            return null;
        }
    }

    [Fact]
    public async Task Scoped_Dependency_Instance_Differs_Per_Message()
    {
        var transport = new InMemoryTransport();
        var capturedIds = new List<Guid>();

        var services = new ServiceCollection()
            .AddSingleton(transport)
            .AddSingleton(capturedIds)
            .AddScoped<ScopedDependency>()
            .AddScoped<CapturingConsumer>()
            .BuildServiceProvider();

        var topicReg = new TopicRegistry();
        topicReg.MapTopic<ScopeMessage, CapturingConsumer>("scope.instance.topic");

        var listener = new TalariaListener(
            transport,
            topicReg,
            new SagaRegistry(),
            new TalariaOptions { ApplicationName = "scope-test" },
            NullLogger<TalariaListener>.Instance,
            services);

        await listener.StartAsync();

        var producer = await transport.CreateProducerAsync<ScopeMessage>("scope.instance.topic", new ProducerOptions());
        for (int i = 0; i < 3; i++)
        {
            await producer.ProduceAsync(new ScopeMessage { Id = $"msg-{i}" });
        }

        await TestAsyncHelpers.WaitUntilAsync(() => capturedIds.Count == 3);

        Assert.Equal(3, capturedIds.Distinct().Count());

        await listener.StopAsync();
    }

    [Fact]
    public async Task Scope_Is_Disposed_After_Handling()
    {
        var transport = new InMemoryTransport();
        var trackers = new List<DisposableTracker>();
        var disposedCount = 0;

        var services = new ServiceCollection()
            .AddSingleton(transport)
            .AddSingleton(trackers)
            .AddScoped<DisposableTracker>(_ =>
                new DisposableTracker(() => Interlocked.Increment(ref disposedCount)))
            .AddScoped<ScopeDisposingConsumer>()
            .BuildServiceProvider();

        var topicReg = new TopicRegistry();
        topicReg.MapTopic<ScopeMessage, ScopeDisposingConsumer>("scope.dispose.topic");

        var listener = new TalariaListener(
            transport,
            topicReg,
            new SagaRegistry(),
            new TalariaOptions { ApplicationName = "scope-test" },
            NullLogger<TalariaListener>.Instance,
            services);

        await listener.StartAsync();

        var producer = await transport.CreateProducerAsync<ScopeMessage>("scope.dispose.topic", new ProducerOptions());
        for (int i = 0; i < 2; i++)
        {
            await producer.ProduceAsync(new ScopeMessage { Id = $"msg-{i}" });
        }

        await TestAsyncHelpers.WaitUntilAsync(() => disposedCount == 2);
        Assert.Equal(2, trackers.Count);

        await listener.StopAsync();
    }

    [Fact]
    public async Task Context_Services_Resolves_Scoped_Services()
    {
        var transport = new InMemoryTransport();
        var capturedIds = new List<Guid>();

        var services = new ServiceCollection()
            .AddSingleton(transport)
            .AddSingleton(capturedIds)
            .AddScoped<ScopedDependency>()
            .AddScoped<ContextServicesConsumer>()
            .BuildServiceProvider();

        var topicReg = new TopicRegistry();
        topicReg.MapTopic<ScopeMessage, ContextServicesConsumer>("scope.context.topic");

        var listener = new TalariaListener(
            transport,
            topicReg,
            new SagaRegistry(),
            new TalariaOptions { ApplicationName = "scope-test" },
            NullLogger<TalariaListener>.Instance,
            services);

        await listener.StartAsync();

        var producer = await transport.CreateProducerAsync<ScopeMessage>("scope.context.topic", new ProducerOptions());
        await producer.ProduceAsync(new ScopeMessage { Id = "ctx-1" });

        await TestAsyncHelpers.WaitUntilAsync(() => capturedIds.Count == 1);

        Assert.Single(capturedIds);

        await listener.StopAsync();
    }

    [Fact]
    public async Task Constructor_Throwing_Consumer_Follows_Retry_Policy_And_DeadLetters()
    {
        var transport = new InMemoryTransport();
        var deferralStore = new InMemoryDeferralStore();

        var services = new ServiceCollection()
            .AddSingleton(transport)
            .AddScoped<ThrowingConstructorConsumer>()
            .BuildServiceProvider();

        var topicReg = new TopicRegistry();
        topicReg.MapTopic<ScopeMessage, ThrowingConstructorConsumer>("scope.throw.topic", new RetryPolicy
        {
            MaxRetryAttempts = 1,
            RetryInterval = TimeSpan.FromMilliseconds(10),
        });

        var listener = new TalariaListener(
            transport,
            topicReg,
            new SagaRegistry(),
            new TalariaOptions
            {
                ApplicationName = "scope-test",
                DeferralBackoff = TimeSpan.FromMilliseconds(10),
            },
            NullLogger<TalariaListener>.Instance,
            services,
            new TalariaListenerStores(DeferralStore: deferralStore));

        await listener.StartAsync();

        var producer = await transport.CreateProducerAsync<ScopeMessage>("scope.throw.topic", new ProducerOptions());
        await producer.ProduceAsync(new ScopeMessage { Id = "throw-1" });

        var dlqMessages = await TestAsyncHelpers.ReadUntilAsync<ScopeMessage>(transport, "scope.throw.topic.dlq", 1);
        var envelope = Assert.Single(dlqMessages);
        Assert.Equal("throw-1", envelope.Payload!.Id);
        Assert.Equal("retries_exhausted", envelope.Headers.DlqReason);

        await listener.StopAsync();
    }

    [Fact]
    public void TopicConsumerEngine_ClassConsumer_Without_ServiceProvider_Throws_InvalidOperationException()
    {
        var transport = new InMemoryTransport();
        var topicReg = new TopicRegistry();
        topicReg.MapTopic<ScopeMessage, CapturingConsumer>("engine-guard.topic");

        var ex = Assert.Throws<InvalidOperationException>(() => new TopicConsumerEngine(
            transport,
            topicReg,
            new TalariaOptions { ApplicationName = "engine-guard" },
            null,
            new MessageProcessingPipeline(new InMemoryIdempotencyStore(), new TalariaOptions(), NullLogger<TalariaListener>.Instance),
            NullLogger<TalariaListener>.Instance,
            serviceProvider: null));

        Assert.Contains("engine-guard.topic", ex.Message);
        Assert.Contains("IServiceProvider", ex.Message);
    }

    [Fact]
    public async Task Scope_Disposal_Fault_Is_Logged_And_Original_Handler_Exception_Drives_Retry()
    {
        var transport = new InMemoryTransport();
        var deferralStore = new InMemoryDeferralStore();
        var observedOrder = new List<string>();
        var loggerProvider = new TestLoggerProvider();
        var loggerFactory = new LoggerFactory();
        loggerFactory.AddProvider(loggerProvider);

        var services = new ServiceCollection()
            .AddSingleton(transport)
            .AddSingleton(observedOrder)
            .AddScoped<ThrowingDisposableTracker>()
            .AddScoped<ScopeDisposalTrackingConsumer>()
            .BuildServiceProvider();

        var topicReg = new TopicRegistry();
        topicReg.MapTopic<ScopeMessage, ScopeDisposalTrackingConsumer>("scope.disposal-fault.topic", new RetryPolicy
        {
            MaxRetryAttempts = 0,
            RetryInterval = TimeSpan.FromMilliseconds(10),
        });

        var listener = new TalariaListener(
            transport,
            topicReg,
            new SagaRegistry(),
            new TalariaOptions
            {
                ApplicationName = "scope-disposal-fault",
                IncludeExceptionDetailsInDlq = true,
            },
            loggerFactory.CreateLogger<TalariaListener>(),
            services,
            new TalariaListenerStores(DeferralStore: deferralStore));

        await listener.StartAsync();

        var producer = await transport.CreateProducerAsync<ScopeMessage>("scope.disposal-fault.topic", new ProducerOptions());
        await producer.ProduceAsync(new ScopeMessage { Id = "fault-1" });

        var dlqMessages = await TestAsyncHelpers.ReadUntilAsync<ScopeMessage>(transport, "scope.disposal-fault.topic.dlq", 1);
        var envelope = Assert.Single(dlqMessages);
        Assert.Equal("fault-1", envelope.Payload!.Id);
        Assert.Equal("handler failure", envelope.Headers.DlqException);

        Assert.Contains(loggerProvider.Entries, e =>
            e.Message.Contains("Scope disposal for topic 'scope.disposal-fault.topic' failed while a handler exception was already in flight"));

        await listener.StopAsync();
    }

    [Fact]
    public async Task Scope_Disposal_Fault_After_Successful_Handler_Is_Logged_And_Message_Is_Committed()
    {
        var transport = new InMemoryTransport();
        var handled = false;
        var loggerProvider = new TestLoggerProvider();
        var loggerFactory = new LoggerFactory();
        loggerFactory.AddProvider(loggerProvider);

        var services = new ServiceCollection()
            .AddSingleton(transport)
            .AddScoped<ThrowingDisposeDependency>()
            .AddScoped<SucceedingWithDisposableConsumer>(sp =>
                new SucceedingWithDisposableConsumer(
                    sp.GetRequiredService<ThrowingDisposeDependency>(),
                    () => handled = true))
            .BuildServiceProvider();

        var topicReg = new TopicRegistry();
        topicReg.MapTopic<ScopeMessage, SucceedingWithDisposableConsumer>("scope.dispose-after-success.topic");

        var listener = new TalariaListener(
            transport,
            topicReg,
            new SagaRegistry(),
            new TalariaOptions
            {
                ApplicationName = "scope-dispose-after-success",
                IncludeExceptionDetailsInDlq = true,
            },
            loggerFactory.CreateLogger<TalariaListener>(),
            services);

        await listener.StartAsync();

        var producer = await transport.CreateProducerAsync<ScopeMessage>("scope.dispose-after-success.topic", new ProducerOptions());
        await producer.ProduceAsync(new ScopeMessage { Id = "success-dispose-fault" });

        await TestAsyncHelpers.WaitUntilAsync(() => handled);

        // The handler succeeded, so the message must be committed — no DLQ, no retry.
        Assert.Empty(await transport.ReadAllFromTopicAsync<ScopeMessage>("scope.dispose-after-success.topic.dlq"));
        Assert.Contains(loggerProvider.Entries, e =>
            e.Message.Contains("Scope disposal for topic 'scope.dispose-after-success.topic' failed after the handler succeeded"));

        await listener.StopAsync();
    }

    [Fact]
    public async Task Scope_Is_Disposed_Before_Retry_Coordination()
    {
        var transport = new InMemoryTransport();
        var deferralStore = new InMemoryDeferralStore();
        var disposedCount = 0;

        var services = new ServiceCollection()
            .AddSingleton(transport)
            .AddScoped<AsyncDisposableTracker>(_ =>
                new AsyncDisposableTracker(() => Interlocked.Increment(ref disposedCount)))
            .AddScoped<OrderTrackingConsumer>()
            .BuildServiceProvider();

        var topicReg = new TopicRegistry();
        topicReg.MapTopic<ScopeMessage, OrderTrackingConsumer>("scope.dispose-before-retry.topic", new RetryPolicy
        {
            MaxRetryAttempts = 1,
            RetryInterval = TimeSpan.FromMilliseconds(10),
        });

        var listener = new TalariaListener(
            transport,
            topicReg,
            new SagaRegistry(),
            new TalariaOptions
            {
                ApplicationName = "scope-dispose-before-retry",
                DeferralBackoff = TimeSpan.FromMilliseconds(10),
            },
            NullLogger<TalariaListener>.Instance,
            services,
            new TalariaListenerStores(DeferralStore: deferralStore));

        await listener.StartAsync();

        var producer = await transport.CreateProducerAsync<ScopeMessage>("scope.dispose-before-retry.topic", new ProducerOptions());
        await producer.ProduceAsync(new ScopeMessage { Id = "order-1" });

        // The retry copy will eventually DLQ; wait for that and verify the scope was disposed
        // (at least once for the original attempt) before retry coordination produced the DLQ.
        var dlqMessages = await TestAsyncHelpers.ReadUntilAsync<ScopeMessage>(
            transport,
            "scope.dispose-before-retry.topic.dlq",
            1,
            TimeSpan.FromSeconds(15));

        var envelope = Assert.Single(dlqMessages);
        Assert.Equal("order-1", envelope.Payload!.Id);
        Assert.Equal("retries_exhausted", envelope.Headers.DlqReason);
        Assert.True(disposedCount >= 1, "Scope should have been disposed before retry coordination completed.");

        await listener.StopAsync();
    }

    [Fact]
    public async Task Fresh_Scope_Per_Retry_Copy()
    {
        var transport = new InMemoryTransport();
        var deferralStore = new InMemoryDeferralStore();
        var capturedIds = new List<int>();

        var services = new ServiceCollection()
            .AddSingleton(transport)
            .AddSingleton(capturedIds)
            .AddScoped<InstanceCounter>()
            .AddScoped<InstanceCapturingConsumer>()
            .BuildServiceProvider();

        var topicReg = new TopicRegistry();
        topicReg.MapTopic<ScopeMessage, InstanceCapturingConsumer>("scope.fresh-per-retry.topic", new RetryPolicy
        {
            MaxRetryAttempts = 1,
            RetryInterval = TimeSpan.FromMilliseconds(10),
        });

        var listener = new TalariaListener(
            transport,
            topicReg,
            new SagaRegistry(),
            new TalariaOptions
            {
                ApplicationName = "scope-fresh-per-retry",
                DeferralBackoff = TimeSpan.FromMilliseconds(10),
            },
            NullLogger<TalariaListener>.Instance,
            services,
            new TalariaListenerStores(DeferralStore: deferralStore));

        await listener.StartAsync();

        var producer = await transport.CreateProducerAsync<ScopeMessage>("scope.fresh-per-retry.topic", new ProducerOptions());
        await producer.ProduceAsync(new ScopeMessage { Id = "retry-scope-1" });

        // Wait until both the original attempt and the retry attempt have been recorded.
        await TestAsyncHelpers.WaitUntilAsync(() => capturedIds.Count == 2, TimeSpan.FromSeconds(15));

        Assert.Equal(2, capturedIds.Distinct().Count());

        await listener.StopAsync();
    }

    [Fact]
    public async Task Duplicate_Delivery_With_Idempotency_Does_Not_Resolve_Consumer_Instance()
    {
        var transport = new InMemoryTransport();
        var countingStore = new CountingIdempotencyStore(new InMemoryIdempotencyStore());
        Interlocked.Exchange(ref ConstructionCountingConsumer.ConstructionCount, 0);

        var services = new ServiceCollection()
            .AddSingleton(transport)
            .AddScoped<ConstructionCountingConsumer>()
            .BuildServiceProvider();

        var topicReg = new TopicRegistry();
        topicReg.MapTopic<ScopeMessage, ConstructionCountingConsumer>("scope.idempotent.topic");

        var listener = new TalariaListener(
            transport,
            topicReg,
            new SagaRegistry(),
            new TalariaOptions { ApplicationName = "scope-idempotent" },
            NullLogger<TalariaListener>.Instance,
            services,
            new TalariaListenerStores(IdempotencyStore: countingStore));

        await listener.StartAsync();

        var producer = await transport.CreateProducerAsync<ScopeMessage>("scope.idempotent.topic", new ProducerOptions());
        var messageId = "duplicate-msg";
        await producer.ProduceAsync(new ScopeMessage { Id = "dup-1" }, new MessageHeaders { MessageId = messageId });
        await producer.ProduceAsync(new ScopeMessage { Id = "dup-1" }, new MessageHeaders { MessageId = messageId });

        // Wait until the engine has observed both deliveries through the idempotency gate
        // before asserting that only one consumer instance was constructed.
        await TestAsyncHelpers.WaitUntilAsync(() => countingStore.AcquireCount >= 2);
        Assert.Equal(1, ConstructionCountingConsumer.ConstructionCount);

        await listener.StopAsync();
    }

    [Fact]
    public void IServiceProviderIsService_Absent_Fallback_Allows_Registration()
    {
        var registry = new TopicRegistry();
        var stubProvider = new NoIsServiceProvider(registry);

        stubProvider.MapTopic<ScopeMessage, CapturingConsumer>("no-is-service.topic");

        var reg = Assert.Single(registry.Registrations);
        Assert.Equal(typeof(CapturingConsumer), reg.ConsumerType);
    }

}
