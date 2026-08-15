// SPDX-License-Identifier: Apache-2.0

using Microsoft.Extensions.DependencyInjection;
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

        await WaitUntilAsync(() => capturedIds.Count == 3);

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

        await WaitUntilAsync(() => disposedCount == 2);
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

        await WaitUntilAsync(() => capturedIds.Count == 1);

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

        var dlqMessages = await ReadUntilAsync<ScopeMessage>(transport, "scope.throw.topic.dlq", 1);
        var envelope = Assert.Single(dlqMessages);
        Assert.Equal("throw-1", envelope.Payload!.Id);
        Assert.Equal("retries_exhausted", envelope.Headers.DlqReason);

        await listener.StopAsync();
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
}
