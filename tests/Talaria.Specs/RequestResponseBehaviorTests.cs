// SPDX-License-Identifier: Apache-2.0

using System.Collections.Concurrent;
using System.Threading;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Talaria.Core;
using Talaria.Core.Abstractions;
using Talaria.Core.Hosting;
using Talaria.Core.Registration;
using Talaria.Core.Requesting;
using Talaria.Core.Sagas;
using Talaria.Transports.InMemory;

namespace Talaria.Specs.Tests;

/// <summary>
/// End-to-end behavior tests for request/response messaging with <c>MapRequest</c> responders and a manually started <see cref="TalariaListener"/>.
/// </summary>
public class RequestResponseBehaviorTests
{
    private sealed record Ping(string Value);
    private sealed record Pong(string Echo);

    private sealed class ScopeCounter
    {
        public int InstanceId { get; } = Interlocked.Increment(ref _nextId);
        private static int _nextId;
    }

    private sealed class ClassResponder : IRequestConsumer<Ping, Pong>
    {
        private readonly ScopeCounter _counter;
        private readonly List<int> _instanceIds;

        public ClassResponder(ScopeCounter counter, List<int> instanceIds)
        {
            _counter = counter;
            _instanceIds = instanceIds;
        }

        public Task<Pong> ConsumeAsync(ConsumeContext<Ping> context, CancellationToken ct = default)
        {
            _instanceIds.Add(_counter.InstanceId);
            return Task.FromResult(new Pong(context.Message.Value));
        }
    }

    private sealed class FaultingResponder : IRequestConsumer<Ping, Pong>
    {
        public Task<Pong> ConsumeAsync(ConsumeContext<Ping> context, CancellationToken ct = default)
        {
            throw new InvalidOperationException("responder failure");
        }
    }

    private sealed class CollectingLogger : ILogger
    {
        public List<(LogLevel Level, string Message)> Entries { get; } = new();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            Entries.Add((logLevel, formatter(state, exception)));
        }
    }

    [Fact]
    public async Task Delegate_Responder_Returns_Typed_Response()
    {
        var transport = new InMemoryTransport();
        var topicRegistry = new TopicRegistry();
        topicRegistry.MapRequest<Ping, Pong>("ping.topic", async (msg, _, _, ct) => new Pong(msg.Value));

        var listener = new TalariaListener(
            transport,
            topicRegistry,
            new SagaRegistry(),
            new TalariaOptions { ApplicationName = "rr-test" },
            NullLogger<TalariaListener>.Instance);

        var factory = new RequestClientFactory(
            transport,
            new TalariaOptions { ApplicationName = "rr-test" },
            NullLoggerFactory.Instance);

        await using (factory.ConfigureAwait(false))
        {
            var client = factory.CreateClient<Ping>("ping.topic");
            await listener.StartAsync();

            var response = await client.GetResponseAsync<Pong>(new Ping("hello"));

            Assert.Equal("hello", response.Echo);
            await listener.StopAsync();
        }
    }

    [Fact]
    public async Task Class_Consumer_Responder_Returns_Typed_Response_From_Scope()
    {
        var transport = new InMemoryTransport();
        var instanceIds = new List<int>();

        var services = new ServiceCollection()
            .AddSingleton(transport)
            .AddSingleton(instanceIds)
            .AddScoped<ScopeCounter>()
            .AddScoped<ClassResponder>()
            .BuildServiceProvider();

        var topicRegistry = new TopicRegistry();
        topicRegistry.MapRequest<Ping, ClassResponder, Pong>("ping.class.topic");

        var listener = new TalariaListener(
            transport,
            topicRegistry,
            new SagaRegistry(),
            new TalariaOptions { ApplicationName = "rr-test" },
            NullLogger<TalariaListener>.Instance,
            services);

        var factory = new RequestClientFactory(
            transport,
            new TalariaOptions { ApplicationName = "rr-test" },
            NullLoggerFactory.Instance);

        await using (factory.ConfigureAwait(false))
        {
            var client = factory.CreateClient<Ping>("ping.class.topic");
            await listener.StartAsync();

            var response = await client.GetResponseAsync<Pong>(new Ping("class"));

            Assert.Equal("class", response.Echo);
            Assert.Single(instanceIds);
            await listener.StopAsync();
        }
    }

    [Fact]
    public async Task Concurrent_Requests_Each_Receive_Correct_Response()
    {
        var transport = new InMemoryTransport();
        var topicRegistry = new TopicRegistry();
        topicRegistry.MapRequest<Ping, Pong>("ping.concurrent.topic", async (msg, _, _, ct) => new Pong(msg.Value));

        var listener = new TalariaListener(
            transport,
            topicRegistry,
            new SagaRegistry(),
            new TalariaOptions { ApplicationName = "rr-test" },
            NullLogger<TalariaListener>.Instance);

        var factory = new RequestClientFactory(
            transport,
            new TalariaOptions { ApplicationName = "rr-test" },
            NullLoggerFactory.Instance);

        await using (factory.ConfigureAwait(false))
        {
            var client = factory.CreateClient<Ping>("ping.concurrent.topic");
            await listener.StartAsync();

            var tasks = Enumerable.Range(0, 20)
                .Select(i => client.GetResponseAsync<Pong>(new Ping($"ping-{i}")))
                .ToList();

            var responses = await Task.WhenAll(tasks);

            var echoes = responses.Select(r => r.Echo).OrderBy(x => x).ToList();
            var expected = Enumerable.Range(0, 20).Select(i => $"ping-{i}").OrderBy(x => x).ToList();
            Assert.Equal(expected, echoes);

            await listener.StopAsync();
        }
    }

    [Fact]
    public async Task No_Responder_Throws_RequestTimeoutException()
    {
        var transport = new InMemoryTransport();
        var listener = new TalariaListener(
            transport,
            new TopicRegistry(),
            new SagaRegistry(),
            new TalariaOptions { ApplicationName = "rr-test" },
            NullLogger<TalariaListener>.Instance);

        var factory = new RequestClientFactory(
            transport,
            new TalariaOptions { ApplicationName = "rr-test", DefaultRequestTimeout = TimeSpan.FromMilliseconds(100) },
            NullLoggerFactory.Instance);

        await using (factory.ConfigureAwait(false))
        {
            var client = factory.CreateClient<Ping>("ping.timeout.topic");
            await listener.StartAsync();

            var ex = await Assert.ThrowsAsync<RequestTimeoutException>(() =>
                client.GetResponseAsync<Pong>(new Ping("timeout")));

            Assert.Contains(ex.RequestId, ex.Message);
            await listener.StopAsync();
        }
    }

    [Fact]
    public async Task Faulting_Responder_Throws_RequestFaultException_Immediately_Without_Retries()
    {
        var transport = new InMemoryTransport();
        var topicRegistry = new TopicRegistry();
        topicRegistry.MapRequest<Ping, Pong>("ping.fault.topic", async (msg, _, _, ct) => throw new InvalidOperationException("boom"));

        var listener = new TalariaListener(
            transport,
            topicRegistry,
            new SagaRegistry(),
            new TalariaOptions { ApplicationName = "rr-test", IncludeExceptionDetailsInDlq = true },
            NullLogger<TalariaListener>.Instance);

        var factory = new RequestClientFactory(
            transport,
            new TalariaOptions { ApplicationName = "rr-test" },
            NullLoggerFactory.Instance);

        await using (factory.ConfigureAwait(false))
        {
            var client = factory.CreateClient<Ping>("ping.fault.topic");
            await listener.StartAsync();

            var ex = await Assert.ThrowsAsync<RequestFaultException>(() =>
                client.GetResponseAsync<Pong>(new Ping("fault")));

            Assert.Equal("boom", ex.ResponderMessage);
            Assert.Equal(typeof(InvalidOperationException).FullName, ex.ResponderExceptionType);
            await listener.StopAsync();
        }
    }

    [Fact]
    public async Task Faulting_Responder_With_Retries_Throws_RequestFaultException_After_Exhaustion()
    {
        var transport = new InMemoryTransport();
        var deferralStore = new InMemoryDeferralStore();

        var topicRegistry = new TopicRegistry();
        topicRegistry.MapRequest<Ping, Pong>(
            "ping.fault.retry.topic",
            async (msg, _, _, ct) => throw new InvalidOperationException("retry-boom"),
            new RetryPolicy { MaxRetryAttempts = 1, RetryInterval = TimeSpan.FromMilliseconds(10) });

        var listener = new TalariaListener(
            transport,
            topicRegistry,
            new SagaRegistry(),
            new TalariaOptions
            {
                ApplicationName = "rr-test",
                IncludeExceptionDetailsInDlq = true,
                DeferralBackoff = TimeSpan.FromMilliseconds(10),
            },
            NullLogger<TalariaListener>.Instance,
            null,
            new TalariaListenerStores(DeferralStore: deferralStore));

        var factory = new RequestClientFactory(
            transport,
            new TalariaOptions { ApplicationName = "rr-test" },
            NullLoggerFactory.Instance);

        await using (factory.ConfigureAwait(false))
        {
            var client = factory.CreateClient<Ping>("ping.fault.retry.topic");
            await listener.StartAsync();

            var ex = await Assert.ThrowsAsync<RequestFaultException>(() =>
                client.GetResponseAsync<Pong>(new Ping("fault-retry")));

            Assert.Equal("retry-boom", ex.ResponderMessage);
            await listener.StopAsync();
        }
    }

    [Fact]
    public async Task Two_Factories_Have_Isolated_Inboxes()
    {
        var transport = new InMemoryTransport();
        var topicRegistry = new TopicRegistry();
        topicRegistry.MapRequest<Ping, Pong>("ping.isolated.topic", async (msg, _, _, ct) => new Pong(msg.Value));

        var listener = new TalariaListener(
            transport,
            topicRegistry,
            new SagaRegistry(),
            new TalariaOptions { ApplicationName = "rr-test" },
            NullLogger<TalariaListener>.Instance);

        var optionsA = new TalariaOptions { ApplicationName = "rr-test" };
        var optionsB = new TalariaOptions { ApplicationName = "rr-test" };

        var factoryA = new RequestClientFactory(transport, optionsA, NullLoggerFactory.Instance);
        var factoryB = new RequestClientFactory(transport, optionsB, NullLoggerFactory.Instance);

        Assert.NotEqual(factoryA.InboxTopic, factoryB.InboxTopic);

        await using (factoryA.ConfigureAwait(false))
        await using (factoryB.ConfigureAwait(false))
        {
            var clientA = factoryA.CreateClient<Ping>("ping.isolated.topic");
            var clientB = factoryB.CreateClient<Ping>("ping.isolated.topic");
            await listener.StartAsync();

            var taskA = clientA.GetResponseAsync<Pong>(new Ping("a"));
            var taskB = clientB.GetResponseAsync<Pong>(new Ping("b"));

            var (responseA, responseB) = (await taskA, await taskB);

            Assert.Equal("a", responseA.Echo);
            Assert.Equal("b", responseB.Echo);

            await listener.StopAsync();
        }
    }

    [Fact]
    public async Task Request_Without_ReplyTo_Logs_Warning_And_Commits()
    {
        var transport = new InMemoryTransport();
        var logger = new CollectingLogger();
        var loggerFactory = LoggerFactory.Create(b => b.AddProvider(new SimpleLoggerProvider(logger)));

        var topicRegistry = new TopicRegistry();
        topicRegistry.MapRequest<Ping, Pong>("ping.no-reply.topic", async (msg, _, _, ct) => new Pong(msg.Value));

        var consumerGroup = "no-reply-group";
        var listener = new TalariaListener(
            transport,
            topicRegistry,
            new SagaRegistry(),
            new TalariaOptions { ApplicationName = "rr-test", ConsumerGroupOverride = consumerGroup },
            loggerFactory.CreateLogger<TalariaListener>());

        await listener.StartAsync();

        var producer = await transport.CreateProducerAsync<Ping>("ping.no-reply.topic", new ProducerOptions());
        await producer.ProduceAsync(new Ping("no-reply"));

        var warned = await TestAsyncHelpers.PollUntilAsync(
            () => Task.FromResult(logger.Entries.Any(e =>
                e.Level == LogLevel.Warning &&
                e.Message.Contains("no") &&
                e.Message.Contains("reply"))),
            TimeSpan.FromSeconds(5));

        Assert.True(warned, "Expected warning about missing reply_to header.");

        await listener.StopAsync();

        // A committed message must not redeliver to a new consumer on the same group.
        var verificationConsumer = await transport.CreateConsumerAsync<Ping>("ping.no-reply.topic", new ConsumerOptions { ConsumerGroup = consumerGroup });
        await using (verificationConsumer.ConfigureAwait(false))
        {
            using var verificationCts = new CancellationTokenSource(TimeSpan.FromMilliseconds(200));
            var enumerator = verificationConsumer.ConsumeAsync(verificationCts.Token).GetAsyncEnumerator();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
                enumerator.MoveNextAsync().AsTask());
        }

        loggerFactory.Dispose();
    }


    [Fact]
    public async Task ClassConsumer_Scope_Disposal_Failure_After_Success_Still_Publishes_Response()
    {
        var transport = new InMemoryTransport();
        var logger = new CollectingLogger();
        var loggerFactory = LoggerFactory.Create(b => b.AddProvider(new SimpleLoggerProvider(logger)));

        var services = new ServiceCollection()
            .AddSingleton(transport)
            .AddScoped<ScopeDisposingDependency>()
            .AddScoped<ClassResponderWithDisposableDependency>()
            .BuildServiceProvider();

        var topicRegistry = new TopicRegistry();
        topicRegistry.MapRequest<Ping, ClassResponderWithDisposableDependency, Pong>("ping.scope-dispose.topic");

        var listener = new TalariaListener(
            transport,
            topicRegistry,
            new SagaRegistry(),
            new TalariaOptions { ApplicationName = "rr-test", IncludeExceptionDetailsInDlq = true },
            loggerFactory.CreateLogger<TalariaListener>(),
            services);

        var factory = new RequestClientFactory(
            transport,
            new TalariaOptions { ApplicationName = "rr-test" },
            NullLoggerFactory.Instance);

        await using (factory.ConfigureAwait(false))
        {
            var client = factory.CreateClient<Ping>("ping.scope-dispose.topic");
            await listener.StartAsync();

            var response = await client.GetResponseAsync<Pong>(new Ping("scope-dispose"));
            Assert.Equal("scope-dispose", response.Echo);

            var logged = await TestAsyncHelpers.PollUntilAsync(
                () => Task.FromResult(logger.Entries.Any(e =>
                    e.Level == LogLevel.Error &&
                    e.Message.Contains("Scope disposal") &&
                    e.Message.Contains("ping.scope-dispose.topic"))),
                TimeSpan.FromSeconds(5));

            Assert.True(logged, "Expected error log for failing scope disposal after handler success.");

            await listener.StopAsync();
        }

        loggerFactory.Dispose();
    }

    [Fact]
    public async Task Response_Publish_Failure_Flows_Through_Retry_And_Handler_May_Run_Multiple_Times()
    {
        var inner = new InMemoryTransport();
        var transport = new ReplyFailingInMemoryTransport(inner);
        var deferralStore = new InMemoryDeferralStore();

        var invocationCount = 0;
        var topicRegistry = new TopicRegistry();
        topicRegistry.MapRequest<Ping, Pong>(
            "ping.reply-fail.topic",
            async (msg, _, _, ct) =>
            {
                Interlocked.Increment(ref invocationCount);
                return new Pong(msg.Value);
            },
            new RetryPolicy { MaxRetryAttempts = 2, RetryInterval = TimeSpan.FromMilliseconds(10) });

        var listener = new TalariaListener(
            transport,
            topicRegistry,
            new SagaRegistry(),
            new TalariaOptions { ApplicationName = "rr-test", DeferralBackoff = TimeSpan.FromMilliseconds(10) },
            NullLogger<TalariaListener>.Instance,
            null,
            new TalariaListenerStores(DeferralStore: deferralStore));

        var factory = new RequestClientFactory(
            transport,
            new TalariaOptions { ApplicationName = "rr-test", DefaultRequestTimeout = TimeSpan.FromSeconds(10) },
            NullLoggerFactory.Instance);

        // Fail the first two response publishes to the reply topic; the retry path must eventually deliver.
        transport.SetReplyFailures(factory.InboxTopic, typeof(Pong), 2);

        await using (factory.ConfigureAwait(false))
        {
            var client = factory.CreateClient<Ping>("ping.reply-fail.topic");
            await listener.StartAsync();

            var response = await client.GetResponseAsync<Pong>(new Ping("retry-response"));

            Assert.Equal("retry-response", response.Echo);
            Assert.True(invocationCount >= 2, $"Handler should run more than once when response publish fails; ran {invocationCount} times.");

            await listener.StopAsync();
        }
    }

    private sealed class ScopeDisposingDependency : IAsyncDisposable
    {
        public ValueTask DisposeAsync()
            => ValueTask.FromException(new InvalidOperationException("scope disposal failure"));
    }

    private sealed class ClassResponderWithDisposableDependency : IRequestConsumer<Ping, Pong>
    {
        private readonly ScopeDisposingDependency _dependency;

        public ClassResponderWithDisposableDependency(ScopeDisposingDependency dependency)
        {
            _dependency = dependency;
        }

        public Task<Pong> ConsumeAsync(ConsumeContext<Ping> context, CancellationToken ct = default)
        {
            _ = _dependency;
            return Task.FromResult(new Pong(context.Message.Value));
        }
    }

    /// <summary>
    /// Wraps an in-memory transport and makes producers to reply topics fail a configurable
    /// number of times. Used to verify that a failed response publish is treated like a
    /// handler failure and flows through retries.
    /// </summary>
    private sealed class ReplyFailingInMemoryTransport : ITransport
    {
        private readonly InMemoryTransport _inner;
        private readonly ConcurrentDictionary<(string Topic, Type MessageType), int> _remainingFailures = new();

        public ReplyFailingInMemoryTransport(InMemoryTransport inner)
        {
            _inner = inner;
        }

        public string Name => _inner.Name;

        public InMemoryTransport InnerTransport => _inner;

        public void SetReplyFailures(string replyTopic, Type messageType, int count)
        {
            _remainingFailures[(replyTopic, messageType)] = count;
        }

        public Task<IConsumer<T>> CreateConsumerAsync<T>(string topic, ConsumerOptions options, CancellationToken ct = default)
            => _inner.CreateConsumerAsync<T>(topic, options, ct);

        public Task<IProducer<T>> CreateProducerAsync<T>(string topic, ProducerOptions options, CancellationToken ct = default)
        {
            var innerTask = _inner.CreateProducerAsync<T>(topic, options, ct);
            if (topic.Contains("-replies-", StringComparison.Ordinal))
            {
                return WrapReplyProducerAsync(innerTask, topic, typeof(T));
            }

            return innerTask;
        }

        public Task<ITransactionalSession> BeginTransactionAsync(
            string? consumerGroup = null, TransactionOffsetSource? offsetSource = null, CancellationToken ct = default)
            => _inner.BeginTransactionAsync(consumerGroup, offsetSource, ct);

        private async Task<IProducer<T>> WrapReplyProducerAsync<T>(Task<IProducer<T>> innerTask, string topic, Type messageType)
        {
            var inner = await innerTask;
            return new FailingReplyProducer<T>(inner, this, topic, messageType);
        }

        private sealed class FailingReplyProducer<T> : IProducer<T>
        {
            private readonly IProducer<T> _inner;
            private readonly ReplyFailingInMemoryTransport _transport;
            private readonly string _topic;
            private readonly Type _messageType;

            public FailingReplyProducer(
                IProducer<T> inner,
                ReplyFailingInMemoryTransport transport,
                string topic,
                Type messageType)
            {
                _inner = inner;
                _transport = transport;
                _topic = topic;
                _messageType = messageType;
            }

            public Task ProduceAsync(T message, MessageHeaders? headers = null, string? partitionKey = null, CancellationToken ct = default)
            {
                var key = (_topic, _messageType);
                if (_transport._remainingFailures.TryGetValue(key, out var remaining) && remaining > 0)
                {
                    _transport._remainingFailures[key] = remaining - 1;
                    throw new InvalidOperationException($"Simulated reply publish failure for {_topic}<{_messageType.Name}>.");
                }

                return _inner.ProduceAsync(message, headers, partitionKey, ct);
            }

            public ValueTask DisposeAsync()
                => _inner.DisposeAsync();
        }
    }

    private sealed class SimpleLoggerProvider : ILoggerProvider
    {
        private readonly ILogger _logger;

        public SimpleLoggerProvider(ILogger logger)
        {
            _logger = logger;
        }

        public ILogger CreateLogger(string categoryName) => _logger;
        public void Dispose() { }
    }
}
