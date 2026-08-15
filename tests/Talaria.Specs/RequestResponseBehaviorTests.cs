// SPDX-License-Identifier: Apache-2.0

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

            Assert.NotNull(ex.RequestId);
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

        var listener = new TalariaListener(
            transport,
            topicRegistry,
            new SagaRegistry(),
            new TalariaOptions { ApplicationName = "rr-test" },
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
        loggerFactory.Dispose();
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
