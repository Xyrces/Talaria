// SPDX-License-Identifier: Apache-2.0

using Microsoft.Extensions.Logging.Abstractions;
using Talaria.Core.Abstractions;
using Talaria.Core.Hosting;
using Talaria.Core.Registration;
using Talaria.Core.Requesting;
using Talaria.Core.Sagas;
using Talaria.Transports.InMemory;

namespace Talaria.Core.Tests;

public class RequestClientFactoryTests
{
    [Fact]
    public void TwoFactories_HaveDistinctInboxTopics()
    {
        var options = new TalariaOptions { ApplicationName = "test-app" };
        var transport = new InMemoryTransport();

        var factory1 = new RequestClientFactory(transport, options, NullLoggerFactory.Instance);
        var factory2 = new RequestClientFactory(transport, options, NullLoggerFactory.Instance);

        Assert.NotEqual(factory1.InboxTopic, factory2.InboxTopic);
        Assert.StartsWith("test-app-replies-", factory1.InboxTopic);
        Assert.StartsWith("test-app-replies-", factory2.InboxTopic);
    }

    [Fact]
    public async Task InboxPump_RestartsAfterTransientConsumerFailure()
    {
        // The first attempt to create the inbox consumer faults; supervision must
        // restart the pump so the request still completes instead of hanging.
        var inner = new InMemoryTransport();
        var transport = new FailOnceConsumerTransport(inner);
        var options = new TalariaOptions { ApplicationName = "rr-restart" };

        var responder = new TalariaListener(
            inner,
            new TopicRegistry().MapRequest<Ping, Pong>(
                "ping.restart", (msg, _, _, _) => Task.FromResult(new Pong(msg.Value))),
            new SagaRegistry(),
            options,
            NullLogger<TalariaListener>.Instance);
        await responder.StartAsync();

        var factory = new RequestClientFactory(transport, options, NullLoggerFactory.Instance);
        await using (factory.ConfigureAwait(false))
        {
            var client = factory.CreateClient<Ping>("ping.restart");
            var response = await client.GetResponseAsync<Pong>(new Ping("hello"));
            Assert.Equal("hello", response.Echo);
        }

        await responder.StopAsync();
        Assert.True(transport.InboxCreateAttempts >= 2, "The pump should have retried inbox consumer creation after the transient failure.");
    }

    [Fact]
    public async Task Caller_Cancellation_Throws_OperationCanceledException_Not_Timeout()
    {
        var transport = new InMemoryTransport();
        var options = new TalariaOptions
        {
            ApplicationName = "rr-cancel",
            DefaultRequestTimeout = TimeSpan.FromSeconds(30),
        };

        var factory = new RequestClientFactory(transport, options, NullLoggerFactory.Instance);
        await using (factory.ConfigureAwait(false))
        {
            var client = factory.CreateClient<Ping>("ping.cancel.topic");
            using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));

            var ex = await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
                client.GetResponseAsync<Pong>(new Ping("cancel"), cts.Token));

            Assert.IsNotType<RequestTimeoutException>(ex);
        }
    }

    [Fact]
    public async Task Duplicate_Response_Is_Ignored_After_First_Wins()
    {
        var inner = new InMemoryTransport();
        var transport = new RecordingTransport(inner);
        var options = new TalariaOptions
        {
            ApplicationName = "rr-dup",
            DefaultRequestTimeout = TimeSpan.FromSeconds(30),
        };

        var factory = new RequestClientFactory(transport, options, NullLoggerFactory.Instance);
        await using (factory.ConfigureAwait(false))
        {
            var client = factory.CreateClient<Ping>("ping.dup.topic");

            var requestTask = client.GetResponseAsync<Pong>(new Ping("dup"));

            // Wait for the request to be published so we have its request id.
            var requestHeaders = await transport.WaitForRequestAsync("ping.dup.topic", TimeSpan.FromSeconds(5));
            var requestId = requestHeaders.RequestId;
            Assert.NotNull(requestId);

            // Produce two responses with the same request id; the first must win.
            var replyProducer = await inner.CreateProducerAsync<Pong>(factory.InboxTopic, new ProducerOptions());
            await replyProducer.ProduceAsync(
                new Pong("first"),
                new MessageHeaders { RequestId = requestId },
                ct: default);
            await replyProducer.ProduceAsync(
                new Pong("duplicate"),
                new MessageHeaders { RequestId = requestId },
                ct: default);

            var response = await requestTask;
            Assert.Equal("first", response.Echo);
        }
    }

    [Fact]
    public async Task Dispose_Completes_Pending_With_ObjectDisposedException()
    {
        var transport = new InMemoryTransport();
        var options = new TalariaOptions
        {
            ApplicationName = "rr-dispose",
            DefaultRequestTimeout = TimeSpan.FromSeconds(30),
        };

        var factory = new RequestClientFactory(transport, options, NullLoggerFactory.Instance);
        var client = factory.CreateClient<Ping>("ping.dispose.topic");

        var requestTask = client.GetResponseAsync<Pong>(new Ping("dispose"));
        await factory.DisposeAsync();

        var ex = await Assert.ThrowsAsync<ObjectDisposedException>(() => requestTask);
        Assert.Equal(nameof(RequestClientFactory), ex.ObjectName);
    }

    private sealed record Ping(string Value);
    private sealed record Pong(string Echo);

    private sealed class FailOnceConsumerTransport : Talaria.Core.Abstractions.ITransport
    {
        private readonly InMemoryTransport _inner;
        private int _inboxFailures;

        public FailOnceConsumerTransport(InMemoryTransport inner) => _inner = inner;

        public int InboxCreateAttempts; // approximate; used only for the restart assertion

        public string Name => _inner.Name;

        public Task<Talaria.Core.Abstractions.IConsumer<T>> CreateConsumerAsync<T>(
            string topic, Talaria.Core.Abstractions.ConsumerOptions options, CancellationToken ct = default)
        {
            if (topic.Contains("-replies-", StringComparison.Ordinal))
            {
                Interlocked.Increment(ref InboxCreateAttempts);
                if (Interlocked.Increment(ref _inboxFailures) == 1)
                {
                    throw new InvalidOperationException("simulated transient inbox consumer failure");
                }
            }

            return _inner.CreateConsumerAsync<T>(topic, options, ct);
        }

        public Task<Talaria.Core.Abstractions.IProducer<T>> CreateProducerAsync<T>(
            string topic, Talaria.Core.Abstractions.ProducerOptions options, CancellationToken ct = default)
            => _inner.CreateProducerAsync<T>(topic, options, ct);

        public Task<Talaria.Core.Abstractions.ITransactionalSession> BeginTransactionAsync(
            string? consumerGroup = null, Talaria.Core.Abstractions.TransactionOffsetSource? offsetSource = null, CancellationToken ct = default)
            => _inner.BeginTransactionAsync(consumerGroup, offsetSource, ct);
    }

    private sealed class RecordingTransport : Talaria.Core.Abstractions.ITransport
    {
        private readonly InMemoryTransport _inner;
        private readonly Dictionary<string, TaskCompletionSource<MessageHeaders>> _requests = new();
        private readonly object _gate = new();

        public RecordingTransport(InMemoryTransport inner)
        {
            _inner = inner;
        }

        public string Name => _inner.Name;

        public Task<IConsumer<T>> CreateConsumerAsync<T>(string topic, ConsumerOptions options, CancellationToken ct = default)
            => _inner.CreateConsumerAsync<T>(topic, options, ct);

        public Task<IProducer<T>> CreateProducerAsync<T>(string topic, ProducerOptions options, CancellationToken ct = default)
        {
            var innerProducerTask = _inner.CreateProducerAsync<T>(topic, options, ct);
            return WrapProducerAsync(innerProducerTask, topic);
        }

        public Task<ITransactionalSession> BeginTransactionAsync(
            string? consumerGroup = null, TransactionOffsetSource? offsetSource = null, CancellationToken ct = default)
            => _inner.BeginTransactionAsync(consumerGroup, offsetSource, ct);

        public Task<MessageHeaders> WaitForRequestAsync(string topic, TimeSpan timeout)
        {
            TaskCompletionSource<MessageHeaders> tcs;
            lock (_gate)
            {
                if (_requests.TryGetValue(topic, out var existing))
                {
                    tcs = existing;
                }
                else
                {
                    tcs = new TaskCompletionSource<MessageHeaders>(TaskCreationOptions.RunContinuationsAsynchronously);
                    _requests[topic] = tcs;
                }
            }

            return tcs.Task.WaitAsync(timeout);
        }

        private async Task<IProducer<T>> WrapProducerAsync<T>(Task<IProducer<T>> innerTask, string topic)
        {
            var inner = await innerTask;
            return new RecordingProducer<T>(this, inner, topic);
        }

        private void RecordRequest(string topic, MessageHeaders headers)
        {
            TaskCompletionSource<MessageHeaders>? tcs;
            lock (_gate)
            {
                if (!_requests.TryGetValue(topic, out tcs))
                {
                    tcs = new TaskCompletionSource<MessageHeaders>(TaskCreationOptions.RunContinuationsAsynchronously);
                    _requests[topic] = tcs;
                }
            }

            tcs.TrySetResult(new MessageHeaders(headers));
        }

        private sealed class RecordingProducer<T> : IProducer<T>
        {
            private readonly RecordingTransport _transport;
            private readonly IProducer<T> _inner;
            private readonly string _topic;

            public RecordingProducer(RecordingTransport transport, IProducer<T> inner, string topic)
            {
                _transport = transport;
                _inner = inner;
                _topic = topic;
            }

            public Task ProduceAsync(T message, MessageHeaders? headers = null, string? partitionKey = null, CancellationToken ct = default)
            {
                if (headers is not null)
                {
                    _transport.RecordRequest(_topic, headers);
                }

                return _inner.ProduceAsync(message, headers, partitionKey, ct);
            }

            public ValueTask DisposeAsync()
                => _inner.DisposeAsync();
        }
    }
}
