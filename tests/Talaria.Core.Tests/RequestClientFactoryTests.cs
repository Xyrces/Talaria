// SPDX-License-Identifier: Apache-2.0

using Microsoft.Extensions.Logging.Abstractions;
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
}
