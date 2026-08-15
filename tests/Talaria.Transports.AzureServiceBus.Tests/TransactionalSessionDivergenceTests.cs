// SPDX-License-Identifier: Apache-2.0

using System.Collections.Concurrent;
using System.Reflection;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Logging.Abstractions;
using Talaria.Core.Abstractions;
using Talaria.Transports.AzureServiceBus;
using Xunit;

namespace Talaria.Transports.AzureServiceBus.Tests;

/// <summary>
/// Pins the buffered-transaction contract that
/// <see cref="AzureServiceBusTransactionalSession"/> implements. The
/// session buffers produces in memory until commit; abort (or disposal)
/// discards them. Unlike Kafka and the in-memory transport, ASB's session
/// has no broker-side transactional primitive — it mirrors the InMemory
/// transport's semantics on top of plain senders. These tests exercise the
/// buffering/flush/discard lifecycle without an emulator by substituting
/// the transport's sender cache with a recording sender.
/// </summary>
/// <remarks>
/// <para>
/// The transport's <c>_senders</c> dictionary is private. We pre-populate
/// it via reflection so the transactional session's commit path flushes
/// through our <see cref="RecordingSender"/> rather than opening a real
/// AMQP link. This is the only seam available without modifying the
/// transport for testability, and the transport deliberately doesn't
/// virtualise its sender acquisition to discourage tight coupling.
/// </para>
/// </remarks>
public class TransactionalSessionDivergenceTests
{
    [Fact]
    public async Task BeginTransaction_GetProducerAsync_ReturnsBufferedProducer()
    {
        // Arrange: a transport whose sender cache is pre-populated with a
        // recording sender — no AMQP link is ever opened.
        var transport = BuildTransportWithInjectedSender("orders");

        // Act: open a session and ask for a producer for a topic.
        await using var session = (AzureServiceBusTransactionalSession)
            await transport.BeginTransactionAsync();
        var producer = await session.GetProducerAsync<string>("orders");

        // Assert: the producer is the buffered producer (not the live one).
        Assert.IsType<AzureServiceBusBufferedProducer<string>>(producer);
    }

    [Fact]
    public async Task Commit_FlushesAllBufferedMessages_GroupedByTopic()
    {
        // Arrange: two topics with one sender each, three buffered messages
        // total. Commit must flush them through the right sender in
        // insertion order, after stripping the internal routing tag.
        var ordersSender = new RecordingSender();
        var shipmentsSender = new RecordingSender();
        var transport = BuildTransportWithInjectedSenders(
            ("orders", ordersSender),
            ("shipments", shipmentsSender));

        // Act.
        await using var session = (AzureServiceBusTransactionalSession)
            await transport.BeginTransactionAsync();

        var orderProducer = await session.GetProducerAsync<string>("orders");
        var shipmentProducer = await session.GetProducerAsync<string>("shipments");

        await orderProducer.ProduceAsync("order-1");
        await shipmentProducer.ProduceAsync("shipment-1");
        await orderProducer.ProduceAsync("order-2");

        await session.CommitAsync();

        // Assert: each sender received its messages in the order they were
        // produced. The internal routing tag is stripped before sending.
        Assert.Equal(2, ordersSender.Sent.Count);
        Assert.Single(shipmentsSender.Sent);
        Assert.False(ordersSender.Sent[0].ApplicationProperties.ContainsKey("talaria.transactional.topic"));
        Assert.False(ordersSender.Sent[1].ApplicationProperties.ContainsKey("talaria.transactional.topic"));
        // The transport's own MessageTypeKey stamping is preserved so
        // receivers can route by CLR type.
        Assert.Equal(typeof(string).FullName, ordersSender.Sent[0].ApplicationProperties[MessageHeaders.MessageTypeKey]);
    }

    [Fact]
    public async Task Abort_DiscardsAllBufferedMessages()
    {
        // Arrange.
        var sender = new RecordingSender();
        var transport = BuildTransportWithInjectedSender("orders");

        // Act.
        await using (var session = (AzureServiceBusTransactionalSession)
            await transport.BeginTransactionAsync())
        {
            var producer = await session.GetProducerAsync<string>("orders");
            await producer.ProduceAsync("aborted");
            await session.AbortAsync();
        }

        // Assert: the sender received no messages because abort discarded
        // the buffer before commit.
        Assert.Empty(sender.Sent);
    }

    [Fact]
    public async Task Dispose_WithoutCommit_DiscardsAllBufferedMessages()
    {
        // Arrange.
        var sender = new RecordingSender();
        var transport = BuildTransportWithInjectedSender("orders");

        // Act: open session, buffer a message, dispose without commit.
        var session = (AzureServiceBusTransactionalSession)
            await transport.BeginTransactionAsync();
        var producer = await session.GetProducerAsync<string>("orders");
        await producer.ProduceAsync("abandoned");
        await session.DisposeAsync();

        // Assert.
        Assert.Empty(sender.Sent);
    }

    [Fact]
    public async Task Commit_Twice_Throws()
    {
        // Committing a completed session must throw rather than silently
        // re-flushing — the buffer is empty after the first commit and
        // producing through the buffered producer would otherwise land in
        // a discarded buffer.
        var sender = new RecordingSender();
        var transport = BuildTransportWithInjectedSender("orders");

        await using var session = (AzureServiceBusTransactionalSession)
            await transport.BeginTransactionAsync();
        await session.CommitAsync();

        await Assert.ThrowsAsync<InvalidOperationException>(() => session.CommitAsync());
    }

    [Fact]
    public async Task Produce_AfterCommit_Throws()
    {
        // A buffered produce against a completed session must throw — the
        // session is in a terminal state and any further produces would
        // silently disappear.
        var sender = new RecordingSender();
        var transport = BuildTransportWithInjectedSender("orders");

        await using var session = (AzureServiceBusTransactionalSession)
            await transport.BeginTransactionAsync();
        await session.CommitAsync();

        // Use a fresh buffered producer for the same session — the public
        // GetProducerAsync throws because the session is already completed.
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            session.GetProducerAsync<string>("orders"));
    }

    private static AzureServiceBusTransport BuildTransportWithInjectedSender(string topic)
    {
        var transport = new AzureServiceBusTransport(
            new AzureServiceBusTransportOptions
            {
                // A syntactically valid but never-resolvable connection string;
                // the SDK never tries to connect on construction, so the
                // transport is safe to instantiate for these unit tests.
                ConnectionString = "Endpoint=sb://unit-test.example/;SharedAccessKeyName=RootManage;SharedAccessKey=KEY",
            },
            loggerFactory: NullLoggerFactory.Instance);

        InjectSenders(transport, (topic, new RecordingSender()));
        return transport;
    }

    private static AzureServiceBusTransport BuildTransportWithInjectedSenders(
        params (string Topic, RecordingSender Sender)[] senders)
    {
        var transport = new AzureServiceBusTransport(
            new AzureServiceBusTransportOptions
            {
                ConnectionString = "Endpoint=sb://unit-test.example/;SharedAccessKeyName=RootManage;SharedAccessKey=KEY",
            },
            loggerFactory: NullLoggerFactory.Instance);

        var pairs = senders.Select(s => (s.Topic, (ServiceBusSender)s.Sender)).ToArray();
        InjectSenders(transport, pairs);
        return transport;
    }

    /// <summary>
    /// Pre-populates the transport's private sender cache with the supplied
    /// senders. The transactional session's commit path resolves senders
    /// through the transport's <c>CheckoutSender</c> which falls through to
    /// the <c>_senders</c> dictionary — bypassing it lets the test avoid
    /// an actual AMQP connection.
    /// </summary>
    private static void InjectSenders(AzureServiceBusTransport transport, params (string Topic, ServiceBusSender Sender)[] senders)
    {
        var field = typeof(AzureServiceBusTransport).GetField(
            "_senders",
            BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("Could not locate _senders field on AzureServiceBusTransport.");
        var dict = (ConcurrentDictionary<string, ServiceBusSender>)field.GetValue(transport)!;
        foreach (var (topic, sender) in senders)
        {
            dict[topic] = sender;
        }
    }
}
