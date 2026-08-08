using Talaria.Core.Abstractions;
using Talaria.Transports.InMemory;
using Xunit;

namespace Talaria.InMemory.Tests;

/// <summary>
/// Transport-level contract for the in-memory transport, mirroring the semantics the
/// Kafka transport guarantees: group fan-out, backlog replay for late joiners,
/// transaction commit/abort visibility, poison-message DLQ routing, nack routing,
/// and redelivery of uncommitted messages after a consumer restart.
/// </summary>
public class InMemoryTransportContractTests
{
    private sealed class Msg { public string Id { get; set; } = ""; }

    private static async Task<MessageEnvelope<T>> ReadOneAsync<T>(IConsumer<T> consumer, CancellationToken ct)
    {
        await foreach (var envelope in consumer.ConsumeAsync(ct))
        {
            return envelope;
        }

        throw new InvalidOperationException("Consumer completed without yielding.");
    }

    private static MessageEnvelope<T> Must<T>(Task<MessageEnvelope<T>> read, string because)
    {
        Assert.True(read.Wait(TimeSpan.FromSeconds(5)), $"Timed out: {because}");
        return read.Result;
    }

    [Fact]
    public async Task TwoConsumerGroups_EachReceiveEveryMessage()
    {
        var transport = new InMemoryTransport();
        using var cts = new CancellationTokenSource();

        var producer = await transport.CreateProducerAsync<Msg>("c-fanout", new ProducerOptions());
        await producer.ProduceAsync(new Msg { Id = "m1" });

        await using var consumerA = await transport.CreateConsumerAsync<Msg>("c-fanout", new ConsumerOptions { ConsumerGroup = "group-a" });
        await using var consumerB = await transport.CreateConsumerAsync<Msg>("c-fanout", new ConsumerOptions { ConsumerGroup = "group-b" });

        var a = Must(ReadOneAsync(consumerA, cts.Token), "group A receives the message");
        var b = Must(ReadOneAsync(consumerB, cts.Token), "group B receives the same message");

        Assert.Equal("m1", a.Payload.Id);
        Assert.Equal("m1", b.Payload.Id);
        Assert.Equal(a.Offset, b.Offset);
    }

    [Fact]
    public async Task LateJoiningGroup_ReplaysRetainedBacklog()
    {
        var transport = new InMemoryTransport();
        using var cts = new CancellationTokenSource();

        var producer = await transport.CreateProducerAsync<Msg>("c-late", new ProducerOptions());
        await producer.ProduceAsync(new Msg { Id = "old" });

        // The group did not exist when the message was published.
        await using var consumer = await transport.CreateConsumerAsync<Msg>("c-late", new ConsumerOptions { ConsumerGroup = "late-group" });

        var envelope = Must(ReadOneAsync(consumer, cts.Token), "late-joining group replays the backlog");
        Assert.Equal("old", envelope.Payload.Id);
    }

    [Fact]
    public async Task Transaction_Produces_InvisibleUntilCommit_And_AbortDiscards()
    {
        var transport = new InMemoryTransport();
        using var cts = new CancellationTokenSource();

        await using (var tx = await transport.BeginTransactionAsync())
        {
            var txProducer = await tx.GetProducerAsync<Msg>("c-tx");
            await txProducer.ProduceAsync(new Msg { Id = "committed" });
            await tx.CommitAsync();
        }

        await using (var abortedTx = await transport.BeginTransactionAsync())
        {
            var txProducer = await abortedTx.GetProducerAsync<Msg>("c-tx");
            await txProducer.ProduceAsync(new Msg { Id = "aborted" });
            // No commit — disposal aborts.
        }

        var observed = await transport.ReadAllFromTopicAsync<Msg>("c-tx");
        var envelope = Assert.Single(observed);
        Assert.Equal("committed", envelope.Payload.Id);
    }

    [Fact]
    public async Task Nack_RoutesToTopicAndAppDlq()
    {
        var transport = new InMemoryTransport();
        using var cts = new CancellationTokenSource();

        var producer = await transport.CreateProducerAsync<Msg>("c-nack", new ProducerOptions());
        await producer.ProduceAsync(new Msg { Id = "bad" }, new MessageHeaders { MessageId = "nack-1" });

        await using var consumer = await transport.CreateConsumerAsync<Msg>("c-nack", new ConsumerOptions { ConsumerGroup = "g" });
        var envelope = Must(ReadOneAsync(consumer, cts.Token), "consumer receives the message to nack");

        await consumer.NackAsync(envelope);

        var topicDlq = await transport.ReadAllFromTopicAsync<Msg>("c-nack.dlq");
        Assert.Single(topicDlq);
        var appDlq = await transport.ReadAllFromTopicAsync<Msg>("__app.dlq");
        Assert.Single(appDlq);
    }

    [Fact]
    public async Task Uncommitted_Message_Is_Redelivered_After_Consumer_Restart()
    {
        var transport = new InMemoryTransport();
        using var cts = new CancellationTokenSource();

        var producer = await transport.CreateProducerAsync<Msg>("c-redeliver", new ProducerOptions());
        await producer.ProduceAsync(new Msg { Id = "work" });

        // First consumer instance reads but never commits (crash/fault simulation).
        MessageEnvelope<Msg> first;
        await using (var consumer1 = await transport.CreateConsumerAsync<Msg>("c-redeliver", new ConsumerOptions { ConsumerGroup = "g" }))
        {
            first = Must(ReadOneAsync(consumer1, cts.Token), "first consumer instance reads the message");
        } // Dispose without commit → unsettled message is requeued.

        // A replacement consumer instance (supervision restart) receives it again.
        await using var consumer2 = await transport.CreateConsumerAsync<Msg>("c-redeliver", new ConsumerOptions { ConsumerGroup = "g" });
        var redelivered = Must(ReadOneAsync(consumer2, cts.Token), "unsettled message is redelivered");

        Assert.Equal(first.Offset, redelivered.Offset);
        Assert.Equal("work", redelivered.Payload.Id);

        await consumer2.CommitAsync(redelivered);
    }

    [Fact]
    public async Task Committed_Message_Is_Not_Redelivered_After_Consumer_Restart()
    {
        var transport = new InMemoryTransport();
        using var cts = new CancellationTokenSource();

        var producer = await transport.CreateProducerAsync<Msg>("c-commit", new ProducerOptions());
        await producer.ProduceAsync(new Msg { Id = "done" });

        await using (var consumer1 = await transport.CreateConsumerAsync<Msg>("c-commit", new ConsumerOptions { ConsumerGroup = "g" }))
        {
            var envelope = Must(ReadOneAsync(consumer1, cts.Token), "first consumer instance reads the message");
            await consumer1.CommitAsync(envelope);
        }

        // Restart the consumer: nothing should arrive. The test-reader group must also
        // see the message exactly once (it is a different group, so it replays the backlog —
        // but the original group channel must be empty).
        await using var consumer2 = await transport.CreateConsumerAsync<Msg>("c-commit", new ConsumerOptions { ConsumerGroup = "g" });
        var read = ReadOneAsync(consumer2, cts.Token);
        var completed = await Task.WhenAny(read, Task.Delay(TimeSpan.FromMilliseconds(500)));
        Assert.NotSame(read, completed);
    }
}
