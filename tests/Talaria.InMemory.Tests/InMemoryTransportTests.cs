using Talaria.Core.Abstractions;
using Talaria.Transports.InMemory;

namespace Talaria.InMemory.Tests;

public class InMemoryTransportTests
{
    [Fact]
    public async Task ProduceAndConsume_RoundTrips_Message()
    {
        // Arrange
        var transport = new InMemoryTransport();
        var producer = await transport.CreateProducerAsync<TestOrder>(
            "orders", new ProducerOptions());
        var consumer = await transport.CreateConsumerAsync<TestOrder>(
            "orders", new ConsumerOptions());

        // Act
        await producer.ProduceAsync(new TestOrder("ORD-1", 42m));

        // Assert
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        await foreach (var envelope in consumer.ConsumeAsync(cts.Token))
        {
            Assert.Equal("ORD-1", envelope.Payload.OrderId);
            Assert.Equal(42m, envelope.Payload.Total);
            await consumer.CommitAsync(envelope, cts.Token);
            break; // Only expect one message
        }
    }

    [Fact]
    public async Task NackAsync_Routes_To_TopicDlq()
    {
        // Arrange
        var transport = new InMemoryTransport();
        var producer = await transport.CreateProducerAsync<TestOrder>(
            "orders", new ProducerOptions());
        var consumer = await transport.CreateConsumerAsync<TestOrder>(
            "orders", new ConsumerOptions());

        await producer.ProduceAsync(new TestOrder("ORD-FAIL", 10m));

        // Act
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        await foreach (var envelope in consumer.ConsumeAsync(cts.Token))
        {
            await consumer.NackAsync(envelope, cts.Token);
            break;
        }

        // Assert — message should appear in topic-specific DLQ
        var dlqMessages = await transport.ReadAllFromTopicAsync<TestOrder>("orders.dlq");
        Assert.Single(dlqMessages);
        Assert.Equal("ORD-FAIL", dlqMessages[0].Payload.OrderId);
    }

    [Fact]
    public async Task NackAsync_Routes_To_AppDlq()
    {
        // Arrange
        var transport = new InMemoryTransport();
        var producer = await transport.CreateProducerAsync<TestOrder>(
            "orders", new ProducerOptions());
        var consumer = await transport.CreateConsumerAsync<TestOrder>(
            "orders", new ConsumerOptions());

        await producer.ProduceAsync(new TestOrder("ORD-FAIL", 10m));

        // Act
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        await foreach (var envelope in consumer.ConsumeAsync(cts.Token))
        {
            await consumer.NackAsync(envelope, cts.Token);
            break;
        }

        // Assert — message should appear in app-wide DLQ
        var dlqMessages = await transport.ReadAllFromTopicAsync<TestOrder>("__app.dlq");
        Assert.Single(dlqMessages);
    }

    [Fact]
    public async Task Headers_Are_Preserved_Through_RoundTrip()
    {
        var transport = new InMemoryTransport();
        var producer = await transport.CreateProducerAsync<TestOrder>(
            "orders", new ProducerOptions());
        var consumer = await transport.CreateConsumerAsync<TestOrder>(
            "orders", new ConsumerOptions());

        var headers = new MessageHeaders
        {
            TraceParent = "00-abc123-def456-01",
            HopCount = 5,
        };

        await producer.ProduceAsync(new TestOrder("ORD-H", 1m), headers);

        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        await foreach (var envelope in consumer.ConsumeAsync(cts.Token))
        {
            Assert.Equal("00-abc123-def456-01", envelope.Headers.TraceParent);
            // HopCount is engine-owned: producing with an existing count means "forward", so it increments.
            Assert.Equal(6, envelope.Headers.HopCount);
            break;
        }
    }

    [Fact]
    public async Task DlqBus_IsUnbounded_DoesNotDropDeadLetters()
    {
        // Arrange: a tiny regular-topic capacity so a bounded DLQ would definitely overflow.
        var options = new InMemoryTransportOptions { ChannelCapacity = 2 };
        var transport = new InMemoryTransport(options);
        var producer = await transport.CreateProducerAsync<TestOrder>(
            "orders", new ProducerOptions());
        var consumer = await transport.CreateConsumerAsync<TestOrder>(
            "orders", new ConsumerOptions());

        const int messageCount = 10;

        // Produce concurrently: the small topic channel would block if we published all 10 upfront.
        var producerTask = Task.Run(async () =>
        {
            for (var i = 0; i < messageCount; i++)
            {
                await producer.ProduceAsync(new TestOrder($"ORD-{i}", i));
            }
        });

        // Act: nack every message. The DLQ must accept all of them without blocking or dropping.
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var nacked = 0;
        await foreach (var envelope in consumer.ConsumeAsync(cts.Token))
        {
            await consumer.NackAsync(envelope, cts.Token);
            nacked++;
            if (nacked == messageCount)
            {
                break;
            }
        }

        await producerTask;
        Assert.Equal(messageCount, nacked);

        // Assert: both topic-specific and app-wide DLQs retained every dead letter.
        var topicDlqMessages = await transport.ReadAllFromTopicAsync<TestOrder>("orders.dlq");
        var appDlqMessages = await transport.ReadAllFromTopicAsync<TestOrder>("__app.dlq");

        Assert.Equal(messageCount, topicDlqMessages.Count);
        Assert.Equal(messageCount, appDlqMessages.Count);
    }
}

public record TestOrder(string OrderId, decimal Total);
