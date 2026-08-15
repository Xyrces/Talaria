using Microsoft.Extensions.DependencyInjection;
using Talaria.Core.Abstractions;
using Talaria.Core.Registration;
using Testcontainers.Kafka;
using Xunit;

namespace Talaria.Transports.Kafka.Tests;

public class KafkaReliabilityIntegrationTests : IAsyncLifetime
{
    private KafkaContainer? _kafkaContainer;
    private IServiceProvider _serviceProvider = null!;

    public async Task InitializeAsync()
    {
        if (!DockerFactAttribute.IsDockerRunning()) return;

        _kafkaContainer = new KafkaBuilder("confluentinc/cp-kafka:7.4.0")
            .Build();

        await _kafkaContainer.StartAsync();

        var services = new ServiceCollection();
        var builder = services.AddTalaria();
        builder.UseKafkaTransport(opts =>
        {
            opts.BootstrapServers = _kafkaContainer!.GetBootstrapAddress();
            opts.BaseConsumerConfig.AutoOffsetReset = Confluent.Kafka.AutoOffsetReset.Earliest;
            // Read committed so aborted transactional produces are invisible to consumers.
            opts.BaseConsumerConfig.IsolationLevel = Confluent.Kafka.IsolationLevel.ReadCommitted;
        });

        _serviceProvider = services.BuildServiceProvider();
    }

    public async Task DisposeAsync()
    {
        if (_kafkaContainer != null)
        {
            await _kafkaContainer.DisposeAsync();
        }
    }

    [DockerFact]
    public async Task PoisonMessage_RoutesToDlq_AndConsumerLoopSurvives()
    {
        var transport = _serviceProvider.GetRequiredService<ITransport>();

        string topic = $"test-poison-{Guid.NewGuid():N}";

        var poisonProducer = await transport.CreateProducerAsync<string>(topic, new ProducerOptions());
        var validProducer = await transport.CreateProducerAsync<int>(topic, new ProducerOptions());
        await using var consumer = await transport.CreateConsumerAsync<int>(
            topic,
            new ConsumerOptions { ConsumerGroup = $"test-group-{Guid.NewGuid():N}" });

        // A string payload serializes as a JSON string ("not-a-number" with quotes),
        // which JsonSerializer.Deserialize<int> cannot convert — a poison message for the int consumer.
        await poisonProducer.ProduceAsync("not-a-number", new MessageHeaders { MessageId = "poison-1" });
        await validProducer.ProduceAsync(42, new MessageHeaders { MessageId = "valid-1" });

        // The poison message is skipped (routed to the DLQ internally); the loop survives
        // and still yields the subsequent valid message.
        var valid = await TryNextAsync(consumer, TimeSpan.FromSeconds(15));
        Assert.NotNull(valid);
        Assert.Equal(42, valid!.Payload);
        Assert.Equal(topic, valid.SourceTopic);

        // The poison message landed on the DLQ topic with the raw payload and a DLQ reason header.
        await using var dlqConsumer = await transport.CreateConsumerAsync<string>(
            topic + ".dlq",
            new ConsumerOptions { ConsumerGroup = $"test-group-{Guid.NewGuid():N}" });

        var deadLetter = await TryNextAsync(dlqConsumer, TimeSpan.FromSeconds(15));
        Assert.NotNull(deadLetter);
        Assert.Equal("not-a-number", deadLetter!.Payload);
        Assert.Equal("DeserializationFailed", deadLetter.Headers.DlqReason);
    }

    [DockerFact]
    public async Task Nack_RoutesToDlq_WithoutRedelivery()
    {
        var transport = _serviceProvider.GetRequiredService<ITransport>();

        string topic = $"test-nack-{Guid.NewGuid():N}";

        var producer = await transport.CreateProducerAsync<string>(topic, new ProducerOptions());
        await using var consumer = await transport.CreateConsumerAsync<string>(
            topic,
            new ConsumerOptions { ConsumerGroup = $"test-group-{Guid.NewGuid():N}" });

        await producer.ProduceAsync("nack-me", new MessageHeaders { MessageId = "nack-1" });

        var received = new List<MessageEnvelope<string>>();
        using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10)))
        {
            try
            {
                await foreach (var env in consumer.ConsumeAsync(cts.Token))
                {
                    received.Add(env);
                    if (received.Count == 1)
                    {
                        // Nack moves it to the DLQ and commits; keep polling for the rest of
                        // the window to catch any redelivery of the same message.
                        await consumer.NackAsync(env);
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // Window elapsed — expected.
            }
        }

        Assert.Single(received);
        Assert.Equal("nack-me", received[0].Payload);

        await using var dlqConsumer = await transport.CreateConsumerAsync<string>(
            topic + ".dlq",
            new ConsumerOptions { ConsumerGroup = $"test-group-{Guid.NewGuid():N}" });

        var deadLetter = await TryNextAsync(dlqConsumer, TimeSpan.FromSeconds(15));
        Assert.NotNull(deadLetter);
        Assert.Equal("nack-me", deadLetter!.Payload);
    }

    [DockerFact]
    public async Task Commit_ThenRestartSameGroup_DoesNotRedeliver()
    {
        var transport = _serviceProvider.GetRequiredService<ITransport>();

        string topic = $"test-commit-{Guid.NewGuid():N}";
        string group = $"test-group-{Guid.NewGuid():N}";

        var producer = await transport.CreateProducerAsync<string>(topic, new ProducerOptions());
        await producer.ProduceAsync("first", new MessageHeaders { MessageId = "m-1" });

        var consumer1 = await transport.CreateConsumerAsync<string>(topic, new ConsumerOptions { ConsumerGroup = group });
        var first = await TryNextAsync(consumer1, TimeSpan.FromSeconds(15));
        Assert.NotNull(first);
        Assert.Equal("first", first!.Payload);
        await consumer1.CommitAsync(first);
        // Commits are queued and drained by the poll thread (~100ms) — give it time before restart.
        await Task.Delay(TimeSpan.FromSeconds(1));
        await consumer1.DisposeAsync();

        // A new consumer in the same group resumes from the committed offset: no redelivery.
        var consumer2 = await transport.CreateConsumerAsync<string>(topic, new ConsumerOptions { ConsumerGroup = group });
        var replay = await TryNextAsync(consumer2, TimeSpan.FromSeconds(8));
        Assert.Null(replay);
        await consumer2.DisposeAsync();

        // ...but newly produced messages are still delivered (on a fresh consumer because
        // each consumer instance may only be enumerated once).
        await producer.ProduceAsync("second", new MessageHeaders { MessageId = "m-2" });

        // Poll until the new consumer joins and receives the message. The group may need
        // a moment to finish rebalancing after the previous member left, so we recreate
        // the consumer each attempt (each instance allows only one enumeration).
        var second = await TryNextAsyncWithRetry(
            () => transport.CreateConsumerAsync<string>(topic, new ConsumerOptions { ConsumerGroup = group }),
            TimeSpan.FromSeconds(15),
            TimeSpan.FromMilliseconds(200));
        Assert.NotNull(second);
        Assert.Equal("second", second!.Payload);
    }

    [DockerFact]
    public async Task TransactionalAbort_ProducesNothing()
    {
        var transport = _serviceProvider.GetRequiredService<ITransport>();

        string topic = $"test-tx-abort-{Guid.NewGuid():N}";

        await using (var session = await transport.BeginTransactionAsync())
        {
            var txProducer = await session.GetProducerAsync<string>(topic);
            await txProducer.ProduceAsync("tx-1");
            await txProducer.ProduceAsync("tx-2");
            await session.AbortAsync();
        }

        await using var consumer = await transport.CreateConsumerAsync<string>(
            topic,
            new ConsumerOptions { ConsumerGroup = $"test-group-{Guid.NewGuid():N}" });

        var received = await TryNextAsync(consumer, TimeSpan.FromSeconds(8));
        Assert.Null(received);
    }

    [DockerFact]
    public async Task TransactionalCommit_MakesProducesVisible_AndCommitsConsumedOffset()
    {
        var transport = _serviceProvider.GetRequiredService<ITransport>();

        string topic = $"test-tx-commit-{Guid.NewGuid():N}";

        // Committed transactional produces become visible to read-committed consumers.
        await using (var session = await transport.BeginTransactionAsync())
        {
            var txProducer = await session.GetProducerAsync<string>(topic);
            await txProducer.ProduceAsync("tx-a");
            await txProducer.ProduceAsync("tx-b");
            await session.CommitAsync();
        }

        await using var checkConsumer = await transport.CreateConsumerAsync<string>(
            topic,
            new ConsumerOptions { ConsumerGroup = $"test-group-{Guid.NewGuid():N}" });

        // Read both messages within ONE enumeration: abandoning an enumeration and
        // starting a new one rejoins the group and replays uncommitted messages.
        var both = await CollectAsync(checkConsumer, expectedCount: 2, TimeSpan.FromSeconds(15));
        Assert.Equal(2, both.Count);
        Assert.Equal("tx-a", both[0].Payload);
        Assert.Equal("tx-b", both[1].Payload);

        // Consume one message with group G, then commit its offset inside a second transaction.
        string group = $"test-group-{Guid.NewGuid():N}";
        var consumerG1 = await transport.CreateConsumerAsync<string>(topic, new ConsumerOptions { ConsumerGroup = group });
        var consumed = await TryNextAsync(consumerG1, TimeSpan.FromSeconds(15));
        Assert.NotNull(consumed);
        Assert.Equal("tx-a", consumed!.Payload);
        Assert.NotNull(consumed.Partition);

        await using (var session2 = await transport.BeginTransactionAsync(
            group,
            new TransactionOffsetSource(topic, consumed.Partition!.Value, consumed.Offset)))
        {
            var txProducer2 = await session2.GetProducerAsync<string>(topic);
            await txProducer2.ProduceAsync("tx-c");
            await session2.CommitAsync();
        }

        await consumerG1.DisposeAsync();
        // Give the group a moment to release the old member before the new one joins.
        await Task.Delay(TimeSpan.FromSeconds(1));

        // A fresh consumer in group G resumes after the transactionally committed offset:
        // the first message is not re-yielded, later messages are.
        await using var consumerG2 = await transport.CreateConsumerAsync<string>(topic, new ConsumerOptions { ConsumerGroup = group });
        var received = await CollectAsync(consumerG2, TimeSpan.FromSeconds(10));

        Assert.DoesNotContain(received, env => env.Payload == "tx-a");
        Assert.Contains(received, env => env.Payload == "tx-b");
    }

    /// <summary>
    /// Returns the first envelope yielded within the timeout, or null if none arrives.
    /// </summary>
    private static async Task<MessageEnvelope<T>?> TryNextAsync<T>(IConsumer<T> consumer, TimeSpan timeout)
    {
        using var cts = new CancellationTokenSource(timeout);
        try
        {
            await foreach (var env in consumer.ConsumeAsync(cts.Token))
            {
                return env;
            }
        }
        catch (OperationCanceledException)
        {
            // Timeout elapsed with no message — expected when asserting absence.
        }
        return null;
    }

    /// <summary>
    /// Polls <see cref="TryNextAsync{T}"/>, recreating the consumer each attempt, until a
    /// message arrives or the overall deadline expires. This tolerates Kafka group
    /// rebalancing delays without a fixed sleep.
    /// </summary>
    private static async Task<MessageEnvelope<T>?> TryNextAsyncWithRetry<T>(
        Func<Task<IConsumer<T>>> consumerFactory,
        TimeSpan overallDeadline,
        TimeSpan attemptTimeout)
    {
        var deadline = DateTime.UtcNow + overallDeadline;
        while (DateTime.UtcNow < deadline)
        {
            var consumer = await consumerFactory();
            try
            {
                var env = await TryNextAsync(consumer, attemptTimeout);
                if (env is not null)
                {
                    return env;
                }
            }
            finally
            {
                await consumer.DisposeAsync();
            }
        }
        return null;
    }

    /// <summary>
    /// Collects every envelope yielded within the window, then returns them.
    /// </summary>
    private static async Task<List<MessageEnvelope<T>>> CollectAsync<T>(IConsumer<T> consumer, TimeSpan window)
    {
        var received = new List<MessageEnvelope<T>>();
        using var cts = new CancellationTokenSource(window);
        try
        {
            await foreach (var env in consumer.ConsumeAsync(cts.Token))
            {
                received.Add(env);
            }
        }
        catch (OperationCanceledException)
        {
            // Window elapsed — expected when draining.
        }
        return received;
    }

    /// <summary>
    /// Collects envelopes within one enumeration until <paramref name="expectedCount"/>
    /// is reached or the timeout elapses.
    /// </summary>
    private static async Task<List<MessageEnvelope<T>>> CollectAsync<T>(IConsumer<T> consumer, int expectedCount, TimeSpan timeout)
    {
        var received = new List<MessageEnvelope<T>>();
        using var cts = new CancellationTokenSource(timeout);
        try
        {
            await foreach (var env in consumer.ConsumeAsync(cts.Token))
            {
                received.Add(env);
                if (received.Count >= expectedCount)
                {
                    return received;
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Timeout elapsed — expected when asserting absence.
        }
        return received;
    }
}
