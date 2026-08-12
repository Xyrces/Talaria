// SPDX-License-Identifier: AGPL-3.0-or-later

using Talaria.Core.Abstractions;

namespace Talaria.Tests.TransportContract;

/// <summary>
/// The behavioral contract every Talaria transport must satisfy. Each
/// scenario is parameterized over the available <see cref="TransportContractRow"/>
/// implementations; adding a new transport means adding one row, not
/// duplicating <c>[Fact]</c> methods here.
/// </summary>
/// <remarks>
/// <para>
/// Six scenarios are pinned — the same six that
/// <c>Talaria.InMemory.Tests.InMemoryTransportContractTests</c> covered:
/// group fan-out, late-joining replay, transactional commit/abort
/// visibility, nack routing to topic + app DLQ, uncommitted-message
/// redelivery after consumer restart, and committed-message
/// non-redelivery after consumer restart.
/// </para>
/// <para>
/// The Kafka row is wired up only when Docker is available (via
/// <see cref="DockerFactAttribute.IsDockerRunning"/>) and the
/// <see cref="KafkaContainerFixture"/> has started its container.
/// </para>
/// </remarks>
/// <since>1.0.0</since>
public class TransportContractMatrix
{
    /// <summary>
    /// Shared message payload used by every scenario. Public so the
    /// parameterised <c>[Theory]</c> methods can name it in assertions.
    /// </summary>
    public sealed class Msg
    {
        public string Id { get; set; } = "";
    }

    public static IEnumerable<object[]> Rows()
    {
        // InMemory is always available.
        yield return new object[] { new InMemoryTransportRow() };

        // Kafka is conditional on Docker availability. The row's IsAvailable
        // hook handles the per-test skip reason when the container fixture
        // couldn't start.
        if (DockerFactAttribute.IsDockerRunning())
        {
            var kafkaRow = new KafkaTransportRow();
            kafkaRow.Fixture = KafkaFixtureBootstrapper.GetOrCreateFixtureAsync().GetAwaiter().GetResult();
            if (kafkaRow.Fixture is { IsAvailable: true })
            {
                yield return new object[] { kafkaRow };
            }
        }
    }

    [SkippableTheory]
    [MemberData(nameof(Rows))]
    public async Task TwoConsumerGroups_EachReceiveEveryMessage(TransportContractRow row)
    {
        Skip.IfNot(row.IsAvailable, $"Transport row '{row.DisplayName}' is not available on this host.");

        await using var harness = await row.CreateAsync();
        using var cts = new CancellationTokenSource();

        var topic = $"c-fanout-{Guid.NewGuid():N}";
        var producer = await harness.Transport.CreateProducerAsync<Msg>(topic, new ProducerOptions());
        await producer.ProduceAsync(new Msg { Id = "m1" });

        await using var consumerA = await harness.Transport.CreateConsumerAsync<Msg>(
            topic, new ConsumerOptions { ConsumerGroup = $"group-a-{Guid.NewGuid():N}" });
        await using var consumerB = await harness.Transport.CreateConsumerAsync<Msg>(
            topic, new ConsumerOptions { ConsumerGroup = $"group-b-{Guid.NewGuid():N}" });

        var a = TransportHarness.Must(
            TransportHarness.ReadOneAsync(consumerA, cts.Token),
            "group A receives the message");
        var b = TransportHarness.Must(
            TransportHarness.ReadOneAsync(consumerB, cts.Token),
            "group B receives the same message");

        Assert.Equal("m1", a.Payload.Id);
        Assert.Equal("m1", b.Payload.Id);
        Assert.Equal(a.Offset, b.Offset);
    }

    [SkippableTheory]
    [MemberData(nameof(Rows))]
    public async Task LateJoiningGroup_ReplaysRetainedBacklog(TransportContractRow row)
    {
        Skip.IfNot(row.IsAvailable, $"Transport row '{row.DisplayName}' is not available on this host.");

        await using var harness = await row.CreateAsync();
        using var cts = new CancellationTokenSource();

        var topic = $"c-late-{Guid.NewGuid():N}";
        var producer = await harness.Transport.CreateProducerAsync<Msg>(topic, new ProducerOptions());
        await producer.ProduceAsync(new Msg { Id = "old" });

        await using var consumer = await harness.Transport.CreateConsumerAsync<Msg>(
            topic, new ConsumerOptions { ConsumerGroup = $"late-group-{Guid.NewGuid():N}" });

        var envelope = TransportHarness.Must(
            TransportHarness.ReadOneAsync(consumer, cts.Token),
            "late-joining group replays the backlog");
        Assert.Equal("old", envelope.Payload.Id);
    }

    [SkippableTheory]
    [MemberData(nameof(Rows))]
    public async Task TransactionalCommit_IsVisible_AbortDiscards(TransportContractRow row)
    {
        Skip.IfNot(row.IsAvailable, $"Transport row '{row.DisplayName}' is not available on this host.");

        await using var harness = await row.CreateAsync();

        var topic = $"c-tx-{Guid.NewGuid():N}";

        await using (var tx = await harness.Transport.BeginTransactionAsync())
        {
            var txProducer = await tx.GetProducerAsync<Msg>(topic);
            await txProducer.ProduceAsync(new Msg { Id = "committed" });
            await tx.CommitAsync();
        }

        await using (var abortedTx = await harness.Transport.BeginTransactionAsync())
        {
            var txProducer = await abortedTx.GetProducerAsync<Msg>(topic);
            await txProducer.ProduceAsync(new Msg { Id = "aborted" });
            // No commit — disposal aborts.
        }

        var observed = await row.ReadAllFromTopicAsync<Msg>(harness, topic, TimeSpan.FromSeconds(5));
        var envelope = Assert.Single(observed);
        Assert.Equal("committed", envelope.Payload.Id);
    }

    [SkippableTheory]
    [MemberData(nameof(Rows))]
    public async Task Nack_RoutesToTopicAndAppDlq(TransportContractRow row)
    {
        Skip.IfNot(row.IsAvailable, $"Transport row '{row.DisplayName}' is not available on this host.");

        await using var harness = await row.CreateAsync();
        using var cts = new CancellationTokenSource();

        var topic = $"c-nack-{Guid.NewGuid():N}";
        var producer = await harness.Transport.CreateProducerAsync<Msg>(topic, new ProducerOptions());
        await producer.ProduceAsync(new Msg { Id = "bad" }, new MessageHeaders { MessageId = "nack-1" });

        await using var consumer = await harness.Transport.CreateConsumerAsync<Msg>(
            topic, new ConsumerOptions { ConsumerGroup = $"g-{Guid.NewGuid():N}" });
        var envelope = TransportHarness.Must(
            TransportHarness.ReadOneAsync(consumer, cts.Token),
            "consumer receives the message to nack");
        await consumer.NackAsync(envelope);

        var topicDlq = await row.ReadAllFromTopicAsync<Msg>(harness, $"{topic}.dlq", TimeSpan.FromSeconds(5));
        Assert.Single(topicDlq);
        var appDlq = await row.ReadAllFromTopicAsync<Msg>(harness, "__app.dlq", TimeSpan.FromSeconds(5));
        Assert.Single(appDlq);
    }

    [SkippableTheory]
    [MemberData(nameof(Rows))]
    public async Task Uncommitted_Message_Is_Redelivered_After_Consumer_Restart(TransportContractRow row)
    {
        Skip.IfNot(row.IsAvailable, $"Transport row '{row.DisplayName}' is not available on this host.");

        await using var harness = await row.CreateAsync();
        using var cts = new CancellationTokenSource();

        var topic = $"c-redeliver-{Guid.NewGuid():N}";
        var group = $"g-{Guid.NewGuid():N}";

        var producer = await harness.Transport.CreateProducerAsync<Msg>(topic, new ProducerOptions());
        await producer.ProduceAsync(new Msg { Id = "work" });

        MessageEnvelope<Msg> first;
        await using (var consumer1 = await harness.Transport.CreateConsumerAsync<Msg>(
            topic, new ConsumerOptions { ConsumerGroup = group }))
        {
            first = TransportHarness.Must(
                TransportHarness.ReadOneAsync(consumer1, cts.Token),
                "first consumer instance reads the message");
        }

        await using var consumer2 = await harness.Transport.CreateConsumerAsync<Msg>(
            topic, new ConsumerOptions { ConsumerGroup = group });
        var redelivered = TransportHarness.Must(
            TransportHarness.ReadOneAsync(consumer2, cts.Token),
            "unsettled message is redelivered");

        Assert.Equal(first.Offset, redelivered.Offset);
        Assert.Equal("work", redelivered.Payload.Id);

        await consumer2.CommitAsync(redelivered);
    }

    [SkippableTheory]
    [MemberData(nameof(Rows))]
    public async Task Committed_Message_Is_Not_Redelivered_After_Consumer_Restart(TransportContractRow row)
    {
        Skip.IfNot(row.IsAvailable, $"Transport row '{row.DisplayName}' is not available on this host.");

        await using var harness = await row.CreateAsync();
        using var cts = new CancellationTokenSource();

        var topic = $"c-commit-{Guid.NewGuid():N}";
        var group = $"g-{Guid.NewGuid():N}";

        var producer = await harness.Transport.CreateProducerAsync<Msg>(topic, new ProducerOptions());
        await producer.ProduceAsync(new Msg { Id = "done" });

        await using (var consumer1 = await harness.Transport.CreateConsumerAsync<Msg>(
            topic, new ConsumerOptions { ConsumerGroup = group }))
        {
            var envelope = TransportHarness.Must(
                TransportHarness.ReadOneAsync(consumer1, cts.Token),
                "first consumer instance reads the message");
            await consumer1.CommitAsync(envelope);
        }

        await using var consumer2 = await harness.Transport.CreateConsumerAsync<Msg>(
            topic, new ConsumerOptions { ConsumerGroup = group });
        var read = TransportHarness.ReadOneAsync(consumer2, cts.Token);
        var completed = await Task.WhenAny(read, Task.Delay(TimeSpan.FromMilliseconds(500), cts.Token));
        Assert.NotSame(read, completed);
    }
}

/// <summary>
/// Lazy one-shot bootstrapper for the shared Kafka container fixture. Used
/// by <see cref="TransportContractMatrix.Rows"/> so the row's
/// <see cref="KafkaTransportRow.Fixture"/> can be set without a separate
/// xUnit class-fixture wiring step.
/// </summary>
internal static class KafkaFixtureBootstrapper
{
    private static KafkaContainerFixture? _fixture;
    private static readonly object _gate = new();

    public static async Task<KafkaContainerFixture> GetOrCreateFixtureAsync()
    {
        if (_fixture is not null)
        {
            return _fixture;
        }
        lock (_gate)
        {
            if (_fixture is not null)
            {
                return _fixture;
            }
            _fixture = new KafkaContainerFixture();
        }
        await _fixture.InitializeAsync();
        return _fixture;
    }
}
