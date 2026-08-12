// SPDX-License-Identifier: AGPL-3.0-or-later

using Talaria.Core.Abstractions;

namespace Talaria.Tests.TransportContract;

/// <summary>
/// The behavioral contract every Talaria transport must satisfy. Each
/// scenario is implemented as a pair of <c>[SkippableFact]</c> methods —
/// one <c>InMemory_*</c> that always runs, one <c>Kafka_*</c> that skips
/// when Docker is unavailable. Adding a new transport means adding another
/// pair, not duplicating the scenario body.
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
/// The matrix class is decorated with <c>[Collection(KafkaRowCollection.Name)]</c>
/// so <see cref="KafkaContainerFixture"/> is instantiated once per collection
/// and injected via the constructor. xUnit calls the fixture's
/// <c>InitializeAsync</c> only when at least one test in the collection is
/// about to run — so the Kafka container is not started on hosts where
/// every <c>Kafka_*</c> test will skip. When Docker is unavailable,
/// <see cref="KafkaContainerFixture.IsAvailable"/> is <c>false</c> and the
/// Kafka <c>[SkippableFact]</c> bodies call <c>Skip.IfNot</c> with the same
/// reason text as <see cref="DockerFactAttribute"/>.
/// </para>
/// <para>
/// Why per-<c>[SkippableFact]</c> rather than a <c>[SkippableTheory]</c>
/// over <c>[MemberData]</c>: xUnit evaluates <c>[MemberData]</c> arguments
/// at test discovery time, which would force the Kafka container to start
/// during discovery and hang on hosts that need to pull
/// <c>confluentinc/cp-kafka:7.4.0</c>. <c>[SkippableFact]</c> defers
/// evaluation to execution time, matching the pattern the existing
/// <c>KafkaReliabilityIntegrationTests</c> uses via
/// <c>IAsyncLifetime.InitializeAsync</c>.
/// </para>
/// </remarks>
/// <since>1.0.0</since>
[Collection(KafkaRowCollection.Name)]
public class TransportContractMatrix
{
    private readonly KafkaContainerFixture _kafkaFixture;

    /// <summary>
    /// Shared message payload used by every scenario. Public so the
    /// per-scenario <c>[SkippableFact]</c> methods can name it in
    /// assertions.
    /// </summary>
    public sealed class Msg
    {
        public string Id { get; set; } = "";
    }

    public TransportContractMatrix(KafkaContainerFixture kafkaFixture)
    {
        _kafkaFixture = kafkaFixture;
    }

    // ---- per-transport scenario entry points ------------------------------------

    [SkippableFact]
    public async Task InMemory_TwoConsumerGroups_EachReceiveEveryMessage()
        => await RunTwoConsumerGroupsAsync(new InMemoryTransportRow());

    [SkippableFact]
    public async Task InMemory_LateJoiningGroup_ReplaysRetainedBacklog()
        => await RunLateJoiningGroupReplaysRetainedBacklogAsync(new InMemoryTransportRow());

    [SkippableFact]
    public async Task InMemory_TransactionalCommit_IsVisible_AbortDiscards()
        => await RunTransactionalCommitIsVisibleAbortDiscardsAsync(new InMemoryTransportRow());

    [SkippableFact]
    public async Task InMemory_Nack_RoutesToTopicAndAppDlq()
        => await RunNackRoutesToTopicAndAppDlqAsync(new InMemoryTransportRow());

    [SkippableFact]
    public async Task InMemory_Uncommitted_Message_Is_Redelivered_After_Consumer_Restart()
        => await RunUncommittedMessageIsRedeliveredAfterConsumerRestartAsync(new InMemoryTransportRow());

    [SkippableFact]
    public async Task InMemory_Committed_Message_Is_Not_Redelivered_After_Consumer_Restart()
        => await RunCommittedMessageIsNotRedeliveredAfterConsumerRestartAsync(new InMemoryTransportRow());

    [SkippableFact]
    public async Task Kafka_TwoConsumerGroups_EachReceiveEveryMessage()
        => await RunTwoConsumerGroupsAsync(KafkaRowOrSkip());

    [SkippableFact]
    public async Task Kafka_LateJoiningGroup_ReplaysRetainedBacklog()
        => await RunLateJoiningGroupReplaysRetainedBacklogAsync(KafkaRowOrSkip());

    [SkippableFact]
    public async Task Kafka_TransactionalCommit_IsVisible_AbortDiscards()
        => await RunTransactionalCommitIsVisibleAbortDiscardsAsync(KafkaRowOrSkip());

    [SkippableFact]
    public async Task Kafka_Nack_RoutesToTopicAndAppDlq()
        => await RunNackRoutesToTopicAndAppDlqAsync(KafkaRowOrSkip());

    [SkippableFact]
    public async Task Kafka_Uncommitted_Message_Is_Redelivered_After_Consumer_Restart()
        => await RunUncommittedMessageIsRedeliveredAfterConsumerRestartAsync(KafkaRowOrSkip());

    [SkippableFact]
    public async Task Kafka_Committed_Message_Is_Not_Redelivered_After_Consumer_Restart()
        => await RunCommittedMessageIsNotRedeliveredAfterConsumerRestartAsync(KafkaRowOrSkip());

    // ---- shared scenario bodies --------------------------------------------------

    private async Task<TransportHarness> CreateHarnessAsync(TransportContractRow row)
    {
        Skip.IfNot(row.IsAvailable, $"Transport row '{row.DisplayName}' is not available on this host.");
        return await row.CreateAsync();
    }

    /// <summary>
    /// Resolves a Kafka row against the class-injected fixture, or skips
    /// the test with the standard Docker-not-running message.
    /// </summary>
    private KafkaTransportRow KafkaRowOrSkip()
    {
        Skip.IfNot(
            DockerFactAttribute.IsDockerRunning() && _kafkaFixture.IsAvailable,
            "Docker daemon is not running on this host environment.");
        return new KafkaTransportRow { Fixture = _kafkaFixture };
    }

    private async Task RunTwoConsumerGroupsAsync(TransportContractRow row)
    {
        await using var harness = await CreateHarnessAsync(row);
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

    private async Task RunLateJoiningGroupReplaysRetainedBacklogAsync(TransportContractRow row)
    {
        await using var harness = await CreateHarnessAsync(row);
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

    private async Task RunTransactionalCommitIsVisibleAbortDiscardsAsync(TransportContractRow row)
    {
        await using var harness = await CreateHarnessAsync(row);

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

    private async Task RunNackRoutesToTopicAndAppDlqAsync(TransportContractRow row)
    {
        await using var harness = await CreateHarnessAsync(row);
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

    private async Task RunUncommittedMessageIsRedeliveredAfterConsumerRestartAsync(TransportContractRow row)
    {
        await using var harness = await CreateHarnessAsync(row);
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
        } // Dispose without commit → unsettled message is requeued.

        await using var consumer2 = await harness.Transport.CreateConsumerAsync<Msg>(
            topic, new ConsumerOptions { ConsumerGroup = group });
        var redelivered = TransportHarness.Must(
            TransportHarness.ReadOneAsync(consumer2, cts.Token),
            "unsettled message is redelivered");

        Assert.Equal(first.Offset, redelivered.Offset);
        Assert.Equal("work", redelivered.Payload.Id);

        await consumer2.CommitAsync(redelivered);
    }

    private async Task RunCommittedMessageIsNotRedeliveredAfterConsumerRestartAsync(TransportContractRow row)
    {
        await using var harness = await CreateHarnessAsync(row);
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
