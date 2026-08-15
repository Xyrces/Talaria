// SPDX-License-Identifier: Apache-2.0

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
/// The matrix class deliberately does NOT carry a <c>[Collection]</c>
/// attribute and <see cref="KafkaContainerFixture"/> is NOT an
/// <c>IAsyncLifetime</c> collection fixture. xUnit's
/// collection-fixture machinery instantiates <c>IClassFixture&lt;T&gt;</c>
/// implementations during test discovery for every class in the
/// collection, which on a CI runner with Docker present forces the
/// Kafka image pull + broker port wait to happen during discovery and
/// hangs the run. Instead, the Kafka container is held by a static
/// <see cref="KafkaContainerFixture.EnsureStartedAsync"/> lazy singleton:
/// the container starts only when a <c>Kafka_*</c> test method actually
/// executes; on Docker-less hosts the lazy initializer returns an
/// <c>IsAvailable=false</c> instance and every Kafka_* test skips via
/// <c>Skip.IfNot</c>.
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
public class TransportContractMatrix
{
    /// <summary>
    /// Shared message payload used by every scenario. Public so the
    /// per-scenario <c>[SkippableFact]</c> methods can name it in
    /// assertions.
    /// </summary>
    public sealed class Msg
    {
        public string Id { get; set; } = "";
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
        => await RunTwoConsumerGroupsAsync(await KafkaRowOrSkipAsync());

    [SkippableFact]
    public async Task Kafka_LateJoiningGroup_ReplaysRetainedBacklog()
        => await RunLateJoiningGroupReplaysRetainedBacklogAsync(await KafkaRowOrSkipAsync());

    [SkippableFact]
    public async Task Kafka_TransactionalCommit_IsVisible_AbortDiscards()
        => await RunTransactionalCommitIsVisibleAbortDiscardsAsync(await KafkaRowOrSkipAsync());

    [SkippableFact]
    public async Task Kafka_Nack_RoutesToTopicAndAppDlq()
        => await RunNackRoutesToTopicAndAppDlqAsync(await KafkaRowOrSkipAsync());

    [SkippableFact]
    public async Task Kafka_Uncommitted_Message_Is_Redelivered_After_Consumer_Restart()
        => await RunUncommittedMessageIsRedeliveredAfterConsumerRestartAsync(await KafkaRowOrSkipAsync());

    [SkippableFact]
    public async Task Kafka_Committed_Message_Is_Not_Redelivered_After_Consumer_Restart()
        => await RunCommittedMessageIsNotRedeliveredAfterConsumerRestartAsync(await KafkaRowOrSkipAsync());

    // ---- shared scenario bodies --------------------------------------------------

    private async Task<TransportHarness> CreateHarnessAsync(TransportContractRow row)
    {
        Skip.IfNot(row.IsAvailable, $"Transport row '{row.DisplayName}' is not available on this host.");
        return await row.CreateAsync();
    }

    /// <summary>
    /// Resolves a Kafka row backed by the lazy-singleton Kafka container
    /// fixture, or skips the test with the standard Docker-not-running
    /// message. The fixture's <see cref="KafkaContainerFixture.IsAvailable"/>
    /// is <c>false</c> when Docker is unavailable OR the container start
    /// timed out, in which case the matrix's per-test <c>Skip.IfNot</c> in
    /// <see cref="CreateHarnessAsync"/> suppresses the Kafka scenarios.
    /// </summary>
    private static async Task<KafkaTransportRow> KafkaRowOrSkipAsync()
    {
        // Defer the fixture acquisition until execution time — that is
        // the whole point of using [SkippableFact] + a static lazy
        // singleton rather than [Collection] + IClassFixture.
        var fixture = await KafkaContainerFixture.EnsureStartedAsync().ConfigureAwait(false);
        Skip.IfNot(
            fixture.IsAvailable,
            "Kafka transport contract is opt-in; set TALARIA_RUN_KAFKA_TRANSPORT_CONTRACT=1 to enable, or ensure Docker is running.");
        return new KafkaTransportRow { Fixture = fixture };
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

        if (row.SupportsApplicationDeadLetterQueue)
        {
            var appDlq = await row.ReadAllFromTopicAsync<Msg>(harness, "__app.dlq", TimeSpan.FromSeconds(5));
            Assert.Single(appDlq);
        }
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
