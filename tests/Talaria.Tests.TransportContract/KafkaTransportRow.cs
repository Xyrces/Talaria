// SPDX-License-Identifier: AGPL-3.0-or-later

using Microsoft.Extensions.Logging.Abstractions;
using Talaria.Core.Abstractions;
using Talaria.Transports.Kafka;

namespace Talaria.Tests.TransportContract;

/// <summary>
/// Contract row for the Apache Kafka transport. Availability gates on the
/// same Docker check the existing Kafka test suite uses
/// (<see cref="DockerFactAttribute.IsDockerRunning"/>); when Docker is
/// absent, the row's scenarios skip with the same message as
/// <see cref="DockerFactAttribute"/>.
/// <para>
/// Container lifecycle is held by the lazy-singleton
/// <see cref="KafkaContainerFixture"/> — see
/// <see cref="TransportContractMatrix"/>'s
/// <c>KafkaRowOrSkipAsync</c> — so the broker is spun up once on the
/// first <c>Kafka_*</c> test that runs and reused across every
/// subsequent scenario. The transport instance is per-test (per
/// <see cref="CreateAsync"/>) so consumer groups, transactions, and
/// offsets never leak between cases.
/// </para>
/// </summary>
/// <since>1.0.0</since>
public sealed class KafkaTransportRow : TransportContractRow
{
    /// <summary>
    /// The lazy-singleton fixture that owns the <c>KafkaContainer</c>.
    /// Set by <c>TransportContractMatrix.KafkaRowOrSkipAsync</c> before
    /// the first Kafka scenario runs; null for hosts where Docker is
    /// absent.
    /// </summary>
    public KafkaContainerFixture? Fixture { get; set; }

    public override string DisplayName => "Kafka";

    public override bool IsAvailable =>
        DockerFactAttribute.IsDockerRunning() && Fixture is { IsAvailable: true };

    public override Task<TransportHarness> CreateAsync(CancellationToken ct = default)
    {
        if (Fixture is null || !Fixture.IsAvailable)
        {
            throw new InvalidOperationException("Kafka container is not started (Docker unavailable?).");
        }

        var options = new KafkaTransportOptions
        {
            BootstrapServers = Fixture.BootstrapAddress,
        };
        options.BaseConsumerConfig.AutoOffsetReset = Confluent.Kafka.AutoOffsetReset.Earliest;
        // Read committed so aborted transactional produces are invisible to consumers.
        options.BaseConsumerConfig.IsolationLevel = Confluent.Kafka.IsolationLevel.ReadCommitted;

        var transport = new KafkaTransport(
            options,
            NullLoggerFactory.Instance,
            includeDlqExceptionDetails: false);

        return Task.FromResult(new TransportHarness(transport));
    }

    public override async Task<List<MessageEnvelope<T>>> ReadAllFromTopicAsync<T>(TransportHarness harness, string topic, TimeSpan timeout)
    {
        var list = new List<MessageEnvelope<T>>();
        // Use a fresh consumer-group per call so the read is independent of any
        // consumer instance the scenario under test may already have.
        await using var consumer = await harness.Transport.CreateConsumerAsync<T>(
            topic,
            new ConsumerOptions { ConsumerGroup = $"test-reader-{Guid.NewGuid():N}" });

        // Drain within the budget. A short per-message timeout means an empty
        // topic returns quickly; the loop exits as soon as a poll yields null.
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            var remaining = deadline - DateTime.UtcNow;
            if (remaining <= TimeSpan.Zero) break;
            var envelope = await TransportHarness.TryNextAsync(consumer, remaining);
            if (envelope is null) break;
            list.Add(envelope);
        }
        return list;
    }
}
