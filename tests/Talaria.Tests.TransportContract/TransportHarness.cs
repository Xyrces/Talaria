// SPDX-License-Identifier: AGPL-3.0-or-later

using Talaria.Core.Abstractions;

namespace Talaria.Tests.TransportContract;

/// <summary>
/// Per-test harness that owns a single <see cref="ITransport"/> instance and
/// exposes the helpers the shared behavioral matrix uses to read and assert on
/// envelopes. Transport rows create a fresh harness per scenario so that state
/// (channels, offsets, containers) never leaks between cases.
/// </summary>
/// <remarks>
/// <para>
/// Helper methods are the union of <c>Must</c> + <c>ReadOneAsync</c> from
/// <c>Talaria.InMemory.Tests.InMemoryTransportContractTests</c> and
/// <c>TryNextAsync</c> + <c>CollectAsync</c> from
/// <c>Talaria.Transports.Kafka.Tests.KafkaReliabilityIntegrationTests</c>.
/// Centralising them keeps the matrix assertions readable and prevents the
/// drift that produced the original duplication.
/// </para>
/// </remarks>
/// <since>1.0.0</since>
public sealed class TransportHarness : IAsyncDisposable
{
    /// <summary>
    /// The transport instance the matrix drives. Rows construct this; tests
    /// use it to create producers, consumers, and (where supported) sessions.
    /// </summary>
    public ITransport Transport { get; }

    public TransportHarness(ITransport transport)
    {
        Transport = transport ?? throw new ArgumentNullException(nameof(transport));
    }

    /// <summary>
    /// Yields the first envelope within <paramref name="timeout"/>, or
    /// returns <c>null</c> if none arrives. Cancellation-suppressing on
    /// timeout, matching the Kafka helper behavior.
    /// </summary>
    public static async Task<MessageEnvelope<T>?> TryNextAsync<T>(IConsumer<T> consumer, TimeSpan timeout)
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
    /// Collects up to <paramref name="expectedCount"/> envelopes within
    /// <paramref name="timeout"/>, then returns them. Useful for asserting
    /// batched visibility (e.g. transactional commit + offset commit).
    /// </summary>
    public static async Task<List<MessageEnvelope<T>>> CollectAsync<T>(IConsumer<T> consumer, int expectedCount, TimeSpan timeout)
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
            // Timeout elapsed — expected when asserting absence or draining.
        }
        return received;
    }

    /// <summary>
    /// Drains every envelope from <paramref name="consumer"/> within a single
    /// enumeration of <see cref="IConsumer{T}.ConsumeAsync"/> until the timeout
    /// expires. Holding one enumeration prevents transports such as Kafka from
    /// re-subscribing (and rewinding to the earliest offset) between messages.
    /// </summary>
    public static async Task<List<MessageEnvelope<T>>> DrainAsync<T>(IConsumer<T> consumer, TimeSpan timeout)
    {
        var received = new List<MessageEnvelope<T>>();
        using var cts = new CancellationTokenSource(timeout);
        try
        {
            await foreach (var env in consumer.ConsumeAsync(cts.Token))
            {
                received.Add(env);
            }
        }
        catch (OperationCanceledException)
        {
            // Timeout elapsed — expected when the topic is drained.
        }
        return received;
    }

    /// <summary>
    /// Yields a single envelope or throws when the consumer completes without
    /// yielding. Mirrors the InMemory contract helper used by every
    /// redelivery/backlog assertion.
    /// </summary>
    public static async Task<MessageEnvelope<T>> ReadOneAsync<T>(IConsumer<T> consumer, CancellationToken ct = default)
    {
        await foreach (var envelope in consumer.ConsumeAsync(ct))
        {
            return envelope;
        }
        throw new InvalidOperationException("Consumer completed without yielding.");
    }

    /// <summary>
    /// Synchronously awaits <paramref name="read"/> for up to 5 seconds and
    /// throws on timeout. Used by tests that read from a producer-side wait
    /// rather than an async enumeration.
    /// </summary>
    public static MessageEnvelope<T> Must<T>(Task<MessageEnvelope<T>> read, string because)
    {
        Assert.True(read.Wait(TimeSpan.FromSeconds(5)), $"Timed out: {because}");
        return read.Result;
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}

/// <summary>
/// A single row of the transport contract matrix. Each row knows how to
/// stand up its transport (and any shared broker), how to read every
/// message currently on a topic, and how to clean up on test completion.
/// Future transports (Azure Service Bus, etc.) plug in by adding one
/// implementation of this type — no copy-paste of <c>[Fact]</c> methods.
/// </summary>
/// <since>1.0.0</since>
public abstract class TransportContractRow : IAsyncLifetime
{
    /// <summary>
    /// Human-readable label for the row — shows up as a parameterized test
    /// name suffix (e.g. <c>TwoConsumerGroups_EachReceiveEveryMessage(InMemory)</c>).
    /// </summary>
    public abstract string DisplayName { get; }

    /// <summary>
    /// Skips the entire row's scenarios when this returns <c>false</c> —
    /// rows that require Docker or a remote emulator hook in here.
    /// </summary>
    public virtual bool IsAvailable => true;

    /// <summary>
    /// True when the transport routes nacked messages to a transport-wide
    /// application dead-letter queue (<c>__app.dlq</c>) in addition to the
    /// per-topic DLQ. Rows that do not implement an app-wide DLQ override
    /// this to <c>false</c> so the matrix skips that assertion.
    /// </summary>
    public virtual bool SupportsApplicationDeadLetterQueue => false;

    /// <summary>
    /// Constructs a fresh <see cref="TransportHarness"/> for one scenario.
    /// Implementations may share heavyweight resources (Kafka containers)
    /// across harnesses via the harness's <c>RowOwnedResource</c>; the
    /// harness itself is per-test.
    /// </summary>
    public abstract Task<TransportHarness> CreateAsync(CancellationToken ct = default);

    /// <summary>
    /// Reads every envelope currently on <paramref name="topic"/> up to
    /// <paramref name="timeout"/>. Used by the matrix's DLQ-routing and
    /// transactional-visibility assertions.
    /// </summary>
    public abstract Task<List<MessageEnvelope<T>>> ReadAllFromTopicAsync<T>(TransportHarness harness, string topic, TimeSpan timeout);

    public virtual Task InitializeAsync() => Task.CompletedTask;

    public virtual Task DisposeAsync() => Task.CompletedTask;

    public override string ToString() => DisplayName;
}
