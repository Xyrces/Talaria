// SPDX-License-Identifier: AGPL-3.0-or-later

namespace Talaria.Core.Abstractions;

/// <summary>
/// Result of a transport-native idempotency check performed by an
/// <see cref="IIdempotencyVerifier"/> implementation before a message reaches
/// the consumer pipeline.
/// </summary>
/// <remarks>
/// Distinguishes transport-native duplicate detection (cheap, transport-local,
/// no external store) from the cross-cluster fencing-token deduplication
/// performed by <see cref="IIdempotencyStore"/>. The two are complementary:
/// a transport that surfaces <see cref="Duplicate"/> lets the host skip
/// deserialization and handler invocation entirely.
/// </remarks>
/// <since>1.0.0</since>
public enum IdempotencyVerdict
{
    /// <summary>
    /// The verifier could not determine whether this message is a duplicate
    /// (e.g. its MessageId was empty). The host falls back to the external
    /// <see cref="IIdempotencyStore"/> gate.
    /// </summary>
    Unverifiable = 0,

    /// <summary>
    /// The verifier has no record of this MessageId on this consumer group.
    /// The host must proceed to handler invocation and the external
    /// <see cref="IIdempotencyStore"/> gate.
    /// </summary>
    New = 1,

    /// <summary>
    /// The verifier knows this MessageId has already been processed on this
    /// consumer group. The host must commit the offset without invoking the
    /// handler — exactly-once delivery is preserved end-to-end.
    /// </summary>
    Duplicate = 2,
}

/// <summary>
/// Transport-native duplicate detection. Lets a transport answer "have I seen
/// this MessageId on this consumer group before?" using its own bookkeeping
/// (Azure Service Bus native duplicate detection, Kafka transactional
/// offset metadata, etc.) — without round-tripping to an external store.
/// </summary>
/// <remarks>
/// The pipeline layer in <c>Talaria.Core.Hosting</c> is expected to call
/// <see cref="VerifyAsync"/> on every inbound message before invoking the
/// handler. A <see cref="IdempotencyVerdict.Duplicate"/> result short-circuits
/// the handler dispatch and commits the message offset, mirroring the
/// behavior already produced when the external <see cref="IIdempotencyStore"/>
/// reports a held lock. Implementations that cannot inspect their transport's
/// native duplicate-detection metadata should return <see cref="IdempotencyVerdict.Unverifiable"/>
/// so the host falls back to the external store. The verifier is independent
/// of <see cref="IIdempotencyStore"/>: the two deduplicate on the same
/// MessageId, but the verifier is cheap and transport-local while the store
/// is durable and cluster-wide.
/// </remarks>
/// <since>1.0.0</since>
public interface IIdempotencyVerifier
{
    /// <summary>
    /// Asks the transport whether this MessageId has already been delivered on
    /// this consumer group. Implementations should be cheap (no I/O when
    /// possible) — the verifier sits on the consumer hot path.
    /// </summary>
    /// <typeparam name="T">The CLR payload type of the inbound envelope.</typeparam>
    /// <param name="envelope">The inbound message envelope to inspect.</param>
    /// <param name="consumerGroup">The consumer group the verifier scopes its bookkeeping to.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>
    /// <see cref="IdempotencyVerdict.Duplicate"/> when the transport knows
    /// this MessageId was processed; <see cref="IdempotencyVerdict.New"/> when
    /// it does not; <see cref="IdempotencyVerdict.Unverifiable"/> when the
    /// verifier cannot decide (empty MessageId, transport feature unavailable).
    /// </returns>
    Task<IdempotencyVerdict> VerifyAsync<T>(
        MessageEnvelope<T> envelope,
        string consumerGroup,
        CancellationToken ct = default);
}

/// <summary>
/// A no-op <see cref="IIdempotencyVerifier"/> used when no transport-native
/// duplicate detection is registered. Always returns
/// <see cref="IdempotencyVerdict.Unverifiable"/> so the pipeline falls back to
/// the external <see cref="IIdempotencyStore"/> for deduplication.
/// </summary>
/// <since>1.0.0</since>
public sealed class NullIdempotencyVerifier : IIdempotencyVerifier
{
    /// <summary>The shared singleton instance.</summary>
    public static readonly NullIdempotencyVerifier Instance = new();

    private NullIdempotencyVerifier() { }

    /// <inheritdoc />
    public Task<IdempotencyVerdict> VerifyAsync<T>(
        MessageEnvelope<T> envelope,
        string consumerGroup,
        CancellationToken ct = default)
        => Task.FromResult(IdempotencyVerdict.Unverifiable);
}
