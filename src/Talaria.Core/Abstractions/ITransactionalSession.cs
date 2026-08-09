// SPDX-License-Identifier: AGPL-3.0-or-later

namespace Talaria.Core.Abstractions;

/// <summary>
/// Identifies the consumed message whose offset should be committed inside a transaction.
/// </summary>
/// <param name="Topic">The topic the message was consumed from.</param>
/// <param name="Partition">The partition the message was consumed from.</param>
/// <param name="Offset">The offset of the consumed message (the transaction commits offset + 1).</param>
/// <since>1.0.0</since>
public sealed record TransactionOffsetSource(string Topic, int Partition, long Offset);

/// <summary>
/// A transactional boundary around outbound produces and (for supporting transports)
/// the commit of the consumed message's offset.
/// <para>
/// Kafka: all produces obtained via <see cref="GetProducerAsync{T}"/> plus the offset
/// described by the session's <see cref="TransactionOffsetSource"/> are committed in a
/// single Kafka transaction; aborting discards them atomically.
/// InMemory: produces are buffered and become visible to consumers only on commit.
/// </para>
/// <para>
/// Note: saga state stores (Redis/InMemory) do NOT participate in this transaction —
/// a crash between the state save and the transaction commit replays the message
/// against transitioned state, so saga step handlers must be idempotent.
/// Disposing an open session aborts it.
/// </para>
/// </summary>
/// <since>1.0.0</since>
public interface ITransactionalSession : IAsyncDisposable
{
    /// <summary>
    /// Gets a producer whose writes are buffered inside this transaction.
    /// </summary>
    /// <typeparam name="T">The CLR message type the producer serializes.</typeparam>
    /// <param name="topic">The topic the producer writes to.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>A producer whose writes are committed only when <see cref="CommitAsync"/> succeeds.</returns>
    Task<IProducer<T>> GetProducerAsync<T>(string topic, CancellationToken ct = default);

    /// <summary>
    /// Commits the transaction — all produces and the offset commit within this session
    /// become durable atomically.
    /// </summary>
    /// <param name="ct">Cancellation token.</param>
    /// <remarks>
    /// After commit, every producer returned by <see cref="GetProducerAsync{T}"/> is
    /// considered consumed by the transaction; further produces on those producers must
    /// either obtain a new session or be discarded.
    /// </remarks>
    Task CommitAsync(CancellationToken ct = default);

    /// <summary>
    /// Aborts the transaction — all produces and offset commits within this session
    /// are discarded.
    /// </summary>
    /// <param name="ct">Cancellation token.</param>
    /// <remarks>
    /// Equivalent to disposing the session without a prior commit. Use when a saga step
    /// handler throws and the engine decides not to retry the transaction.
    /// </remarks>
    Task AbortAsync(CancellationToken ct = default);
}
