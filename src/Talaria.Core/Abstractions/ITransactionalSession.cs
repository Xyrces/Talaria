namespace Talaria.Core.Abstractions;

/// <summary>
/// Identifies the consumed message whose offset should be committed inside a transaction.
/// </summary>
/// <param name="Topic">The topic the message was consumed from.</param>
/// <param name="Partition">The partition the message was consumed from.</param>
/// <param name="Offset">The offset of the consumed message (the transaction commits offset + 1).</param>
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
public interface ITransactionalSession : IAsyncDisposable
{
    /// <summary>
    /// Gets a producer whose writes are buffered inside this transaction.
    /// </summary>
    Task<IProducer<T>> GetProducerAsync<T>(string topic, CancellationToken ct = default);

    /// <summary>
    /// Commits the transaction — all produces and the offset commit within this session
    /// become durable atomically.
    /// </summary>
    Task CommitAsync(CancellationToken ct = default);

    /// <summary>
    /// Aborts the transaction — all produces and offset commits within this session
    /// are discarded.
    /// </summary>
    Task AbortAsync(CancellationToken ct = default);
}
