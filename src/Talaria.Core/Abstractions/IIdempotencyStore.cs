namespace Talaria.Core.Abstractions;

/// <summary>
/// An acquired idempotency lock. Carries a fencing token so that only the
/// current owner of the lock can release or complete it — a stale holder
/// (e.g. a worker whose lock expired) cannot remove a newer owner's lock.
/// </summary>
public sealed record IdempotencyLock(string MessageId, string ConsumerQueue, string Token);

/// <summary>
/// A centralized abstraction for enforcing exactly-once delivery across a distributed cluster.
/// Tracks physical message IDs globally to prevent redundant concurrent processing and duplicate replays.
/// </summary>
public interface IIdempotencyStore
{
    /// <summary>
    /// Attempts to mark the message ID as processed exclusively.
    /// If it returns a lock, no other replica has claimed or processed this message;
    /// the caller owns the lock until it releases it, completes it, or it expires.
    /// In an event-driven setup natively backing Exactly-Once Delivery, this replaces complex local locking.
    /// </summary>
    Task<IdempotencyLock?> TryAcquireLockAsync(string messageId, string consumerQueue, TimeSpan expiration, CancellationToken ct = default);

    /// <summary>
    /// Formally seals the message's successful execution, converting the transient lock into a long-lasting flag.
    /// Subsequent checks against this message ID will bounce for the completion TTL.
    /// </summary>
    Task MarkCompleteAsync(IdempotencyLock @lock, CancellationToken ct = default);

    /// <summary>
    /// Removes the lock ensuring it can be legitimately re-tried (e.g. if the consumer failed internally).
    /// Only succeeds when the caller still owns the lock (fencing token match).
    /// </summary>
    Task ReleaseLockAsync(IdempotencyLock @lock, CancellationToken ct = default);
}
