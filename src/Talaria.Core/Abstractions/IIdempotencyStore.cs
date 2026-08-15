// SPDX-License-Identifier: Apache-2.0

namespace Talaria.Core.Abstractions;

/// <summary>
/// An acquired idempotency lock. Carries a fencing token so that only the
/// current owner of the lock can release or complete it — a stale holder
/// (e.g. a worker whose lock expired) cannot remove a newer owner's lock.
/// </summary>
/// <param name="MessageId">The physical message ID the lock protects.</param>
/// <param name="ConsumerQueue">The consumer group (or topic+group) the lock is scoped to.</param>
/// <param name="Token">The fencing token issued at acquisition. Must be presented back to <see cref="IIdempotencyStore.ReleaseLockAsync"/> / <see cref="IIdempotencyStore.MarkCompleteAsync"/>.</param>
/// <since>1.0.0</since>
public sealed record IdempotencyLock(string MessageId, string ConsumerQueue, string Token);

/// <summary>
/// A centralized abstraction for enforcing exactly-once delivery across a distributed cluster.
/// Tracks physical message IDs globally to prevent redundant concurrent processing and duplicate replays.
/// </summary>
/// <remarks>
/// TalariaListener acquires an <see cref="IdempotencyLock"/> before processing a
/// message; only the holder may release or complete the lock. Lease/fencing semantics
/// (<see cref="TalariaOptions.IdempotencyLockTtl"/>) bound how long a crashed worker can
/// hold a lock — after expiry, a new worker may re-acquire the message ID.
/// </remarks>
/// <since>1.0.0</since>
public interface IIdempotencyStore
{
    /// <summary>
    /// Attempts to mark the message ID as processed exclusively.
    /// If it returns a lock, no other replica has claimed or processed this message;
    /// the caller owns the lock until it releases it, completes it, or it expires.
    /// In an event-driven setup natively backing Exactly-Once Delivery, this replaces complex local locking.
    /// </summary>
    /// <param name="messageId">The physical message ID to dedupe on.</param>
    /// <param name="consumerQueue">The consumer group (or topic+group) scoping the lock.</param>
    /// <param name="expiration">How long the lock survives without a release or complete call. Must be greater than zero.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>
    /// The acquired <see cref="IdempotencyLock"/>, or null when another worker already owns
    /// (or has completed) this <paramref name="messageId"/>. A null return tells the caller
    /// to skip processing — the message is a duplicate.
    /// </returns>
    Task<IdempotencyLock?> TryAcquireLockAsync(string messageId, string consumerQueue, TimeSpan expiration, CancellationToken ct = default);

    /// <summary>
    /// Formally seals the message's successful execution, converting the transient lock into a long-lasting flag.
    /// Subsequent checks against this message ID will bounce for the completion TTL.
    /// </summary>
    /// <param name="lock">The lock returned by a prior successful <see cref="TryAcquireLockAsync"/>.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <remarks>
    /// Failure to complete leaves the lock in transient state — it expires via TTL and the
    /// message becomes reprocessable. Always call from a finally block after successful
    /// handler execution.
    /// </remarks>
    Task MarkCompleteAsync(IdempotencyLock @lock, CancellationToken ct = default);

    /// <summary>
    /// Removes the lock ensuring it can be legitimately re-tried (e.g. if the consumer failed internally).
    /// Only succeeds when the caller still owns the lock (fencing token match).
    /// </summary>
    /// <param name="lock">The lock to release.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <remarks>
    /// A stale holder whose lock expired will get a no-op (the fencing token no longer matches),
    /// so it cannot accidentally remove a newer worker's lock.
    /// </remarks>
    Task ReleaseLockAsync(IdempotencyLock @lock, CancellationToken ct = default);
}
