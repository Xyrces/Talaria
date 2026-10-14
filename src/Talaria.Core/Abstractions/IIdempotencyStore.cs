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

/// <summary>The durable processing state of a delivery.</summary>
public enum IdempotencyStatus { Acquired, Busy, Completed }

/// <summary>Only Completed permits acknowledging a delivery without processing it.</summary>
public sealed record IdempotencyAcquisition(IdempotencyStatus Status, IdempotencyLock? Lock = null);

/// <summary>The processing lease expired or was acquired by another worker.</summary>
public sealed class IdempotencyLeaseLostException(string messageId)
    : InvalidOperationException($"Processing ownership was lost for message '{messageId}'.");

/// <summary>
/// Suppresses duplicate message processing across consumer restarts and replicas by tracking
/// message IDs per consumer queue.
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
    /// <summary>Atomically acquires a delivery or distinguishes busy work from completed work.</summary>
    Task<IdempotencyAcquisition> AcquireAsync(string messageId, string consumerQueue, TimeSpan expiration, CancellationToken ct = default);

    /// <summary>Renews only an unexpired processing lease owned by the supplied token.</summary>
    Task<bool> RenewAsync(IdempotencyLock @lock, TimeSpan expiration, CancellationToken ct = default);

    /// <summary>
    /// Attempts to mark the message ID as processed exclusively.
    /// If it returns a lock, no other replica has claimed or processed this message;
    /// the caller owns the lock until it releases it, completes it, or it expires.
    /// </summary>
    /// <param name="messageId">The physical message ID to dedupe on.</param>
    /// <param name="consumerQueue">The consumer group (or topic+group) scoping the lock.</param>
    /// <param name="expiration">How long the lock survives without a release or complete call. Must be greater than zero.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>
    /// The acquired <see cref="IdempotencyLock"/>, or null when another worker already owns
    /// (or has completed) this <paramref name="messageId"/>. Use AcquireAsync to distinguish
    /// these outcomes before acknowledging a delivery.
    /// </returns>
    Task<IdempotencyLock?> TryAcquireLockAsync(string messageId, string consumerQueue, TimeSpan expiration, CancellationToken ct = default);

    /// <summary>
    /// Marks the message as successfully processed, replacing the transient lock with a long-lived completion marker.
    /// Subsequent checks against this message ID will bounce for the completion TTL.
    /// </summary>
    /// <param name="lock">The lock returned by a prior successful <see cref="TryAcquireLockAsync"/>.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <remarks>
    /// Implementations must compare the unexpired ownership token atomically and throw
    /// IdempotencyLeaseLostException on a mismatch. Call only after successful processing.
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
