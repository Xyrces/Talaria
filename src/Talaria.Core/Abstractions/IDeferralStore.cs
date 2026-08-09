// SPDX-License-Identifier: AGPL-3.0-or-later

namespace Talaria.Core.Abstractions;

/// <summary>
/// A saga message that is waiting for its scheduled (re)delivery time.
/// </summary>
/// <param name="Id">Unique identifier of this deferred entry.</param>
/// <param name="Topic">The topic the message must be republished to.</param>
/// <param name="MessageType">Assembly-qualified CLR type name of the payload, used to resolve the deserializer and producer.</param>
/// <param name="PayloadJson">The message payload serialized as JSON.</param>
/// <param name="Headers">Headers to republish with the message (deferral attempt, minted message id, trace context).</param>
/// <param name="CorrelationId">The saga correlation id, if one was resolved.</param>
/// <param name="Attempt">The deferral attempt number (1-based).</param>
/// <param name="DueAt">When the message becomes eligible for republishing.</param>
public sealed record DeferredMessage(
    Guid Id,
    string Topic,
    string MessageType,
    string PayloadJson,
    MessageHeaders Headers,
    string? CorrelationId,
    int Attempt,
    DateTimeOffset DueAt);

/// <summary>
/// A lease on a deferred message. Carries a fencing token so that only the current
/// lease holder can complete or abandon the entry — a stale holder (e.g. a sweeper
/// whose lease expired) cannot remove an entry another sweeper has since acquired.
/// </summary>
/// <param name="Id">Identifier of the leased <see cref="DeferredMessage"/>.</param>
/// <param name="Token">Monotonic fencing token incremented on every acquisition.</param>
public sealed record DeferralLease(Guid Id, long Token);

/// <summary>
/// A deferred message together with the lease that grants exclusive republishing rights
/// until the lease expires.
/// </summary>
public sealed record LeasedDeferral(DeferredMessage Message, DeferralLease Lease);

/// <summary>
/// Durable store for deferred saga messages (out-of-order arrivals and handler-initiated
/// deferrals). Entries survive restarts and can be swept by any node of the application.
/// <para>
/// Lease semantics (Azure Service Bus peek-lock analogue): <see cref="AcquireDueAsync"/>
/// hides the returned entries from other acquirers for the requested lease duration by
/// pushing their visibility time into the future — it does NOT remove them. If the
/// acquirer crashes or shuts down without completing, the lease expires and the entry
/// becomes acquirable again, so a deferred message is never lost mid-sweep. Successful
/// republication is confirmed with <see cref="CompleteAsync"/>; a failed one is
/// rescheduled with <see cref="AbandonAsync"/>. Both are fenced by the lease token, so
/// an expired holder cannot interfere with a newer owner. Because lease expiry can
/// produce a duplicate republication, downstream deduplication (the idempotency store)
/// is what makes processing effectively once — the same model as Service Bus.
/// </para>
/// </summary>
public interface IDeferralStore
{
    /// <summary>Schedules a message for delivery at <see cref="DeferredMessage.DueAt"/>.</summary>
    Task EnqueueAsync(DeferredMessage message, CancellationToken ct = default);

    /// <summary>
    /// Atomically leases up to <paramref name="maxBatch"/> messages due at or before
    /// <paramref name="now"/>, hiding them from other acquirers until
    /// <paramref name="now"/> + <paramref name="leaseDuration"/>. Entries whose lease
    /// expires without completion become acquirable again.
    /// </summary>
    Task<IReadOnlyList<LeasedDeferral>> AcquireDueAsync(
        DateTimeOffset now,
        TimeSpan leaseDuration,
        int maxBatch,
        CancellationToken ct = default);

    /// <summary>
    /// Removes a leased message after successful republication. Only succeeds when the
    /// caller still holds the lease (fencing token match); returns false otherwise.
    /// </summary>
    Task<bool> CompleteAsync(DeferralLease lease, CancellationToken ct = default);

    /// <summary>
    /// Releases a lease without removing the entry, making it visible again at
    /// <paramref name="visibleAt"/> (immediately when null). Only succeeds when the
    /// caller still holds the lease; returns false otherwise.
    /// </summary>
    Task<bool> AbandonAsync(DeferralLease lease, DateTimeOffset? visibleAt = null, CancellationToken ct = default);
}
