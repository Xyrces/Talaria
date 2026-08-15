// SPDX-License-Identifier: Apache-2.0

namespace Talaria.Core.Abstractions;

/// <summary>
/// An outbound saga message staged in the transactional outbox alongside a state
/// transition. The <see cref="MessageHeaders"/> already carry the minted MessageId
/// and trace context, so a relay republication (including duplicates after lease
/// expiry) is deduplicated downstream by the idempotency store.
/// </summary>
/// <param name="Id">Unique identifier of this outbox entry.</param>
/// <param name="Topic">The topic the message must be published to.</param>
/// <param name="MessageType">Assembly-qualified CLR type name of the payload, used to resolve the deserializer and producer.</param>
/// <param name="PayloadJson">The message payload serialized as JSON.</param>
/// <param name="Headers">Headers to publish with the message (minted message id, trace context).</param>
/// <param name="CreatedAt">When the entry was staged.</param>
/// <param name="PartitionKey">Optional partition key used to preserve routing affinity when the message is republished.</param>
/// <since>1.0.0</since>
public sealed record OutboxMessage(
    Guid Id,
    string Topic,
    string MessageType,
    string PayloadJson,
    MessageHeaders Headers,
    DateTimeOffset CreatedAt,
    string? PartitionKey = null);

/// <summary>
/// A lease on an outbox entry. Carries a fencing token so that only the current
/// lease holder can complete or abandon the entry. A stale relay (e.g. one whose
/// lease expired) cannot remove an entry another relay has since acquired.
/// </summary>
/// <param name="Id">Identifier of the leased <see cref="OutboxMessage"/>.</param>
/// <param name="Token">Monotonic fencing token incremented on every acquisition.</param>
/// <since>1.0.0</since>
public sealed record OutboxLease(Guid Id, long Token);

/// <summary>
/// An outbox message together with the lease that grants exclusive publishing rights
/// until the lease expires.
/// </summary>
/// <since>1.0.0</since>
public sealed record LeasedOutboxMessage(OutboxMessage Message, OutboxLease Lease);

/// <summary>
/// Read side of the transactional outbox. Saga state transitions stage their outbound
/// messages here atomically with the state write (see
/// <see cref="IStateStore{TState}.TransitionAsync"/>); a background relay leases
/// pending entries and publishes them to the transport.
/// <para>
/// Lease semantics match <see cref="IDeferralStore"/>: acquiring hides entries for the
/// lease duration instead of removing them, so a relay crash never loses a staged
/// message; the lease expires and another relay re-acquires it. Because every entry
/// carries a stable minted MessageId, the duplicate publish that lease expiry can
/// produce is deduplicated by the downstream idempotency gate: at-least-once
/// publishing with idempotent duplicate suppression.
/// </para>
/// </summary>
/// <since>1.0.0</since>
public interface IOutboxStore
{
    /// <summary>
    /// Atomically leases up to <paramref name="maxBatch"/> pending entries, hiding them
    /// from other acquirers until <paramref name="now"/> + <paramref name="leaseDuration"/>.
    /// Entries whose lease expires without completion become acquirable again.
    /// </summary>
    /// <param name="now">The cutoff timestamp. Entries created at or before now are eligible.</param>
    /// <param name="leaseDuration">How long the lease is granted for. Must be greater than zero.</param>
    /// <param name="maxBatch">Upper bound on how many entries are leased in this call.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>The leased entries. Empty when nothing is pending.</returns>
    Task<IReadOnlyList<LeasedOutboxMessage>> AcquirePendingAsync(
        DateTimeOffset now,
        TimeSpan leaseDuration,
        int maxBatch,
        CancellationToken ct = default);

    /// <summary>
    /// Removes a leased entry after successful publication. Only succeeds when the
    /// caller still holds the lease (fencing token match); returns false otherwise.
    /// </summary>
    /// <param name="lease">The lease returned by <see cref="AcquirePendingAsync"/>.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>True when the entry was removed; false when the lease was no longer valid.</returns>
    Task<bool> CompleteAsync(OutboxLease lease, CancellationToken ct = default);

    /// <summary>
    /// Releases a lease without removing the entry, making it visible again at
    /// <paramref name="visibleAt"/> (immediately when null). Only succeeds when the
    /// caller still holds the lease; returns false otherwise.
    /// </summary>
    /// <param name="lease">The lease returned by <see cref="AcquirePendingAsync"/>.</param>
    /// <param name="visibleAt">When the entry becomes visible again. Null means immediately.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>True when the lease was released; false when the lease was no longer valid.</returns>
    Task<bool> AbandonAsync(OutboxLease lease, DateTimeOffset? visibleAt = null, CancellationToken ct = default);
}
