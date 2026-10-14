// SPDX-License-Identifier: Apache-2.0

using Talaria.Core.Abstractions;

namespace Talaria.Transports.InMemory;

/// <summary>
/// In-memory deferral store with lease (visibility-timeout) semantics matching
/// <see cref="IDeferralStore"/>: acquiring hides entries for the lease duration instead
/// of removing them, and completion/abandonment are fenced by the lease token.
/// Suitable for lightweight single-process deployments, prototyping, and tests —
/// entries do not survive a process restart.
/// </summary>
public sealed class InMemoryDeferralStore : IDeferralStore
{
    private sealed record Entry(DeferredMessage Message, long LeaseToken, DateTimeOffset VisibleAt);

    private readonly object _gate = new();
    private readonly List<Entry> _entries = [];
    private long _nextLeaseToken;

    public Task EnqueueAsync(DeferredMessage message, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(message);
        ct.ThrowIfCancellationRequested();
        lock (_gate)
        {
            var existing = _entries.FirstOrDefault(e => e.Message.Id == message.Id);
            if (existing is not null)
            {
                if (!SameDeferredDelivery(existing.Message, message))
                    throw new InvalidOperationException($"Deferred message id '{message.Id}' is already stored with different delivery data.");
                return Task.CompletedTask;
            }
            _entries.Add(new Entry(Clone(message), LeaseToken: 0, message.DueAt));
        }

        return Task.CompletedTask;
    }

    public Task<IReadOnlyList<LeasedDeferral>> AcquireDueAsync(
        DateTimeOffset now,
        TimeSpan leaseDuration,
        int maxBatch,
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        if (leaseDuration <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(leaseDuration));
        if (maxBatch <= 0) throw new ArgumentOutOfRangeException(nameof(maxBatch));
        lock (_gate)
        {
            var due = _entries
                .Where(e => e.VisibleAt <= now)
                .OrderBy(e => e.VisibleAt)
                .Take(maxBatch)
                .ToList();

            var leased = new List<LeasedDeferral>(due.Count);
            foreach (var entry in due)
            {
                var index = _entries.IndexOf(entry);
                var token = checked(++_nextLeaseToken);
                _entries[index] = entry with { LeaseToken = token, VisibleAt = now.Add(leaseDuration) };
                leased.Add(new LeasedDeferral(
                    Clone(entry.Message),
                    new DeferralLease(entry.Message.Id, token)) { IsReacquired = entry.LeaseToken != 0 });
            }

            return Task.FromResult<IReadOnlyList<LeasedDeferral>>(leased);
        }
    }

    public Task<bool> CompleteAsync(DeferralLease lease, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        lock (_gate)
        {
            var removed = _entries.RemoveAll(e => e.Message.Id == lease.Id && e.LeaseToken == lease.Token);
            return Task.FromResult(removed > 0);
        }
    }

    public Task<bool> AbandonAsync(DeferralLease lease, DateTimeOffset? visibleAt = null, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        lock (_gate)
        {
            var index = _entries.FindIndex(e => e.Message.Id == lease.Id && e.LeaseToken == lease.Token);
            if (index < 0)
            {
                return Task.FromResult(false);
            }

            _entries[index] = _entries[index] with { VisibleAt = visibleAt ?? DateTimeOffset.UtcNow };
            return Task.FromResult(true);
        }
    }

    private static DeferredMessage Clone(DeferredMessage message)
        => message with { Headers = new MessageHeaders(message.Headers) };

    private static bool SameDeferredDelivery(DeferredMessage left, DeferredMessage right)
        => left.Topic == right.Topic
            && left.MessageType == right.MessageType
            && left.PayloadJson == right.PayloadJson
            && left.CorrelationId == right.CorrelationId
            && left.Attempt == right.Attempt
            && left.PartitionKey == right.PartitionKey
            && left.Headers.SequenceEqual(right.Headers);

    /// <summary>Returns the number of currently scheduled messages (test/diagnostic use).</summary>
    public int Count
    {
        get
        {
            lock (_gate)
            {
                return _entries.Count;
            }
        }
    }
}
