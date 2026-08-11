// SPDX-License-Identifier: AGPL-3.0-or-later

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

    public Task EnqueueAsync(DeferredMessage message, CancellationToken ct = default)
    {
        lock (_gate)
        {
            _entries.Add(new Entry(message, LeaseToken: 0, message.DueAt));
        }

        return Task.CompletedTask;
    }

    public Task<IReadOnlyList<LeasedDeferral>> AcquireDueAsync(
        DateTimeOffset now,
        TimeSpan leaseDuration,
        int maxBatch,
        CancellationToken ct = default)
    {
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
                var token = entry.LeaseToken + 1;
                _entries[index] = entry with { LeaseToken = token, VisibleAt = now.Add(leaseDuration) };
                leased.Add(new LeasedDeferral(
                    entry.Message,
                    new DeferralLease(entry.Message.Id, token)));
            }

            return Task.FromResult<IReadOnlyList<LeasedDeferral>>(leased);
        }
    }

    public Task<bool> CompleteAsync(DeferralLease lease, CancellationToken ct = default)
    {
        lock (_gate)
        {
            var removed = _entries.RemoveAll(e => e.Message.Id == lease.Id && e.LeaseToken == lease.Token);
            return Task.FromResult(removed > 0);
        }
    }

    public Task<bool> AbandonAsync(DeferralLease lease, DateTimeOffset? visibleAt = null, CancellationToken ct = default)
    {
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
