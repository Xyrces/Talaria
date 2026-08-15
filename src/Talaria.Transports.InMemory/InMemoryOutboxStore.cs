// SPDX-License-Identifier: Apache-2.0

using Talaria.Core.Abstractions;

namespace Talaria.Transports.InMemory;

/// <summary>
/// In-memory transactional outbox with lease semantics matching <see cref="IOutboxStore"/>.
/// Suitable for lightweight single-process deployments, prototyping, and tests —
/// entries do not survive a process restart. Entries are staged atomically with the
/// saga state write via <see cref="InMemoryStateStore{TState}.TransitionAsync"/> under
/// a shared lock.
/// </summary>
public sealed class InMemoryOutboxStore : IOutboxStore
{
    private sealed record Entry(OutboxMessage Message, long LeaseToken, DateTimeOffset VisibleAt);

    // Shared with InMemoryStateStore<TState> so a state transition and its outbox
    // staging commit under one lock — that is the "transaction" in the outbox pattern.
    internal object Gate { get; } = new();

    private readonly List<Entry> _entries = [];

    /// <summary>Stages entries. Callers must hold <see cref="Gate"/>.</summary>
    internal void Stage(IReadOnlyList<OutboxMessage> messages)
    {
        var now = DateTimeOffset.UtcNow;
        foreach (var message in messages)
        {
            _entries.Add(new Entry(message, LeaseToken: 0, now));
        }
    }

    public Task<IReadOnlyList<LeasedOutboxMessage>> AcquirePendingAsync(
        DateTimeOffset now,
        TimeSpan leaseDuration,
        int maxBatch,
        CancellationToken ct = default)
    {
        lock (Gate)
        {
            var due = _entries
                .Where(e => e.VisibleAt <= now)
                .OrderBy(e => e.VisibleAt)
                .Take(maxBatch)
                .ToList();

            var leased = new List<LeasedOutboxMessage>(due.Count);
            foreach (var entry in due)
            {
                var index = _entries.IndexOf(entry);
                var token = entry.LeaseToken + 1;
                _entries[index] = entry with { LeaseToken = token, VisibleAt = now.Add(leaseDuration) };
                leased.Add(new LeasedOutboxMessage(
                    entry.Message,
                    new OutboxLease(entry.Message.Id, token)));
            }

            return Task.FromResult<IReadOnlyList<LeasedOutboxMessage>>(leased);
        }
    }

    public Task<bool> CompleteAsync(OutboxLease lease, CancellationToken ct = default)
    {
        lock (Gate)
        {
            var removed = _entries.RemoveAll(e => e.Message.Id == lease.Id && e.LeaseToken == lease.Token);
            return Task.FromResult(removed > 0);
        }
    }

    public Task<bool> AbandonAsync(OutboxLease lease, DateTimeOffset? visibleAt = null, CancellationToken ct = default)
    {
        lock (Gate)
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

    /// <summary>Returns the number of pending (not yet completed) entries — test/diagnostic use.</summary>
    public int Count
    {
        get
        {
            lock (Gate)
            {
                return _entries.Count;
            }
        }
    }
}
