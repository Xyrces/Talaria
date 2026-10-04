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
    private readonly HashSet<Guid> _ids = [];
    private long _nextLeaseToken;

    /// <summary>Validates and clones a batch before its state transition begins.</summary>
    internal static IReadOnlyList<OutboxMessage> Prepare(IReadOnlyList<OutboxMessage> messages)
    {
        ArgumentNullException.ThrowIfNull(messages);
        var ids = new HashSet<Guid>();
        var prepared = new OutboxMessage[messages.Count];
        for (var i = 0; i < messages.Count; i++)
        {
            var message = messages[i] ?? throw new ArgumentException("Outbox entries cannot be null.", nameof(messages));
            ArgumentException.ThrowIfNullOrWhiteSpace(message.Topic);
            ArgumentException.ThrowIfNullOrWhiteSpace(message.MessageType);
            ArgumentNullException.ThrowIfNull(message.PayloadJson);
            ArgumentNullException.ThrowIfNull(message.Headers);
            if (!ids.Add(message.Id))
                throw new ArgumentException($"Outbox entry id '{message.Id}' appears more than once in the batch.", nameof(messages));
            prepared[i] = Clone(message);
        }
        return prepared;
    }

    /// <summary>Checks ID uniqueness while callers hold <see cref="Gate"/>.</summary>
    internal void EnsureCanStage(IReadOnlyList<OutboxMessage> messages)
    {
        if (messages.Any(message => _ids.Contains(message.Id)))
            throw new InvalidOperationException("An outbox entry id is already stored.");
    }

    /// <summary>Stages a prepared batch. Callers must hold <see cref="Gate"/> and validate IDs first.</summary>
    internal void StagePrepared(IReadOnlyList<OutboxMessage> messages)
    {
        var now = DateTimeOffset.UtcNow;
        foreach (var message in messages)
        {
            _entries.Add(new Entry(message, LeaseToken: 0, now));
            _ids.Add(message.Id);
        }
    }

    public Task<IReadOnlyList<LeasedOutboxMessage>> AcquirePendingAsync(
        DateTimeOffset now,
        TimeSpan leaseDuration,
        int maxBatch,
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        if (leaseDuration <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(leaseDuration));
        if (maxBatch <= 0) throw new ArgumentOutOfRangeException(nameof(maxBatch));
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
                var token = checked(++_nextLeaseToken);
                _entries[index] = entry with { LeaseToken = token, VisibleAt = now.Add(leaseDuration) };
                leased.Add(new LeasedOutboxMessage(
                    Clone(entry.Message),
                    new OutboxLease(entry.Message.Id, token)) { IsReacquired = entry.LeaseToken != 0 });
            }

            return Task.FromResult<IReadOnlyList<LeasedOutboxMessage>>(leased);
        }
    }

    public Task<bool> CompleteAsync(OutboxLease lease, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        lock (Gate)
        {
            var removed = _entries.RemoveAll(e => e.Message.Id == lease.Id && e.LeaseToken == lease.Token);
            if (removed > 0) _ids.Remove(lease.Id);
            return Task.FromResult(removed > 0);
        }
    }

    public Task<bool> AbandonAsync(OutboxLease lease, DateTimeOffset? visibleAt = null, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
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

    private static OutboxMessage Clone(OutboxMessage message)
        => message with { Headers = new MessageHeaders(message.Headers) };

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
