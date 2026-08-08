using Talaria.Core.Abstractions;

namespace Talaria.Transports.InMemory;

/// <summary>
/// In-memory deferral store backed by a locked list ordered by due time.
/// Suitable for tests and local development — entries do not survive a process restart.
/// </summary>
public sealed class InMemoryDeferralStore : IDeferralStore
{
    private readonly object _gate = new();
    private readonly List<DeferredMessage> _pending = [];

    public Task EnqueueAsync(DeferredMessage message, CancellationToken ct = default)
    {
        lock (_gate)
        {
            _pending.Add(message);
        }

        return Task.CompletedTask;
    }

    public Task<IReadOnlyList<DeferredMessage>> PopDueAsync(DateTimeOffset now, int maxBatch, CancellationToken ct = default)
    {
        lock (_gate)
        {
            var due = _pending
                .Where(m => m.DueAt <= now)
                .OrderBy(m => m.DueAt)
                .Take(maxBatch)
                .ToList();

            foreach (var message in due)
            {
                _pending.Remove(message);
            }

            return Task.FromResult<IReadOnlyList<DeferredMessage>>(due);
        }
    }

    public Task CompleteAsync(Guid id, CancellationToken ct = default)
    {
        // Entries are removed on pop, so completion is a confirmation no-op.
        // Implemented for interface symmetry with stores that stage claims separately.
        lock (_gate)
        {
            _pending.RemoveAll(m => m.Id == id);
        }

        return Task.CompletedTask;
    }

    public Task RequeueAsync(DeferredMessage message, DateTimeOffset newDueAt, CancellationToken ct = default)
    {
        lock (_gate)
        {
            _pending.Add(message with { DueAt = newDueAt });
        }

        return Task.CompletedTask;
    }

    /// <summary>Returns the number of currently scheduled messages (test/diagnostic use).</summary>
    public int Count
    {
        get
        {
            lock (_gate)
            {
                return _pending.Count;
            }
        }
    }
}
