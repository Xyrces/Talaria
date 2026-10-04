// SPDX-License-Identifier: Apache-2.0
using Talaria.Core.Abstractions;

namespace Talaria.Transports.InMemory;

/// <summary>Process-local inbox with atomic, fenced processing leases.</summary>
public sealed class InMemoryIdempotencyStore : IIdempotencyStore
{
    private readonly Dictionary<(string Queue, string Id), Entry> _entries = new();
    private readonly object _gate = new();
    private readonly TimeProvider _time;
    private static readonly TimeSpan CompletionTtl = TimeSpan.FromDays(30);
    private static readonly TimeSpan ExpirationSweepInterval = TimeSpan.FromMinutes(5);
    private DateTimeOffset _nextExpirationSweep = DateTimeOffset.MinValue;

    public InMemoryIdempotencyStore(TimeProvider? timeProvider = null) => _time = timeProvider ?? TimeProvider.System;

    public Task<IdempotencyAcquisition> AcquireAsync(string messageId, string consumerQueue, TimeSpan expiration, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        if (expiration <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(expiration));
        lock (_gate)
        {
            var key = (consumerQueue, messageId);
            var now = _time.GetUtcNow();
            PruneExpired(now);
            if (_entries.TryGetValue(key, out var entry) && entry.Expires > now)
                return Task.FromResult(new IdempotencyAcquisition(entry.Completed ? IdempotencyStatus.Completed : IdempotencyStatus.Busy));
            var lease = new IdempotencyLock(messageId, consumerQueue, Guid.NewGuid().ToString("N"));
            _entries[key] = new Entry(lease.Token, now + expiration, false);
            return Task.FromResult(new IdempotencyAcquisition(IdempotencyStatus.Acquired, lease));
        }
    }

    public async Task<IdempotencyLock?> TryAcquireLockAsync(string messageId, string consumerQueue, TimeSpan expiration, CancellationToken ct = default)
        => (await AcquireAsync(messageId, consumerQueue, expiration, ct)).Lock;

    public Task<bool> RenewAsync(IdempotencyLock @lock, TimeSpan expiration, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        if (expiration <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(expiration));
        lock (_gate)
        {
            PruneExpired(_time.GetUtcNow());
            if (!Owns(@lock)) return Task.FromResult(false);
            _entries[(@lock.ConsumerQueue, @lock.MessageId)] = new Entry(@lock.Token, _time.GetUtcNow() + expiration, false);
            return Task.FromResult(true);
        }
    }

    public Task MarkCompleteAsync(IdempotencyLock @lock, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        lock (_gate)
        {
            PruneExpired(_time.GetUtcNow());
            if (!Owns(@lock)) throw new IdempotencyLeaseLostException(@lock.MessageId);
            _entries[(@lock.ConsumerQueue, @lock.MessageId)] = new Entry(@lock.Token, _time.GetUtcNow() + CompletionTtl, true);
        }
        return Task.CompletedTask;
    }

    public Task ReleaseLockAsync(IdempotencyLock @lock, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        lock (_gate)
        {
            PruneExpired(_time.GetUtcNow());
            if (Owns(@lock)) _entries.Remove((@lock.ConsumerQueue, @lock.MessageId));
        }
        return Task.CompletedTask;
    }

    private bool Owns(IdempotencyLock lease) =>
        _entries.TryGetValue((lease.ConsumerQueue, lease.MessageId), out var entry)
        && !entry.Completed && entry.Token == lease.Token && entry.Expires > _time.GetUtcNow();

    public int Count
    {
        get
        {
            lock (_gate)
            {
                PruneExpired(_time.GetUtcNow());
                return _entries.Count;
            }
        }
    }
    public void Clear() { lock (_gate) _entries.Clear(); }
    private void PruneExpired(DateTimeOffset now)
    {
        if (now < _nextExpirationSweep) return;
        _nextExpirationSweep = now + ExpirationSweepInterval;
        foreach (var key in _entries.Where(pair => pair.Value.Expires <= now).Select(pair => pair.Key).ToArray())
            _entries.Remove(key);
    }
    private sealed record Entry(string Token, DateTimeOffset Expires, bool Completed);
}
