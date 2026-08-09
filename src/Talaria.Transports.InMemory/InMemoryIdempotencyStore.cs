// SPDX-License-Identifier: AGPL-3.0-or-later

using System.Collections.Concurrent;
using Talaria.Core.Abstractions;

namespace Talaria.Transports.InMemory;

/// <summary>
/// In-memory idempotency store using ConcurrentDictionary.
/// Provides container-free exactly-once semantics for testing and local development.
/// </summary>
public sealed class InMemoryIdempotencyStore : IIdempotencyStore
{
    private static readonly TimeSpan CompletionTtl = TimeSpan.FromDays(30);

    private readonly ConcurrentDictionary<string, LockEntry> _store = new();

    public Task<IdempotencyLock?> TryAcquireLockAsync(
        string messageId,
        string consumerQueue,
        TimeSpan expiration,
        CancellationToken ct = default)
    {
        var key = $"{consumerQueue}:{messageId}";
        var now = DateTimeOffset.UtcNow;

        SweepExpired(now);

        while (true)
        {
            if (_store.TryGetValue(key, out var existing))
            {
                if (existing.Expiry > now)
                {
                    // Lock active (either PROCESSING or COMPLETED)
                    return Task.FromResult<IdempotencyLock?>(null);
                }
            }

            var token = Guid.NewGuid().ToString("N");
            var entry = new LockEntry(token, now.Add(expiration));

            if (existing != null)
            {
                if (_store.TryUpdate(key, entry, existing))
                {
                    return Task.FromResult<IdempotencyLock?>(new IdempotencyLock(messageId, consumerQueue, token));
                }
            }
            else
            {
                if (_store.TryAdd(key, entry))
                {
                    return Task.FromResult<IdempotencyLock?>(new IdempotencyLock(messageId, consumerQueue, token));
                }
            }
        }
    }

    public Task MarkCompleteAsync(
        IdempotencyLock @lock,
        CancellationToken ct = default)
    {
        var key = $"{@lock.ConsumerQueue}:{@lock.MessageId}";
        var entry = new LockEntry(@lock.Token, DateTimeOffset.UtcNow.Add(CompletionTtl));
        _store[key] = entry;
        return Task.CompletedTask;
    }

    public Task ReleaseLockAsync(
        IdempotencyLock @lock,
        CancellationToken ct = default)
    {
        var key = $"{@lock.ConsumerQueue}:{@lock.MessageId}";

        // CAS remove: only delete the entry if we still own it (fencing token match).
        // A stale holder must not remove a newer owner's lock.
        while (_store.TryGetValue(key, out var existing))
        {
            if (existing.Token != @lock.Token)
            {
                return Task.CompletedTask;
            }

            if (_store.TryRemove(new KeyValuePair<string, LockEntry>(key, existing)))
            {
                return Task.CompletedTask;
            }
        }

        return Task.CompletedTask;
    }

    /// <summary>
    /// Returns the total number of entries tracked in the idempotency store.
    /// </summary>
    public int Count => _store.Count;

    /// <summary>
    /// Clears all entries from the store.
    /// </summary>
    public void Clear() => _store.Clear();

    private void SweepExpired(DateTimeOffset now)
    {
        foreach (var kvp in _store)
        {
            if (kvp.Value.Expiry <= now)
            {
                _store.TryRemove(kvp);
            }
        }
    }

    private record LockEntry(string Token, DateTimeOffset Expiry);
}
