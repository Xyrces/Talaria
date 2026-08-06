using System.Collections.Concurrent;
using Talaria.Core.Abstractions;

namespace Talaria.Transports.InMemory;

/// <summary>
/// In-memory idempotency store using ConcurrentDictionary.
/// Provides container-free exactly-once semantics for testing and local development.
/// </summary>
public sealed class InMemoryIdempotencyStore : IIdempotencyStore
{
    private readonly ConcurrentDictionary<string, LockEntry> _store = new();

    public Task<bool> TryAcquireLockAsync(
        string messageId,
        string consumerQueue,
        TimeSpan expiration,
        CancellationToken ct = default)
    {
        var key = $"{consumerQueue}:{messageId}";
        var now = DateTimeOffset.UtcNow;

        while (true)
        {
            if (_store.TryGetValue(key, out var existing))
            {
                if (existing.Expiry > now)
                {
                    // Lock active (either PROCESSING or COMPLETED)
                    return Task.FromResult(false);
                }
            }

            var entry = new LockEntry("PROCESSING", now.Add(expiration));

            if (existing != null)
            {
                if (_store.TryUpdate(key, entry, existing))
                {
                    return Task.FromResult(true);
                }
            }
            else
            {
                if (_store.TryAdd(key, entry))
                {
                    return Task.FromResult(true);
                }
            }
        }
    }

    public Task MarkCompleteAsync(
        string messageId,
        string consumerQueue,
        CancellationToken ct = default)
    {
        var key = $"{consumerQueue}:{messageId}";
        var entry = new LockEntry("COMPLETED", DateTimeOffset.UtcNow.AddDays(30));
        _store[key] = entry;
        return Task.CompletedTask;
    }

    public Task ReleaseLockAsync(
        string messageId,
        string consumerQueue,
        CancellationToken ct = default)
    {
        var key = $"{consumerQueue}:{messageId}";
        _store.TryRemove(key, out _);
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

    private record LockEntry(string Status, DateTimeOffset Expiry);
}
