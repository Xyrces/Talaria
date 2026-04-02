using System.Collections.Concurrent;
using Talaria.Core.Abstractions;

namespace Talaria.Transports.InMemory;

/// <summary>
/// In-memory state store using ConcurrentDictionary.
/// Provides fast, deterministic saga state persistence for testing.
/// </summary>
public sealed class InMemoryStateStore<TState> : IStateStore<TState>
    where TState : class, new()
{
    private readonly ConcurrentDictionary<string, TState> _store = new();

    public Task<TState?> GetAsync(string correlationId, CancellationToken ct = default)
    {
        _store.TryGetValue(correlationId, out var state);
        return Task.FromResult(state);
    }

    public Task SaveAsync(string correlationId, TState state, CancellationToken ct = default)
    {
        _store[correlationId] = state;
        return Task.CompletedTask;
    }

    public Task DeleteAsync(string correlationId, CancellationToken ct = default)
    {
        _store.TryRemove(correlationId, out _);
        return Task.CompletedTask;
    }

    /// <summary>
    /// Returns the number of stored saga states. Useful for test assertions.
    /// </summary>
    public int Count => _store.Count;

    /// <summary>
    /// Clears all stored state. Useful between tests.
    /// </summary>
    public void Clear() => _store.Clear();
}
