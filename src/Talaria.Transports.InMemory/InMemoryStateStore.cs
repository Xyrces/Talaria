using System.Collections.Concurrent;
using Talaria.Core.Abstractions;

namespace Talaria.Transports.InMemory;

/// <summary>
/// In-memory state store using ConcurrentDictionary.
/// Suitable for lightweight single-process deployments, prototyping, and tests.
/// When an <see cref="InMemoryOutboxStore"/> is registered (automatic via
/// UseInMemoryStateStore), <see cref="TransitionAsync"/> applies the state change and
/// stages outbound messages under one lock — an atomic unit for a single process.
/// </summary>
public sealed class InMemoryStateStore<TState> : IStateStore<TState>
    where TState : class, new()
{
    private readonly ConcurrentDictionary<string, TState> _store = new();
    private readonly InMemoryOutboxStore? _outbox;

    public InMemoryStateStore(InMemoryOutboxStore? outbox = null)
    {
        _outbox = outbox;
    }

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

    public Task TransitionAsync(
        string correlationId,
        TState? newState,
        IReadOnlyList<OutboxMessage> outbox,
        CancellationToken ct = default)
    {
        if (outbox.Count > 0 && _outbox is null)
        {
            throw new InvalidOperationException(
                "Cannot stage outbound saga messages: no InMemoryOutboxStore is available. " +
                "It is registered automatically by UseInMemoryStateStore(); when constructing " +
                "the store manually, pass an InMemoryOutboxStore to the constructor.");
        }

        if (_outbox is not null)
        {
            // One lock covers the state write and the outbox staging: either both become
            // visible to the relay or neither does.
            lock (_outbox.Gate)
            {
                Apply(correlationId, newState);
                _outbox.Stage(outbox);
            }
        }
        else
        {
            Apply(correlationId, newState);
        }

        return Task.CompletedTask;
    }

    private void Apply(string correlationId, TState? newState)
    {
        if (newState is null)
        {
            _store.TryRemove(correlationId, out _);
        }
        else
        {
            _store[correlationId] = newState;
        }
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
