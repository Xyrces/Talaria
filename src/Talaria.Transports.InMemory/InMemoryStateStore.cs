// SPDX-License-Identifier: Apache-2.0

using System.Collections.Concurrent;
using System.Text.Json;
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
    private readonly TimeProvider _time;
    private readonly object _localGate = new();
    private object Gate => _outbox?.Gate ?? _localGate;
    private readonly Dictionary<string, History> _history = new();
    private DateTimeOffset _nextExpirationSweep = DateTimeOffset.MinValue;
    private static readonly TimeSpan ExpirationSweepInterval = TimeSpan.FromMinutes(5);
    private sealed class History
    {
        public long Version;
        public bool Completed;
        public DateTimeOffset Expires;
        public HashSet<string> Messages = new();
    }

    public Task<SagaSnapshot<TState>> ReadSnapshotAsync(string correlationId, string messageId, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        lock (Gate)
        {
            var history = GetHistory(correlationId);
            _store.TryGetValue(correlationId, out var state);
            return Task.FromResult(new SagaSnapshot<TState>(Clone(state), history.Version, history.Completed, history.Messages.Contains(messageId)));
        }
    }

    public Task<SagaCommitStatus> TryTransitionAsync(string correlationId, string messageId, long expectedVersion,
        TState? newState, IReadOnlyList<OutboxMessage> outbox, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        ArgumentNullException.ThrowIfNull(outbox);
        var snapshot = Clone(newState); // Serialization must succeed before any state changes.
        if (outbox.Count > 0 && _outbox is null) throw new InvalidOperationException("Saga dispatch requires an outbox in the same persistence store.");
        var staged = _outbox is null ? Array.Empty<OutboxMessage>() : InMemoryOutboxStore.Prepare(outbox);
        lock (Gate)
        {
            var history = GetHistory(correlationId);
            if (history.Messages.Contains(messageId)) return Task.FromResult(SagaCommitStatus.AlreadyProcessed);
            if (history.Version != expectedVersion) return Task.FromResult(SagaCommitStatus.Conflict);
            _outbox?.EnsureCanStage(staged);
            Apply(correlationId, snapshot);
            _outbox?.StagePrepared(staged);
            history.Version++;
            history.Completed = newState is null;
            history.Messages.Add(messageId);
            history.Expires = _time.GetUtcNow().AddDays(30);
            return Task.FromResult(SagaCommitStatus.Committed);
        }
    }

    private History GetHistory(string id)
    {
        var now = _time.GetUtcNow();
        if (_history.TryGetValue(id, out var existing) && existing.Expires <= now)
        {
            _history.Remove(id);
            _store.TryRemove(id, out _);
        }
        PruneExpired(now);
        if (!_history.TryGetValue(id, out var history))
            _history[id] = history = new History { Expires = now.AddDays(30) };
        return history;
    }

    private void PruneExpired(DateTimeOffset now)
    {
        if (now < _nextExpirationSweep) return;
        _nextExpirationSweep = now + ExpirationSweepInterval;
        // Every store operation also collects expired correlations. Expiring only the
        // requested key left cold saga IDs and their processed-message sets forever.
        // A short sweep interval avoids scanning the whole in-memory state on every access.
        foreach (var expired in _history.Where(pair => pair.Value.Expires <= now).Select(pair => pair.Key).ToArray())
        {
            _history.Remove(expired);
            _store.TryRemove(expired, out _);
        }
    }

    public InMemoryStateStore(InMemoryOutboxStore? outbox = null, TimeProvider? timeProvider = null)
    {
        _outbox = outbox;
        _time = timeProvider ?? TimeProvider.System;
    }

    public Task<TState?> GetAsync(string correlationId, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        lock (Gate)
        {
            _ = GetHistory(correlationId);
            _store.TryGetValue(correlationId, out var state);
            return Task.FromResult(Clone(state));
        }
    }

    public Task SaveAsync(string correlationId, TState state, CancellationToken ct = default)
    {
        lock (Gate)
        {
            var history = GetHistory(correlationId);
            _store[correlationId] = Clone(state)!;
            history.Version++;
            history.Completed = false;
            history.Expires = _time.GetUtcNow().AddDays(30);
        }
        return Task.CompletedTask;
    }

    public Task DeleteAsync(string correlationId, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        lock (Gate)
        {
            _store.TryRemove(correlationId, out _);
            var history = GetHistory(correlationId);
            history.Version++;
            history.Completed = true;
            history.Expires = _time.GetUtcNow().AddDays(30);
        }
        return Task.CompletedTask;
    }

    public Task TransitionAsync(
        string correlationId,
        TState? newState,
        IReadOnlyList<OutboxMessage> outbox,
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        ArgumentNullException.ThrowIfNull(outbox);
        if (outbox.Count > 0 && _outbox is null)
        {
            throw new InvalidOperationException(
                "Cannot stage outbound saga messages: no InMemoryOutboxStore is available. " +
                "It is registered automatically by UseInMemoryStateStore(); when constructing " +
                "the store manually, pass an InMemoryOutboxStore to the constructor.");
        }

        var snapshot = Clone(newState);
        var staged = _outbox is null ? Array.Empty<OutboxMessage>() : InMemoryOutboxStore.Prepare(outbox);
        lock (Gate)
        {
            var history = GetHistory(correlationId);
            _outbox?.EnsureCanStage(staged);
            Apply(correlationId, snapshot);
            _outbox?.StagePrepared(staged);
            history.Version++;
            history.Completed = newState is null;
            history.Expires = _time.GetUtcNow().AddDays(30);
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
            _store[correlationId] = Clone(newState)!;
        }
    }

    /// <summary>
    /// Returns the number of stored saga states. Useful for test assertions.
    /// </summary>
    public int Count
    {
        get
        {
            lock (Gate)
            {
                PruneExpired(_time.GetUtcNow());
                return _store.Count;
            }
        }
    }

    internal int TrackedCorrelationCount
    {
        get
        {
            lock (Gate)
            {
                PruneExpired(_time.GetUtcNow());
                return _history.Count;
            }
        }
    }


    /// <summary>
    /// Clears all stored state. Useful between tests.
    /// </summary>
    public void Clear()
    {
        lock (Gate) { _store.Clear(); _history.Clear(); }
    }

    private static TState? Clone(TState? state) => state is null
        ? null : JsonSerializer.Deserialize<TState>(JsonSerializer.SerializeToUtf8Bytes(state));
}
