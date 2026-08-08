namespace Talaria.Core.Abstractions;

/// <summary>
/// Persists saga state keyed by a correlation ID.
/// Implementations may use in-memory storage, Redis, Kafka compacted topics, etc.
/// </summary>
public interface IStateStore<TState> where TState : class, new()
{
    /// <summary>
    /// Retrieves the current state for the given correlation ID, or null if not found.
    /// </summary>
    Task<TState?> GetAsync(string correlationId, CancellationToken ct = default);

    /// <summary>
    /// Persists the state for the given correlation ID.
    /// </summary>
    Task SaveAsync(string correlationId, TState state, CancellationToken ct = default);

    /// <summary>
    /// Removes the state for the given correlation ID (saga completed).
    /// </summary>
    Task DeleteAsync(string correlationId, CancellationToken ct = default);

    /// <summary>
    /// Atomically applies a saga state transition and stages its outbound messages in
    /// the transactional outbox: either the state update and every outbox entry are
    /// persisted together, or nothing is. A null <paramref name="newState"/> purges the
    /// state (saga completed). Pass an empty outbox for transitions without dispatches.
    /// </summary>
    Task TransitionAsync(
        string correlationId,
        TState? newState,
        IReadOnlyList<OutboxMessage> outbox,
        CancellationToken ct = default);
}
