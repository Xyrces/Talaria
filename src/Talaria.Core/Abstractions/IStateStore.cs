namespace Talaria.Core.Abstractions;

/// <summary>
/// Persists saga state keyed by a correlation ID.
/// Implementations may use in-memory storage, Redis, Kafka compacted topics, etc.
/// </summary>
/// <typeparam name="TState">The CLR saga state type. Must be a reference type with a public parameterless constructor.</typeparam>
/// <remarks>
/// The state store is the source of truth for saga progression. The transactional outbox
/// (<see cref="IOutboxStore"/>) is registered automatically alongside the Redis and InMemory
/// state stores so that <see cref="TransitionAsync"/> can stage outbound messages atomically
/// with the state write; without an outbox, saga dispatch falls back to direct transactional
/// produce, which is not atomic with the state save.
/// </remarks>
/// <since>1.0.0</since>
public interface IStateStore<TState> where TState : class, new()
{
    /// <summary>
    /// Retrieves the current state for the given correlation ID, or null if not found.
    /// </summary>
    /// <param name="correlationId">The saga instance key.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>The current state, or null when no saga exists for the correlation ID.</returns>
    Task<TState?> GetAsync(string correlationId, CancellationToken ct = default);

    /// <summary>
    /// Persists the state for the given correlation ID.
    /// </summary>
    /// <param name="correlationId">The saga instance key.</param>
    /// <param name="state">The new state. Overwrites any existing state.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <remarks>
    /// Does NOT stage outbound messages. For atomic state + dispatch, use
    /// <see cref="TransitionAsync"/> instead.
    /// </remarks>
    Task SaveAsync(string correlationId, TState state, CancellationToken ct = default);

    /// <summary>
    /// Removes the state for the given correlation ID (saga completed).
    /// </summary>
    /// <param name="correlationId">The saga instance key.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <remarks>
    /// A delete against a non-existent correlation ID is a no-op. A crash between
    /// <see cref="SaveAsync"/> and <see cref="DeleteAsync"/> is possible without an outbox;
    /// prefer <see cref="TransitionAsync"/> for completed-saga semantics.
    /// </remarks>
    Task DeleteAsync(string correlationId, CancellationToken ct = default);

    /// <summary>
    /// Atomically applies a saga state transition and stages its outbound messages in
    /// the transactional outbox: either the state update and every outbox entry are
    /// persisted together, or nothing is. A null <paramref name="newState"/> purges the
    /// state (saga completed). Pass an empty outbox for transitions without dispatches.
    /// </summary>
    /// <param name="correlationId">The saga instance key.</param>
    /// <param name="newState">The new state. Null marks the saga completed and purges the prior state.</param>
    /// <param name="outbox">The outbound messages to publish after the state transition. Empty when no dispatches.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <remarks>
    /// This is the only API guaranteed to be atomic across state and outbox. The background
    /// outbox relay (<see cref="IOutboxStore"/>) takes care of publishing staged messages
    /// at-least-once with lease + fencing semantics; downstream consumers dedupe via the
    /// idempotency store.
    /// </remarks>
    Task TransitionAsync(
        string correlationId,
        TState? newState,
        IReadOnlyList<OutboxMessage> outbox,
        CancellationToken ct = default);
}
