namespace Talaria.Core.Sagas;

/// <summary>
/// Provides a pure-function API for saga transitions.
/// Does not perform immediate IO; instead it structures the <see cref="SagaResult{TState}"/> result.
/// </summary>
/// <typeparam name="TState">The CLR saga state type.</typeparam>
/// <since>1.0.0</since>
public interface ISagaContext<TState>
{
    /// <summary>
    /// Dispatches a message to the transport after the saga state is committed.
    /// </summary>
    /// <typeparam name="TMessage">The CLR message type. Must be one of the saga's declared <c>DispatchTo</c> mappings.</typeparam>
    /// <param name="message">The message payload to dispatch.</param>
    /// <returns>The same context, for fluent chaining.</returns>
    /// <remarks>
    /// The dispatch is staged — it is published only after the step handler returns
    /// <see cref="SagaResult{TState}.Transition"/> or <see cref="SagaResult{TState}.Complete"/> and the state
    /// transition is durably committed.
    /// </remarks>
    ISagaContext<TState> Dispatch<TMessage>(TMessage message) where TMessage : class;

    /// <summary>
    /// Marks the saga as completed. The state will be purged from the state store.
    /// </summary>
    /// <returns>A result describing a completed saga.</returns>
    SagaResult<TState> Complete();

    /// <summary>
    /// Transitions the saga to the new state and returns the result.
    /// </summary>
    /// <param name="newState">The new saga state. Must not be null.</param>
    /// <returns>A result describing a transition with the staged dispatches.</returns>
    SagaResult<TState> Transition(TState newState);

    /// <summary>
    /// Defers the processing of the current message (e.g. out of order message).
    /// Any messages dispatched during this handler invocation are discarded.
    /// </summary>
    /// <returns>A result describing a deferral.</returns>
    SagaResult<TState> Defer();
}
