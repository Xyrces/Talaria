namespace Talaria.Core.Sagas;

/// <summary>
/// Provides a pure-function API for saga transitions.
/// Does not perform immediate IO; instead it structures the <see cref="SagaResult{TState}"/>.
/// </summary>
public interface ISagaContext<TState>
{
    /// <summary>
    /// Dispatches a message to the transport after the saga state is committed.
    /// </summary>
    ISagaContext<TState> Dispatch<TMessage>(TMessage message) where TMessage : class;

    /// <summary>
    /// Marks the saga as completed. The state will be purged from the state store.
    /// </summary>
    SagaResult<TState> Complete();

    /// <summary>
    /// Transitions the saga to the new state and returns the result.
    /// </summary>
    SagaResult<TState> Transition(TState newState);

    /// <summary>
    /// Defers the processing of the current message (e.g. out of order message).
    /// </summary>
    SagaResult<TState> Defer();
}
