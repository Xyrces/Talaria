// SPDX-License-Identifier: Apache-2.0

using System.Threading;

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
    /// Cancellation token that is canceled when the saga consumer engine is stopping.
    /// Handlers can observe this token to cooperate with graceful shutdown.
    /// </summary>
    CancellationToken CancellationToken { get; }

    /// <summary>
    /// Dispatches a message to the transport after the saga state is committed.
    /// </summary>
    /// <typeparam name="TMessage">The CLR message type. Must be one of the saga's declared <c>DispatchTo</c> mappings.</typeparam>
    /// <param name="message">The message payload to dispatch.</param>
    /// <returns>The same context, for fluent chaining.</returns>
    /// <remarks>
    /// The dispatch is staged — it is published only after the step handler returns
    /// <see cref="SagaResult{TState}.Transition(TState, IReadOnlyList{object})"/> or <see cref="SagaResult{TState}.Complete(IReadOnlyList{object})"/> and the state
    /// transition is durably committed.
    /// </remarks>
    ISagaContext<TState> Dispatch<TMessage>(TMessage message) where TMessage : class;

    /// <summary>
    /// Stages a message for delayed delivery after the saga state transition is committed.
    /// </summary>
    /// <typeparam name="TMessage">The CLR message type. Must be one of the saga's declared <c>DispatchTo</c> mappings.</typeparam>
    /// <param name="message">The message payload to dispatch.</param>
    /// <param name="delay">Non-negative delay relative to the current UTC time before the message becomes eligible for relay.</param>
    /// <returns>The same context, for fluent chaining.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="message"/> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="delay"/> is negative.</exception>
    ISagaContext<TState> Dispatch<TMessage>(TMessage message, TimeSpan delay) where TMessage : class
    {
        ArgumentNullException.ThrowIfNull(message);
        if (delay < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(delay), delay, "Delay must be non-negative.");
        }

        return Dispatch(message, DateTimeOffset.UtcNow.Add(delay));
    }

    /// <summary>
    /// Stages a message for delivery at or after <paramref name="dueAt"/> once the saga state transition is committed.
    /// </summary>
    /// <typeparam name="TMessage">The CLR message type. Must be one of the saga's declared <c>DispatchTo</c> mappings.</typeparam>
    /// <param name="message">The message payload to dispatch.</param>
    /// <param name="dueAt">The earliest UTC timestamp at which the message becomes eligible for relay.</param>
    /// <returns>The same context, for fluent chaining.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="message"/> is null.</exception>
    ISagaContext<TState> Dispatch<TMessage>(TMessage message, DateTimeOffset dueAt) where TMessage : class
        => throw new NotSupportedException($"Saga context '{GetType().Name}' does not support scheduled dispatch.");

    /// <summary>
    /// Requests a scheduled saga timeout to be delivered after <paramref name="delay"/> if the saga is still active and has not canceled or superseded the timeout.
    /// </summary>
    /// <typeparam name="TTimeout">The CLR timeout message type configured via <c>OnTimeout&lt;TTimeout&gt;</c>.</typeparam>
    /// <param name="timeout">The timeout message payload.</param>
    /// <param name="delay">Non-negative delay relative to the current UTC time before the timeout fires.</param>
    /// <returns>The same context, for fluent chaining.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="timeout"/> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="delay"/> is negative.</exception>
    ISagaContext<TState> RequestTimeout<TTimeout>(TTimeout timeout, TimeSpan delay) where TTimeout : class
    {
        ArgumentNullException.ThrowIfNull(timeout);
        if (delay < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(delay), delay, "Delay must be non-negative.");
        }

        return RequestTimeout(timeout, DateTimeOffset.UtcNow.Add(delay));
    }

    /// <summary>
    /// Requests a scheduled saga timeout to be delivered at or after <paramref name="dueAt"/> if the saga is still active and has not canceled or superseded the timeout.
    /// </summary>
    /// <typeparam name="TTimeout">The CLR timeout message type configured via <c>OnTimeout&lt;TTimeout&gt;</c>.</typeparam>
    /// <param name="timeout">The timeout message payload.</param>
    /// <param name="dueAt">The earliest UTC timestamp at which the timeout fires.</param>
    /// <returns>The same context, for fluent chaining.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="timeout"/> is null.</exception>
    ISagaContext<TState> RequestTimeout<TTimeout>(TTimeout timeout, DateTimeOffset dueAt) where TTimeout : class
        => throw new NotSupportedException($"Saga context '{GetType().Name}' does not support timeouts.");

    /// <summary>
    /// Cancels any pending or previously requested timeout of type <typeparamref name="TTimeout"/> for this saga instance.
    /// </summary>
    /// <typeparam name="TTimeout">The CLR timeout message type to cancel.</typeparam>
    /// <returns>The same context, for fluent chaining.</returns>
    ISagaContext<TState> CancelTimeout<TTimeout>() where TTimeout : class
        => throw new NotSupportedException($"Saga context '{GetType().Name}' does not support timeout cancellation.");

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

    /// <summary>
    /// Records the execution of a named forward step in the compensation journal so that
    /// branching or repeated steps are compensated in exact reverse chronological order.
    /// </summary>
    /// <param name="stepName">The logical name of the forward step that succeeded.</param>
    /// <returns>The same context, for fluent chaining.</returns>
    /// <exception cref="ArgumentException">Thrown when <paramref name="stepName"/> is null, empty, or whitespace.</exception>
    ISagaContext<TState> RecordStep(string stepName)
        => throw new NotSupportedException($"Saga context '{GetType().Name}' does not support step recording.");

    /// <summary>
    /// Initiates or continues durable compensation for <paramref name="newState"/>, canceling all
    /// prior saga timeouts. When sequential <c>CompensateStep</c> rules are configured on an
    /// <see cref="ICompensatableState"/>, persists the pending compensation queue and dispatches
    /// only the first compensating command; if no steps require compensation, runs
    /// <c>OnCompensationCompleted</c> (or completes immediately).
    /// </summary>
    /// <param name="newState">The updated saga state to persist while compensating. Must not be null.</param>
    /// <returns>A result describing a compensating transition.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="newState"/> is null.</exception>
    SagaResult<TState> Compensate(TState newState)
        => throw new NotSupportedException($"Saga context '{GetType().Name}' does not support compensation.");

    /// <summary>
    /// Performs single-phase batch compensation: evaluates all applicable compensation rules in
    /// reverse order against <paramref name="stateSnapshot"/> (or the current step state when null),
    /// stages all compensating commands to the outbox in a single batch (durably requesting
    /// compensation without cross-service execution ordering), cancels prior timeouts, and marks
    /// the saga completed.
    /// </summary>
    /// <param name="stateSnapshot">Optional state snapshot used to evaluate compensation predicates; defaults to the step's current state.</param>
    /// <returns>A result describing a compensating completion.</returns>
    SagaResult<TState> CompensateAndComplete(TState? stateSnapshot = default)
        => throw new NotSupportedException($"Saga context '{GetType().Name}' does not support compensation.");
}
