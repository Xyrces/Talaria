// SPDX-License-Identifier: Apache-2.0

using System.Linq;

namespace Talaria.Core.Sagas;

/// <summary>Describes an outbound message staged by a saga step, optionally scheduled for future delivery or marked as a saga timeout.</summary>
/// <param name="Message">The outbound message payload.</param>
/// <param name="ScheduledAt">Optional UTC timestamp when the message becomes eligible for relay; null for immediate dispatch.</param>
/// <param name="IsTimeout">True when staged via <see cref="ISagaContext{TState}.RequestTimeout{TTimeout}(TTimeout, TimeSpan)"/>.</param>
/// <since>1.0.0</since>
public sealed record SagaScheduledDispatch(object Message, DateTimeOffset? ScheduledAt = null, bool IsTimeout = false)
{
    /// <summary>True when staged as a saga compensation command.</summary>
    public bool IsCompensation { get; init; }

    /// <summary>Optional logical name of the compensation step.</summary>
    public string? CompensationStepName { get; init; }

    /// <summary>Optional unique operation identifier for a sequential compensation step.</summary>
    public string? CompensationOperationId { get; init; }

    /// <summary>Optional compensation generation counter.</summary>
    public int? CompensationGeneration { get; init; }
}

/// <summary>
/// Defines the outcome of a saga transition.
/// Construct via the factories — the only representable states are a completed saga,
/// a transition to a non-null new state, a compensation transition/completion, or a deferral.
/// </summary>
/// <typeparam name="TState">The CLR saga state type.</typeparam>
/// <since>1.0.0</since>
public sealed record SagaResult<TState>
{
    private SagaResult(
        TState? state,
        bool isCompleted,
        bool isDeferred,
        IReadOnlyList<object> outboundMessages,
        IReadOnlyList<SagaScheduledDispatch>? dispatches = null,
        IReadOnlyList<Type>? canceledTimeouts = null,
        bool isCompensating = false,
        TState? compensationState = default,
        IReadOnlyList<string>? recordedSteps = null)
    {
        State = state;
        IsCompleted = isCompleted;
        IsDeferred = isDeferred;
        OutboundMessages = outboundMessages;
        Dispatches = dispatches ?? (outboundMessages.Count == 0
            ? Array.Empty<SagaScheduledDispatch>()
            : outboundMessages.Select(static m => new SagaScheduledDispatch(m)).ToArray());
        CanceledTimeouts = canceledTimeouts ?? Array.Empty<Type>();
        IsCompensating = isCompensating;
        CompensationState = compensationState;
        RecordedSteps = recordedSteps ?? Array.Empty<string>();
    }

    /// <summary>The new saga state. Null unless this is a transition.</summary>
    public TState? State { get; }

    /// <summary>True when the result represents a completed saga (state will be purged).</summary>
    public bool IsCompleted { get; }

    /// <summary>True when the result represents a handler-initiated deferral.</summary>
    public bool IsDeferred { get; }

    /// <summary>True when the result initiates or continues saga compensation.</summary>
    public bool IsCompensating { get; init; }

    /// <summary>Optional state snapshot used to evaluate single-phase batch compensation rules when completing.</summary>
    public TState? CompensationState { get; init; }

    /// <summary>Named forward steps recorded during this handler invocation via <see cref="ISagaContext{TState}.RecordStep(string)"/>.</summary>
    public IReadOnlyList<string> RecordedSteps { get; init; } = Array.Empty<string>();

    /// <summary>The messages staged for dispatch after the transition is committed.</summary>
    public IReadOnlyList<object> OutboundMessages { get; }

    /// <summary>The dispatches staged alongside this result, including optional scheduling and timeout metadata.</summary>
    public IReadOnlyList<SagaScheduledDispatch> Dispatches { get; }

    /// <summary>The timeout message types canceled or superseded during this step.</summary>
    public IReadOnlyList<Type> CanceledTimeouts { get; }

    /// <summary>Marks the saga as completed; its state will be purged.</summary>
    /// <param name="outboundMessages">The messages staged for dispatch alongside the completion.</param>
    /// <returns>A completed-saga result.</returns>
    public static SagaResult<TState> Complete(IReadOnlyList<object> outboundMessages) =>
        new(default, isCompleted: true, isDeferred: false, outboundMessages);

    /// <summary>Marks the saga as completed; its state will be purged.</summary>
    /// <param name="outboundMessages">The messages staged for dispatch alongside the completion.</param>
    /// <param name="dispatches">Optional scheduled/timeout dispatch descriptors.</param>
    /// <param name="canceledTimeouts">Optional timeout contract types canceled or superseded during this step.</param>
    /// <returns>A completed-saga result.</returns>
    public static SagaResult<TState> Complete(
        IReadOnlyList<object> outboundMessages,
        IReadOnlyList<SagaScheduledDispatch>? dispatches,
        IReadOnlyList<Type>? canceledTimeouts = null) =>
        new(default, isCompleted: true, isDeferred: false, outboundMessages, dispatches, canceledTimeouts);

    /// <summary>Transitions the saga to <paramref name="state"/> (must not be null).</summary>
    /// <param name="state">The new saga state. Must not be null.</param>
    /// <param name="outboundMessages">The messages staged for dispatch alongside the transition.</param>
    /// <returns>A transition result.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="state"/> is null.</exception>
    public static SagaResult<TState> Transition(TState state, IReadOnlyList<object> outboundMessages) =>
        new(state ?? throw new ArgumentNullException(nameof(state), "A saga transition requires a non-null state."),
            isCompleted: false, isDeferred: false, outboundMessages);

    /// <summary>Transitions the saga to <paramref name="state"/> (must not be null).</summary>
    /// <param name="state">The new saga state. Must not be null.</param>
    /// <param name="outboundMessages">The messages staged for dispatch alongside the transition.</param>
    /// <param name="dispatches">Optional scheduled/timeout dispatch descriptors.</param>
    /// <param name="canceledTimeouts">Optional timeout contract types canceled or superseded during this step.</param>
    /// <param name="recordedSteps">Optional named forward steps recorded during this step.</param>
    /// <returns>A transition result.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="state"/> is null.</exception>
    public static SagaResult<TState> Transition(
        TState state,
        IReadOnlyList<object> outboundMessages,
        IReadOnlyList<SagaScheduledDispatch>? dispatches,
        IReadOnlyList<Type>? canceledTimeouts = null,
        IReadOnlyList<string>? recordedSteps = null) =>
        new(state ?? throw new ArgumentNullException(nameof(state), "A saga transition requires a non-null state."),
            isCompleted: false, isDeferred: false, outboundMessages, dispatches, canceledTimeouts, isCompensating: false, compensationState: default, recordedSteps: recordedSteps);

    /// <summary>Initiates or continues saga compensation with <paramref name="state"/> (must not be null).</summary>
    /// <param name="state">The saga state to persist while compensating. Must not be null.</param>
    /// <param name="outboundMessages">The messages staged for dispatch alongside the transition.</param>
    /// <param name="dispatches">Optional scheduled/timeout/compensation dispatch descriptors.</param>
    /// <param name="canceledTimeouts">Optional timeout contract types canceled during this step.</param>
    /// <param name="recordedSteps">Optional named forward steps recorded during this step.</param>
    /// <returns>A compensating transition result.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="state"/> is null.</exception>
    public static SagaResult<TState> Compensate(
        TState state,
        IReadOnlyList<object> outboundMessages,
        IReadOnlyList<SagaScheduledDispatch>? dispatches = null,
        IReadOnlyList<Type>? canceledTimeouts = null,
        IReadOnlyList<string>? recordedSteps = null) =>
        new(state ?? throw new ArgumentNullException(nameof(state), "A saga compensation transition requires a non-null state."),
            isCompleted: false, isDeferred: false, outboundMessages, dispatches, canceledTimeouts, isCompensating: true, compensationState: state, recordedSteps: recordedSteps);

    /// <summary>Performs single-phase batch compensation and marks the saga completed.</summary>
    /// <param name="compensationState">Optional state snapshot used to evaluate compensation predicates.</param>
    /// <param name="outboundMessages">The messages staged for dispatch alongside the completion.</param>
    /// <param name="dispatches">Optional scheduled/timeout/compensation dispatch descriptors.</param>
    /// <param name="canceledTimeouts">Optional timeout contract types canceled during this step.</param>
    /// <param name="recordedSteps">Optional named forward steps recorded during this step.</param>
    /// <returns>A compensating completion result.</returns>
    public static SagaResult<TState> CompensateAndComplete(
        TState? compensationState,
        IReadOnlyList<object> outboundMessages,
        IReadOnlyList<SagaScheduledDispatch>? dispatches = null,
        IReadOnlyList<Type>? canceledTimeouts = null,
        IReadOnlyList<string>? recordedSteps = null) =>
        new(default, isCompleted: true, isDeferred: false, outboundMessages, dispatches, canceledTimeouts, isCompensating: true, compensationState: compensationState, recordedSteps: recordedSteps);

    /// <summary>Defers processing of the current message.</summary>
    /// <returns>A deferral result. Any staged dispatches are discarded.</returns>
    public static SagaResult<TState> Defer() =>
        new(default, isCompleted: false, isDeferred: true, Array.Empty<object>(), Array.Empty<SagaScheduledDispatch>(), Array.Empty<Type>());
}
