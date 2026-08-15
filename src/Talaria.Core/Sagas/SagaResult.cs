// SPDX-License-Identifier: Apache-2.0

namespace Talaria.Core.Sagas;

/// <summary>
/// Defines the outcome of a saga transition.
/// Construct via the factories — the only representable states are a completed saga,
/// a transition to a non-null new state, or a deferral.
/// </summary>
/// <typeparam name="TState">The CLR saga state type.</typeparam>
/// <since>1.0.0</since>
public sealed record SagaResult<TState>
{
    private SagaResult(TState? state, bool isCompleted, bool isDeferred, IReadOnlyList<object> outboundMessages)
    {
        State = state;
        IsCompleted = isCompleted;
        IsDeferred = isDeferred;
        OutboundMessages = outboundMessages;
    }

    /// <summary>The new saga state. Null unless this is a transition.</summary>
    public TState? State { get; }

    /// <summary>True when the result represents a completed saga (state will be purged).</summary>
    public bool IsCompleted { get; }

    /// <summary>True when the result represents a handler-initiated deferral.</summary>
    public bool IsDeferred { get; }

    /// <summary>The messages staged for dispatch after the transition is committed.</summary>
    public IReadOnlyList<object> OutboundMessages { get; }

    /// <summary>Marks the saga as completed; its state will be purged.</summary>
    /// <param name="outboundMessages">The messages staged for dispatch alongside the completion.</param>
    /// <returns>A completed-saga result.</returns>
    public static SagaResult<TState> Complete(IReadOnlyList<object> outboundMessages) =>
        new(default, isCompleted: true, isDeferred: false, outboundMessages);

    /// <summary>Transitions the saga to <paramref name="state"/> (must not be null).</summary>
    /// <param name="state">The new saga state. Must not be null.</param>
    /// <param name="outboundMessages">The messages staged for dispatch alongside the transition.</param>
    /// <returns>A transition result.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="state"/> is null.</exception>
    public static SagaResult<TState> Transition(TState state, IReadOnlyList<object> outboundMessages) =>
        new(state ?? throw new ArgumentNullException(nameof(state), "A saga transition requires a non-null state."),
            isCompleted: false, isDeferred: false, outboundMessages);

    /// <summary>Defers processing of the current message.</summary>
    /// <returns>A deferral result. Any staged dispatches are discarded.</returns>
    public static SagaResult<TState> Defer() =>
        new(default, isCompleted: false, isDeferred: true, Array.Empty<object>());
}
