namespace Talaria.Core.Sagas;

/// <summary>
/// Defines the outcome of a saga transition.
/// Construct via the factories — the only representable states are a completed saga,
/// a transition to a non-null new state, or a deferral.
/// </summary>
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

    public bool IsCompleted { get; }

    public bool IsDeferred { get; }

    public IReadOnlyList<object> OutboundMessages { get; }

    /// <summary>Marks the saga as completed; its state will be purged.</summary>
    public static SagaResult<TState> Complete(IReadOnlyList<object> outboundMessages) =>
        new(default, isCompleted: true, isDeferred: false, outboundMessages);

    /// <summary>Transitions the saga to <paramref name="state"/> (must not be null).</summary>
    public static SagaResult<TState> Transition(TState state, IReadOnlyList<object> outboundMessages) =>
        new(state ?? throw new ArgumentNullException(nameof(state), "A saga transition requires a non-null state."),
            isCompleted: false, isDeferred: false, outboundMessages);

    /// <summary>Defers processing of the current message.</summary>
    public static SagaResult<TState> Defer() =>
        new(default, isCompleted: false, isDeferred: true, Array.Empty<object>());
}
