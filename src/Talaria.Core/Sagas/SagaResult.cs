namespace Talaria.Core.Sagas;

/// <summary>
/// Defines the outcome of a saga transition.
/// </summary>
public record SagaResult<TState>(
    TState? State,
    bool IsCompleted,
    bool IsDeferred,
    IReadOnlyList<object> OutboundMessages);
