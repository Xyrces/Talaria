// SPDX-License-Identifier: Apache-2.0

namespace Talaria.Core.Sagas;

/// <summary>
/// A registered saga: its state type and the ordered message steps that drive it.
/// </summary>
/// <since>1.0.0</since>
public class SagaRegistration
{
    /// <summary>The CLR type of the saga state.</summary>
    public required Type StateType { get; init; }

    /// <summary>The ordered message-driven steps of this saga.</summary>
    public List<SagaStepRegistration> Steps { get; } = new();

    /// <summary>
    /// Explicit routing for dispatched messages: message CLR type → topic.
    /// Declared via <c>SagaConfigurator.DispatchTo</c>.
    /// </summary>
    public Dictionary<Type, string> DispatchTopics { get; } = new();

    /// <summary>
    /// Ordered compensation rules declared via <c>CompensateStep</c> or <c>CompensateWith</c>.
    /// </summary>
    public List<SagaCompensationRegistration> Compensations { get; } = new();

    /// <summary>
    /// Late forward message mutators invoked when a forward message arrives while the saga is compensating.
    /// </summary>
    public Dictionary<Type, Action<object, object>> LateArrivalHandlers { get; } = new();

    /// <summary>
    /// Optional callback invoked when all sequential compensations have been acknowledged.
    /// </summary>
    public Func<object, ISagaContext<object>, Task<SagaResult<object>>>? OnCompensationCompleted { get; set; }
}

/// <summary>
/// A single saga step: the topic/message that triggers it and the erased delegate used by the engine.
/// </summary>
/// <since>1.0.0</since>
public class SagaStepRegistration
{
    /// <summary>The topic name this step listens on.</summary>
    public required string TopicName { get; init; }

    /// <summary>The CLR message type that triggers this step.</summary>
    public required Type MessageType { get; init; }

    /// <summary>True for steps that start a new saga instance; false for transitions.</summary>
    public required bool IsStarter { get; init; }

    /// <summary>True for steps that handle a scheduled saga timeout message.</summary>
    public bool IsTimeout { get; init; }

    /// <summary>True for steps that handle a sequential compensation acknowledgment message.</summary>
    public bool IsCompensationAck { get; init; }

    /// <summary>True for standalone steps registered solely via <c>OnLateArrival</c>.</summary>
    public bool IsLateArrival { get; init; }

    /// <summary>The logical compensation step name associated with an acknowledgment step.</summary>
    public string? CompensationStepName { get; init; }

    /// <summary>Optional selector that extracts the compensation operation ID from an acknowledgment payload.</summary>
    public Func<object, string?>? OperationIdSelector { get; init; }

    /// <summary>Optional mutator invoked when a late forward message arrives while the saga is compensating.</summary>
    public Action<object, object>? LateArrivalMutator { get; init; }

    // The generic delegate signature for executing the step from TalariaListener:
    // Task<SagaResult<object>> Execute(object? state, object payload, ISagaContext<object> context)
    /// <summary>The erased async handler invoked for each delivered message.</summary>
    public required Func<object?, object, ISagaContext<object>, Task<SagaResult<object>>> Handler { get; init; }

    /// <summary>Resolves correlation ID from the explicit message strongly typed.</summary>
    public Func<object, string>? CorrelationResolver { get; init; }
}

/// <summary>
/// Describes a declarative compensation rule registered on a saga.
/// </summary>
/// <since>1.0.0</since>
public sealed class SagaCompensationRegistration
{
    /// <summary>The logical name of the step being compensated.</summary>
    public required string StepName { get; init; }

    /// <summary>The CLR type of the compensating command.</summary>
    public required Type CommandType { get; init; }

    /// <summary>The destination topic for the compensating command.</summary>
    public required string CommandTopic { get; init; }

    /// <summary>The CLR type of the acknowledgment message for sequential compensation, or null for batch compensation.</summary>
    public Type? AckType { get; init; }

    /// <summary>The topic on which the acknowledgment message is consumed for sequential compensation.</summary>
    public string? AckTopic { get; init; }

    /// <summary>Erased factory that evaluates whether compensation is required for the given state and returns the command or null.</summary>
    public required Func<object, object?> Factory { get; init; }

    /// <summary>Optional callback invoked with the saga state and acknowledgment payload when a sequential step is acknowledged.</summary>
    public Action<object, object>? OnCompensated { get; init; }

    /// <summary>True when this rule participates in sequential acknowledged compensation (<see cref="AckType"/> is non-null).</summary>
    public bool IsSequential => AckType is not null;
}
