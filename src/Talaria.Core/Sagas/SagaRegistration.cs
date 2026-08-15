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

    // The generic delegate signature for executing the step from TalariaListener:
    // Task<SagaResult<object>> Execute(object? state, object payload, ISagaContext<object> context)
    /// <summary>The erased async handler invoked for each delivered message.</summary>
    public required Func<object?, object, ISagaContext<object>, Task<SagaResult<object>>> Handler { get; init; }

    /// <summary>Resolves correlation ID from the explicit message strongly typed.</summary>
    public Func<object, string>? CorrelationResolver { get; init; }
}
