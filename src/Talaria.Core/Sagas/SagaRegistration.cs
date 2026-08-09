// SPDX-License-Identifier: AGPL-3.0-or-later

namespace Talaria.Core.Sagas;

/// <summary>
/// A registered saga: its state type and the ordered message steps that drive it.
/// </summary>
public class SagaRegistration
{
    public required Type StateType { get; init; }

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
public class SagaStepRegistration
{
    public required string TopicName { get; init; }

    public required Type MessageType { get; init; }

    public required bool IsStarter { get; init; }

    // The generic delegate signature for executing the step from the HostedService:
    // Task<SagaResult<object>> Execute(object? state, object payload, ISagaContext<object> context)
    public required Func<object?, object, ISagaContext<object>, Task<SagaResult<object>>> Handler { get; init; }

    // Resolves correlation ID from the explicit message strongly typed.
    public Func<object, string>? CorrelationResolver { get; init; }
}
