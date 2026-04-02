namespace Talaria.Core.Sagas;

public class SagaRegistration
{
    public Type StateType { get; init; } = default!;
    
    // We store the generic message transitions as Funcs taking (payload, headers, resolver, stateStore)
    // Actually, it's probably better to store them as definitions containing their topic names and delegates.
    public List<SagaStepRegistration> Steps { get; } = new();
}

public class SagaStepRegistration
{
    public string TopicName { get; init; } = string.Empty;
    public Type MessageType { get; init; } = default!;
    public bool IsStarter { get; init; }
    
    // The generic delegate signature for executing the step from the HostedService
    // Task<SagaResult<object>> Execute(object state, object payload, MessageHeaders headers)
    public Func<object?, object, ISagaContext<object>, Task<SagaResult<object>>> Handler { get; init; } = default!;

    // Resolves correlation ID from the explicit message strongly typed.
    public Func<object, string>? CorrelationResolver { get; init; }
}
