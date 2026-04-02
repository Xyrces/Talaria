namespace Talaria.Core.Sagas;

internal sealed class SagaContext<TState> : ISagaContext<TState>
{
    private readonly List<object> _outboundMessages = new();

    public ISagaContext<TState> Dispatch<TMessage>(TMessage message) where TMessage : class
    {
        _outboundMessages.Add(message);
        return this;
    }

    public SagaResult<TState> Complete()
    {
        return new SagaResult<TState>(
            State: default,
            IsCompleted: true,
            IsDeferred: false,
            OutboundMessages: _outboundMessages.AsReadOnly());
    }

    public SagaResult<TState> Transition(TState newState)
    {
        return new SagaResult<TState>(
            State: newState,
            IsCompleted: false,
            IsDeferred: false,
            OutboundMessages: _outboundMessages.AsReadOnly());
    }

    public SagaResult<TState> Defer()
    {
        return new SagaResult<TState>(
            State: default,
            IsCompleted: false,
            IsDeferred: true,
            OutboundMessages: Array.Empty<object>());
    }
}
