using System.Text.Json;
using Talaria.Core.Abstractions;

namespace Talaria.Core.Sagas;

public class SagaConfigurator<TState> where TState : class, new()
{
    private readonly SagaRegistry _registry;
    private readonly SagaRegistration _registration = new SagaRegistration
    {
        StateType = typeof(TState)
    };

    public SagaConfigurator(SagaRegistry registry)
    {
        _registry = registry;
        _registry.Add(_registration);
    }

    /// <summary>
    /// Configures a message that starts a new saga instance.
    /// </summary>
    public SagaConfigurator<TState> StartedBy<TMessage>(
        string topic,
        Func<TMessage, ISagaContext<TState>, Task<SagaResult<TState>>> handler,
        Func<TMessage, string>? correlateBy = null) where TMessage : class
    {
        _registration.Steps.Add(new SagaStepRegistration
        {
            TopicName = topic,
            MessageType = typeof(TMessage),
            IsStarter = true,
            CorrelationResolver = correlateBy == null ? null : (msg => correlateBy((TMessage)msg)),
            Handler = async (stateObj, msgObj, rawContext) =>
            {
                var message = (TMessage)msgObj;
                // Since this is a starter, stateObj should be null or default
                
                // Wrap the object-typed context in a typed wrapper or cast it.
                // Our internal rawContext is actually SagaContext<TState> but masquerading as ISagaContext<object>?
                // Actually we can implement a generic TypedContext in the dispatch pipeline.
                var context = new TypedSagaContextWrapper((ISagaContext<object>)rawContext);
                
                var result = await handler(message, context);

                return new SagaResult<object>(
                    result.State!,
                    result.IsCompleted,
                    result.IsDeferred,
                    result.OutboundMessages);
            }
        });
        return this;
    }

    /// <summary>
    /// Configures an existing saga transition via a message.
    /// </summary>
    public SagaConfigurator<TState> On<TMessage>(
        string topic,
        Func<TState, TMessage, ISagaContext<TState>, Task<SagaResult<TState>>> handler,
        Func<TMessage, string>? correlateBy = null) where TMessage : class
    {
        _registration.Steps.Add(new SagaStepRegistration
        {
            TopicName = topic,
            MessageType = typeof(TMessage),
            IsStarter = false,
            CorrelationResolver = correlateBy == null ? null : (msg => correlateBy((TMessage)msg)),
            Handler = async (stateObj, msgObj, rawContext) =>
            {
                var state = stateObj as TState ?? throw new InvalidOperationException($"State is missing for non-starter message {typeof(TMessage).Name}");
                var message = (TMessage)msgObj;
                var context = new TypedSagaContextWrapper((ISagaContext<object>)rawContext);
                
                var result = await handler(state, message, context);

                return new SagaResult<object>(
                    result.State!,
                    result.IsCompleted,
                    result.IsDeferred,
                    result.OutboundMessages);
            }
        });
        return this;
    }

    private class TypedSagaContextWrapper : ISagaContext<TState>
    {
        private readonly ISagaContext<object> _inner;

        public TypedSagaContextWrapper(ISagaContext<object> inner)
        {
            _inner = inner;
        }

        public SagaResult<TState> Complete()
        {
            var res = _inner.Complete();
            return new SagaResult<TState>(default, res.IsCompleted, res.IsDeferred, res.OutboundMessages);
        }

        public SagaResult<TState> Defer()
        {
             var res = _inner.Defer();
             return new SagaResult<TState>(default, res.IsCompleted, res.IsDeferred, res.OutboundMessages);
        }

        public ISagaContext<TState> Dispatch<TMessage>(TMessage message) where TMessage : class
        {
            _inner.Dispatch(message);
            return this;
        }

        public SagaResult<TState> Transition(TState newState)
        {
            var res = _inner.Transition(newState!);
            return new SagaResult<TState>(newState, res.IsCompleted, res.IsDeferred, res.OutboundMessages);
        }
    }
}
