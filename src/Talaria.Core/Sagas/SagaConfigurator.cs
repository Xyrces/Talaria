// SPDX-License-Identifier: AGPL-3.0-or-later

using Talaria.Core.Abstractions;

namespace Talaria.Core.Sagas;

/// <summary>
/// Fluent DSL for defining a saga's message-driven state machine.
/// The registration is only added to the registry once configuration completes
/// successfully (see <see cref="Registration.TalariaEndpointExtensions.MapSaga{TState}"/>).
/// </summary>
public class SagaConfigurator<TState> where TState : class, new()
{
    private readonly SagaRegistry _registry;
    private readonly SagaRegistration _registration = new()
    {
        StateType = typeof(TState)
    };

    public SagaConfigurator(SagaRegistry registry)
    {
        _registry = registry;
    }

    /// <summary>
    /// Publishes the configured registration into the registry. Called by MapSaga after
    /// the configure callback completes; a throwing callback leaves nothing registered.
    /// </summary>
    internal void Complete()
    {
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
                var context = new TypedSagaContextWrapper(rawContext);

                var result = await handler(message, context);

                return ToObjectResult(result);
            }
        });
        return this;
    }

    /// <summary>
    /// Declares the topic that dispatched messages of <typeparamref name="TMessage"/> are routed to.
    /// Required for every message type any step of this saga dispatches — the engine throws
    /// at dispatch time when a dispatched type has no mapping (instead of silently deriving
    /// a topic from the CLR type name).
    /// </summary>
    public SagaConfigurator<TState> DispatchTo<TMessage>(string topic) where TMessage : class
    {
        _registration.DispatchTopics[typeof(TMessage)] = topic;
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
                var context = new TypedSagaContextWrapper(rawContext);

                var result = await handler(state, message, context);

                return ToObjectResult(result);
            }
        });
        return this;
    }

    private static SagaResult<object> ToObjectResult(SagaResult<TState> result)
    {
        if (result.IsCompleted)
        {
            return SagaResult<object>.Complete(result.OutboundMessages);
        }

        if (result.IsDeferred)
        {
            return SagaResult<object>.Defer();
        }

        return SagaResult<object>.Transition(result.State!, result.OutboundMessages);
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
            return SagaResult<TState>.Complete(res.OutboundMessages);
        }

        public SagaResult<TState> Defer()
        {
            return SagaResult<TState>.Defer();
        }

        public ISagaContext<TState> Dispatch<TMessage>(TMessage message) where TMessage : class
        {
            _inner.Dispatch(message);
            return this;
        }

        public SagaResult<TState> Transition(TState newState)
        {
            var res = _inner.Transition(newState!);
            return SagaResult<TState>.Transition(newState, res.OutboundMessages);
        }
    }
}
