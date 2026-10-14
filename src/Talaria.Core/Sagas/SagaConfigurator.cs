// SPDX-License-Identifier: Apache-2.0

using System.Threading;
using Talaria.Core.Abstractions;

namespace Talaria.Core.Sagas;

/// <summary>
/// Fluent DSL for defining a saga's message-driven state machine.
/// The registration is only added to the registry once configuration completes
/// successfully (see <see cref="Registration.TalariaEndpointExtensions.MapSaga{TState}"/>).
/// </summary>
/// <typeparam name="TState">The CLR saga state type.</typeparam>
/// <since>1.0.0</since>
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
    /// <typeparam name="TMessage">The CLR message type that triggers this step.</typeparam>
    /// <param name="topic">The topic the step listens on.</param>
    /// <param name="handler">Async handler that returns the new saga state.</param>
    /// <param name="correlateBy">Optional explicit correlation resolver. Defaults to <see cref="CorrelationResolver.Resolve{TMessage}(TMessage, MessageHeaders)"/>.</param>
    /// <returns>The same configurator, for chaining.</returns>
    /// <example>
    /// <code>
    /// services.MapSaga&lt;OrderState&gt;(s =&gt; s
    ///     .StartedBy&lt;OrderPlaced&gt;("orders.placed",
    ///         (msg, ctx) =&gt; ctx.Transition(new OrderState(msg.OrderId))));
    /// </code>
    /// </example>
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
    /// <typeparam name="TMessage">The CLR message type.</typeparam>
    /// <param name="topic">The topic dispatched messages of <typeparamref name="TMessage"/> are routed to.</param>
    /// <returns>The same configurator, for chaining.</returns>
    public SagaConfigurator<TState> DispatchTo<TMessage>(string topic) where TMessage : class
    {
        _registration.DispatchTopics[typeof(TMessage)] = topic;
        return this;
    }

    /// <summary>
    /// Configures an existing saga transition via a message.
    /// </summary>
    /// <typeparam name="TMessage">The CLR message type that triggers this step.</typeparam>
    /// <param name="topic">The topic the step listens on.</param>
    /// <param name="handler">Async handler that receives the current state and returns the next state.</param>
    /// <param name="correlateBy">Optional explicit correlation resolver.</param>
    /// <returns>The same configurator, for chaining.</returns>
    /// <remarks>
    /// When the saga state for the resolved correlation ID is missing (no prior starter was
    /// processed), the engine defers the message if an <see cref="IDeferralStore"/> is registered,
    /// or routes it to the DLQ with reason <c>deferral_unavailable</c> if no deferral store is
    /// registered.
    /// </remarks>
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

    /// <summary>
    /// Configures a timeout step for the saga, automatically registering the dispatch route for <typeparamref name="TTimeout"/>.
    /// </summary>
    /// <typeparam name="TTimeout">The CLR timeout message type.</typeparam>
    /// <param name="handler">Async handler invoked when the timeout expires and the saga is still active.</param>
    /// <param name="correlateBy">Optional explicit correlation resolver.</param>
    /// <param name="topic">Optional explicit topic name. When null or whitespace, the convention destination for <typeparamref name="TTimeout"/> is used.</param>
    /// <returns>The same configurator, for chaining.</returns>
    public SagaConfigurator<TState> OnTimeout<TTimeout>(
        Func<TState, TTimeout, ISagaContext<TState>, Task<SagaResult<TState>>> handler,
        Func<TTimeout, string>? correlateBy = null,
        string? topic = null) where TTimeout : class
    {
        ArgumentNullException.ThrowIfNull(handler);
        var destination = string.IsNullOrWhiteSpace(topic)
            ? Registration.MessageNames.Destination(Registration.MessageNames.Contract(typeof(TTimeout)))
            : topic;

        _registration.DispatchTopics.TryAdd(typeof(TTimeout), destination);
        _registration.Steps.Add(new SagaStepRegistration
        {
            TopicName = destination,
            MessageType = typeof(TTimeout),
            IsStarter = false,
            IsTimeout = true,
            CorrelationResolver = correlateBy == null ? null : (msg => correlateBy((TTimeout)msg)),
            Handler = async (stateObj, msgObj, rawContext) =>
            {
                var state = stateObj as TState ?? throw new InvalidOperationException($"State is missing for timeout message {typeof(TTimeout).Name}");
                var message = (TTimeout)msgObj;
                var context = new TypedSagaContextWrapper(rawContext);

                var result = await handler(state, message, context);

                return ToObjectResult(result);
            }
        });
        return this;
    }

    /// <summary>
    /// Configures a timeout step for the saga on an explicit <paramref name="topic"/>, automatically registering the dispatch route for <typeparamref name="TTimeout"/>.
    /// </summary>
    /// <typeparam name="TTimeout">The CLR timeout message type.</typeparam>
    /// <param name="topic">The topic the timeout step listens on and dispatches to.</param>
    /// <param name="handler">Async handler invoked when the timeout expires and the saga is still active.</param>
    /// <param name="correlateBy">Optional explicit correlation resolver.</param>
    /// <returns>The same configurator, for chaining.</returns>
    public SagaConfigurator<TState> OnTimeout<TTimeout>(
        string topic,
        Func<TState, TTimeout, ISagaContext<TState>, Task<SagaResult<TState>>> handler,
        Func<TTimeout, string>? correlateBy = null) where TTimeout : class
        => OnTimeout(handler, correlateBy, topic);

    private static SagaResult<object> ToObjectResult(SagaResult<TState> result)
    {
        if (result.IsCompleted)
        {
            return SagaResult<object>.Complete(result.OutboundMessages, result.Dispatches, result.CanceledTimeouts);
        }

        if (result.IsDeferred)
        {
            return SagaResult<object>.Defer();
        }

        return SagaResult<object>.Transition(result.State!, result.OutboundMessages, result.Dispatches, result.CanceledTimeouts);
    }

    private class TypedSagaContextWrapper : ISagaContext<TState>
    {
        private readonly ISagaContext<object> _inner;

        public TypedSagaContextWrapper(ISagaContext<object> inner)
        {
            _inner = inner;
        }

        public CancellationToken CancellationToken => _inner.CancellationToken;

        public SagaResult<TState> Complete()
        {
            var res = _inner.Complete();
            return SagaResult<TState>.Complete(res.OutboundMessages, res.Dispatches, res.CanceledTimeouts);
        }

        public SagaResult<TState> Defer()
        {
            _ = _inner.Defer();
            return SagaResult<TState>.Defer();
        }

        public ISagaContext<TState> Dispatch<TMessage>(TMessage message) where TMessage : class
        {
            _inner.Dispatch(message);
            return this;
        }

        public ISagaContext<TState> Dispatch<TMessage>(TMessage message, TimeSpan delay) where TMessage : class
        {
            _inner.Dispatch(message, delay);
            return this;
        }

        public ISagaContext<TState> Dispatch<TMessage>(TMessage message, DateTimeOffset dueAt) where TMessage : class
        {
            _inner.Dispatch(message, dueAt);
            return this;
        }

        public ISagaContext<TState> RequestTimeout<TTimeout>(TTimeout timeout, TimeSpan delay) where TTimeout : class
        {
            _inner.RequestTimeout(timeout, delay);
            return this;
        }

        public ISagaContext<TState> RequestTimeout<TTimeout>(TTimeout timeout, DateTimeOffset dueAt) where TTimeout : class
        {
            _inner.RequestTimeout(timeout, dueAt);
            return this;
        }

        public ISagaContext<TState> CancelTimeout<TTimeout>() where TTimeout : class
        {
            _inner.CancelTimeout<TTimeout>();
            return this;
        }

        public SagaResult<TState> Transition(TState newState)
        {
            var res = _inner.Transition(newState!);
            return SagaResult<TState>.Transition(newState, res.OutboundMessages, res.Dispatches, res.CanceledTimeouts);
        }
    }
}
