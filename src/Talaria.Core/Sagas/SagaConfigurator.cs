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
    private readonly List<(Type MessageType, string? Topic, Func<object, string>? CorrelateBy, Action<object, object> Mutator)> _pendingLateArrivals = new();

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
        foreach (var late in _pendingLateArrivals)
        {
            if (!_registration.Steps.Any(s => s.MessageType == late.MessageType))
            {
                var resolvedTopic = string.IsNullOrWhiteSpace(late.Topic)
                    ? Registration.MessageNames.Destination(Registration.MessageNames.Contract(late.MessageType))
                    : late.Topic;

                var capturedMutator = late.Mutator;
                _registration.Steps.Add(new SagaStepRegistration
                {
                    TopicName = resolvedTopic,
                    MessageType = late.MessageType,
                    IsStarter = false,
                    IsLateArrival = true,
                    CorrelationResolver = late.CorrelateBy,
                    LateArrivalMutator = capturedMutator,
                    Handler = (stateObj, msgObj, rawCtx) =>
                    {
                        if (stateObj is null)
                        {
                            return Task.FromResult(rawCtx.Complete());
                        }

                        capturedMutator(stateObj, msgObj);
                        return Task.FromResult(rawCtx.Transition(stateObj));
                    }
                });
            }
        }

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

                return ToObjectResult(result, result.State);
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

                return ToObjectResult(result, state);
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

                return ToObjectResult(result, state);
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

    /// <summary>
    /// Registers a sequential, acknowledged compensation step. When compensation is triggered via
    /// <see cref="ISagaContext{TState}.Compensate(TState)"/>, applicable steps are dispatched one at a time
    /// in reverse order, waiting for <typeparamref name="TAck"/> before dispatching the next compensation step.
    /// </summary>
    /// <typeparam name="TCompensation">The CLR type of the compensating command.</typeparam>
    /// <typeparam name="TAck">The CLR type of the acknowledgment message published when the compensation completes.</typeparam>
    /// <param name="when">Predicate determining whether this step requires compensation given the current saga state.</param>
    /// <param name="create">Factory that creates the compensating command from the saga state.</param>
    /// <param name="onCompensated">Optional callback invoked to mutate saga state when <typeparamref name="TAck"/> arrives.</param>
    /// <param name="correlateBy">Optional explicit correlation resolver for <typeparamref name="TAck"/>.</param>
    /// <param name="operationIdSelector">Optional selector extracting the compensation operation ID from <typeparamref name="TAck"/> for fencing.</param>
    /// <param name="stepName">Optional logical step name used to match <see cref="ISagaContext{TState}.RecordStep(string)"/> entries.</param>
    /// <param name="commandTopic">Optional explicit destination topic for <typeparamref name="TCompensation"/>.</param>
    /// <param name="ackTopic">Optional explicit topic on which <typeparamref name="TAck"/> is consumed.</param>
    /// <returns>The same configurator, for chaining.</returns>
    public SagaConfigurator<TState> CompensateStep<TCompensation, TAck>(
        Func<TState, bool> when,
        Func<TState, TCompensation> create,
        Action<TState, TAck>? onCompensated = null,
        Func<TAck, string>? correlateBy = null,
        Func<TAck, string?>? operationIdSelector = null,
        string? stepName = null,
        string? commandTopic = null,
        string? ackTopic = null)
        where TCompensation : class
        where TAck : class
    {
        ArgumentNullException.ThrowIfNull(when);
        ArgumentNullException.ThrowIfNull(create);

        var resolvedStepName = string.IsNullOrWhiteSpace(stepName)
            ? Registration.MessageNames.Contract(typeof(TCompensation))
            : stepName.Trim();

        string resolvedCmdTopic;
        if (!string.IsNullOrWhiteSpace(commandTopic))
        {
            _registration.DispatchTopics[typeof(TCompensation)] = commandTopic;
            resolvedCmdTopic = commandTopic;
        }
        else if (_registration.DispatchTopics.TryGetValue(typeof(TCompensation), out var existingCmdTopic))
        {
            resolvedCmdTopic = existingCmdTopic;
        }
        else
        {
            resolvedCmdTopic = Registration.MessageNames.Destination(Registration.MessageNames.Contract(typeof(TCompensation)));
            _registration.DispatchTopics.TryAdd(typeof(TCompensation), resolvedCmdTopic);
        }

        var resolvedAckTopic = string.IsNullOrWhiteSpace(ackTopic)
            ? Registration.MessageNames.Destination(Registration.MessageNames.Contract(typeof(TAck)))
            : ackTopic;

        _registration.Compensations.Add(new SagaCompensationRegistration
        {
            StepName = resolvedStepName,
            CommandType = typeof(TCompensation),
            CommandTopic = resolvedCmdTopic,
            AckType = typeof(TAck),
            AckTopic = resolvedAckTopic,
            Factory = stateObj =>
            {
                var typedState = (TState)stateObj;
                if (!when(typedState))
                {
                    return null;
                }

                return create(typedState) ?? throw new InvalidOperationException(
                    $"Compensation factory for '{typeof(TCompensation).Name}' returned null when predicate was true.");
            },
            OnCompensated = onCompensated is null
                ? null
                : (s, a) => onCompensated((TState)s, (TAck)a)
        });

        _registration.Steps.Add(new SagaStepRegistration
        {
            TopicName = resolvedAckTopic,
            MessageType = typeof(TAck),
            IsStarter = false,
            IsCompensationAck = true,
            CompensationStepName = resolvedStepName,
            CorrelationResolver = correlateBy == null ? null : (msg => correlateBy((TAck)msg)),
            OperationIdSelector = operationIdSelector == null ? null : (msg => operationIdSelector((TAck)msg)),
            Handler = static (stateObj, _, rawCtx) =>
                Task.FromResult(stateObj is null ? rawCtx.Complete() : rawCtx.Transition(stateObj))
        });

        return this;
    }

    /// <summary>
    /// Configures a callback invoked once all pending sequential compensation steps have been acknowledged.
    /// When omitted, the saga automatically completes and purges its state after the final acknowledgment.
    /// </summary>
    /// <param name="handler">Async handler that produces the final transition or completion result.</param>
    /// <returns>The same configurator, for chaining.</returns>
    public SagaConfigurator<TState> OnCompensationCompleted(
        Func<TState, ISagaContext<TState>, Task<SagaResult<TState>>> handler)
    {
        ArgumentNullException.ThrowIfNull(handler);
        _registration.OnCompensationCompleted = async (stateObj, rawContext) =>
        {
            var typedState = (TState)stateObj;
            var res = await handler(typedState, new TypedSagaContextWrapper(rawContext));
            return ToObjectResult(res, typedState);
        };
        return this;
    }

    /// <summary>
    /// Configures a hook for a forward message that arrives late while the saga is already in
    /// <see cref="SagaPhase.Compensating"/>. The <paramref name="recordLateEffect"/> mutator updates the
    /// saga state, and the engine automatically appends any newly applicable <c>CompensateStep</c> rules
    /// to the pending compensation queue so the late forward effect is also compensated.
    /// </summary>
    /// <typeparam name="TMessage">The CLR type of the late-arriving forward message.</typeparam>
    /// <param name="recordLateEffect">Mutator that records the late forward effect onto the saga state.</param>
    /// <param name="topic">Optional explicit topic name when no <see cref="On{TMessage}"/> step is registered for <typeparamref name="TMessage"/>.</param>
    /// <param name="correlateBy">Optional explicit correlation resolver when no <see cref="On{TMessage}"/> step is registered for <typeparamref name="TMessage"/>.</param>
    /// <returns>The same configurator, for chaining.</returns>
    public SagaConfigurator<TState> OnLateArrival<TMessage>(
        Action<TState, TMessage> recordLateEffect,
        string? topic = null,
        Func<TMessage, string>? correlateBy = null)
        where TMessage : class
    {
        ArgumentNullException.ThrowIfNull(recordLateEffect);
        Action<object, object> erasedMutator = (s, m) => recordLateEffect((TState)s, (TMessage)m);
        _registration.LateArrivalHandlers[typeof(TMessage)] = erasedMutator;
        _pendingLateArrivals.Add((
            typeof(TMessage),
            topic,
            correlateBy == null ? null : (msg => correlateBy((TMessage)msg)),
            erasedMutator));
        return this;
    }

    /// <summary>
    /// Registers a single-phase batch compensation rule evaluated in reverse order when
    /// <see cref="ISagaContext{TState}.CompensateAndComplete(TState)"/> (or batch <see cref="ISagaContext{TState}.Compensate(TState)"/>) is called.
    /// </summary>
    /// <typeparam name="TCompensation">The CLR type of the compensating command.</typeparam>
    /// <param name="when">Predicate determining whether this compensation command should be staged.</param>
    /// <param name="create">Factory that creates the compensating command from the saga state.</param>
    /// <param name="topic">Optional explicit destination topic for <typeparamref name="TCompensation"/>.</param>
    /// <param name="stepName">Optional logical step name used to match <see cref="ISagaContext{TState}.RecordStep(string)"/> entries.</param>
    /// <returns>The same configurator, for chaining.</returns>
    public SagaConfigurator<TState> CompensateWith<TCompensation>(
        Func<TState, bool> when,
        Func<TState, TCompensation> create,
        string? topic = null,
        string? stepName = null)
        where TCompensation : class
    {
        ArgumentNullException.ThrowIfNull(when);
        ArgumentNullException.ThrowIfNull(create);

        return CompensateWith<TCompensation>(
            state => when(state)
                ? (create(state) ?? throw new InvalidOperationException($"Compensation factory for '{typeof(TCompensation).Name}' returned null when predicate was true."))
                : null,
            topic,
            stepName);
    }

    /// <summary>
    /// Registers a single-phase batch compensation rule on an explicit <paramref name="topic"/>.
    /// </summary>
    /// <typeparam name="TCompensation">The CLR type of the compensating command.</typeparam>
    /// <param name="topic">The destination topic for <typeparamref name="TCompensation"/>.</param>
    /// <param name="when">Predicate determining whether this compensation command should be staged.</param>
    /// <param name="create">Factory that creates the compensating command from the saga state.</param>
    /// <returns>The same configurator, for chaining.</returns>
    public SagaConfigurator<TState> CompensateWith<TCompensation>(
        string topic,
        Func<TState, bool> when,
        Func<TState, TCompensation> create)
        where TCompensation : class
        => CompensateWith(when, create, topic, stepName: null);

    /// <summary>
    /// Registers a single-phase batch compensation rule using a nullable selector.
    /// </summary>
    /// <typeparam name="TCompensation">The CLR type of the compensating command.</typeparam>
    /// <param name="select">Selector that returns the compensating command when applicable, or null when no compensation is needed.</param>
    /// <param name="topic">Optional explicit destination topic for <typeparamref name="TCompensation"/>.</param>
    /// <param name="stepName">Optional logical step name used to match <see cref="ISagaContext{TState}.RecordStep(string)"/> entries.</param>
    /// <returns>The same configurator, for chaining.</returns>
    public SagaConfigurator<TState> CompensateWith<TCompensation>(
        Func<TState, TCompensation?> select,
        string? topic = null,
        string? stepName = null)
        where TCompensation : class
    {
        ArgumentNullException.ThrowIfNull(select);

        var resolvedStepName = string.IsNullOrWhiteSpace(stepName)
            ? Registration.MessageNames.Contract(typeof(TCompensation))
            : stepName.Trim();

        string resolvedTopic;
        if (!string.IsNullOrWhiteSpace(topic))
        {
            _registration.DispatchTopics[typeof(TCompensation)] = topic;
            resolvedTopic = topic;
        }
        else if (_registration.DispatchTopics.TryGetValue(typeof(TCompensation), out var existingTopic))
        {
            resolvedTopic = existingTopic;
        }
        else
        {
            resolvedTopic = Registration.MessageNames.Destination(Registration.MessageNames.Contract(typeof(TCompensation)));
            _registration.DispatchTopics.TryAdd(typeof(TCompensation), resolvedTopic);
        }

        _registration.Compensations.Add(new SagaCompensationRegistration
        {
            StepName = resolvedStepName,
            CommandType = typeof(TCompensation),
            CommandTopic = resolvedTopic,
            AckType = null,
            AckTopic = null,
            Factory = stateObj => select((TState)stateObj)
        });

        return this;
    }

    /// <summary>
    /// Registers a single-phase batch compensation rule on an explicit <paramref name="topic"/> using a nullable selector.
    /// </summary>
    /// <typeparam name="TCompensation">The CLR type of the compensating command.</typeparam>
    /// <param name="topic">The destination topic for <typeparamref name="TCompensation"/>.</param>
    /// <param name="select">Selector that returns the compensating command when applicable, or null when no compensation is needed.</param>
    /// <returns>The same configurator, for chaining.</returns>
    public SagaConfigurator<TState> CompensateWith<TCompensation>(
        string topic,
        Func<TState, TCompensation?> select)
        where TCompensation : class
        => CompensateWith(select, topic, stepName: null);

    private SagaResult<object> ToObjectResult(SagaResult<TState> result, TState? currentState = null)
    {
        if (result.IsDeferred)
        {
            return SagaResult<object>.Defer();
        }

        if (!result.IsCompensating)
        {
            if (result.IsCompleted)
            {
                return SagaResult<object>.Complete(result.OutboundMessages, result.Dispatches, result.CanceledTimeouts);
            }

            return SagaResult<object>.Transition(result.State!, result.OutboundMessages, result.Dispatches, result.CanceledTimeouts, result.RecordedSteps);
        }

        var mergedCanceled = result.CanceledTimeouts
            .Concat(_registration.Steps.Where(static s => s.IsTimeout).Select(static s => s.MessageType))
            .Distinct()
            .ToArray();

        var isSequential = !result.IsCompleted
            && result.State is ICompensatableState
            && _registration.Compensations.Any(static c => c.IsSequential);

        if (isSequential)
        {
            return SagaResult<object>.Compensate(result.State!, result.OutboundMessages, result.Dispatches, mergedCanceled, result.RecordedSteps);
        }

        var evalState = (object?)(result.IsCompleted ? (result.CompensationState ?? currentState) : (result.State ?? currentState));
        if (evalState is ICompensatableState evalComp && result.CompensationState is null && result.RecordedSteps.Count > 0)
        {
            evalComp.Compensation ??= new SagaCompensationState();
            foreach (var step in result.RecordedSteps)
            {
                evalComp.Compensation.RecordedSteps.Add(step);
            }
        }

        var applicable = evalState is not null
            ? SagaCompensationEvaluator.EvaluateApplicableCompensations(_registration, evalState)
            : Array.Empty<(SagaCompensationRegistration Rule, object Command)>();

        var compDispatches = applicable
            .Select(static item => new SagaScheduledDispatch(item.Command)
            {
                IsCompensation = true,
                CompensationStepName = item.Rule.StepName,
                CompensationOperationId = Guid.NewGuid().ToString("N"),
                CompensationGeneration = 1
            })
            .ToArray();

        var combinedDispatches = compDispatches.Concat(result.Dispatches).ToArray();
        var combinedOutbound = combinedDispatches.Select(static d => d.Message).ToArray();

        if (result.IsCompleted)
        {
            return SagaResult<object>.CompensateAndComplete(evalState, combinedOutbound, combinedDispatches, mergedCanceled, result.RecordedSteps);
        }

        if (result.State is ICompensatableState batchCompState)
        {
            batchCompState.Compensation ??= new SagaCompensationState();
            batchCompState.Compensation.Generation++;
            batchCompState.Compensation.Phase = SagaPhase.Compensated;
        }

        return SagaResult<object>.Compensate(result.State!, combinedOutbound, combinedDispatches, mergedCanceled, result.RecordedSteps);
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

        public ISagaContext<TState> RecordStep(string stepName)
        {
            _inner.RecordStep(stepName);
            return this;
        }

        public SagaResult<TState> Transition(TState newState)
        {
            var res = _inner.Transition(newState!);
            return SagaResult<TState>.Transition(newState, res.OutboundMessages, res.Dispatches, res.CanceledTimeouts, res.RecordedSteps);
        }

        public SagaResult<TState> Compensate(TState newState)
        {
            var res = _inner.Compensate(newState!);
            return SagaResult<TState>.Compensate(newState, res.OutboundMessages, res.Dispatches, res.CanceledTimeouts, res.RecordedSteps);
        }

        public SagaResult<TState> CompensateAndComplete(TState? stateSnapshot = default)
        {
            var res = _inner.CompensateAndComplete(stateSnapshot);
            return SagaResult<TState>.CompensateAndComplete(stateSnapshot, res.OutboundMessages, res.Dispatches, res.CanceledTimeouts, res.RecordedSteps);
        }
    }
}

internal static class SagaCompensationEvaluator
{
    public static IReadOnlyList<(SagaCompensationRegistration Rule, object Command)> EvaluateApplicableCompensations(
        SagaRegistration registration,
        object evalState)
    {
        if (registration.Compensations.Count == 0)
        {
            return Array.Empty<(SagaCompensationRegistration, object)>();
        }

        var results = new List<(SagaCompensationRegistration Rule, object Command)>();

        if (evalState is ICompensatableState compState
            && compState.Compensation?.RecordedSteps is { Count: > 0 } recorded)
        {
            var evaluatedNames = new HashSet<string>(StringComparer.Ordinal);
            for (var i = recorded.Count - 1; i >= 0; i--)
            {
                var stepName = recorded[i];
                var rule = registration.Compensations.FirstOrDefault(
                    c => string.Equals(c.StepName, stepName, StringComparison.Ordinal));
                if (rule is not null)
                {
                    evaluatedNames.Add(rule.StepName);
                    var cmd = rule.Factory(evalState);
                    if (cmd is not null)
                    {
                        results.Add((rule, cmd));
                    }
                }
            }

            for (var i = registration.Compensations.Count - 1; i >= 0; i--)
            {
                var rule = registration.Compensations[i];
                if (!evaluatedNames.Contains(rule.StepName))
                {
                    var cmd = rule.Factory(evalState);
                    if (cmd is not null)
                    {
                        results.Add((rule, cmd));
                    }
                }
            }

            return results;
        }

        for (var i = registration.Compensations.Count - 1; i >= 0; i--)
        {
            var rule = registration.Compensations[i];
            var cmd = rule.Factory(evalState);
            if (cmd is not null)
            {
                results.Add((rule, cmd));
            }
        }

        return results;
    }
}
