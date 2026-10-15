// SPDX-License-Identifier: Apache-2.0

using System.Linq;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Talaria.Core.Abstractions;
using Talaria.Core.Registration;
using Talaria.Core.Sagas;

namespace Talaria.Core.Hosting;

/// <summary>
/// Host-agnostic engine that runs supervised consumer loops for all saga registrations,
/// including producer cache, dispatch routes, and state-store accessors.
/// </summary>
internal sealed class SagaConsumerEngine
{
    private readonly ITransport _transport;
    private readonly IServiceProvider _serviceProvider;
    private readonly IReadOnlyList<SagaRegistration> _registrations;
    private readonly TalariaOptions _options;
    private readonly IDeferralStore? _deferralStore;
    private readonly IOutboxStore? _outboxStore;
    private readonly MessageProcessingPipeline _pipeline;
    private readonly ILogger _logger;
    private CancellationToken _intake;
    private TalariaHealth? _health;

    private readonly IReadOnlyDictionary<Type, string> _dispatchRoutes;

    private sealed record StepRoute(
        SagaRegistration Registration,
        SagaStepRegistration Step,
        IStateStoreAccessor StateStore);

    private sealed record StepResolution(StepRoute? Route, bool BelongsToAnotherSaga);

    public SagaConsumerEngine(
        ITransport transport,
        IServiceProvider serviceProvider,
        SagaRegistry registry,
        TalariaOptions options,
        IDeferralStore? deferralStore,
        IOutboxStore? outboxStore,
        MessageProcessingPipeline pipeline,
        ILogger logger)
    {
        _transport = transport;
        _serviceProvider = serviceProvider;
        _registrations = registry.Registrations;
        _options = options;
        _deferralStore = deferralStore;
        _outboxStore = outboxStore;
        _pipeline = pipeline;
        _logger = logger;
        _dispatchRoutes = BuildAndValidateDispatchRoutes();
    }

    public IReadOnlyDictionary<Type, string> DispatchRoutes => _dispatchRoutes;


    public async Task RunAsync(CancellationToken ct, CancellationToken? intake = null, TalariaHealth? health = null)
    {
        _intake = intake ?? ct;
        _health = health;
        var retryCoordinator = new RetryCoordinator(_deferralStore, _options, _logger);

        var stepsByTopic = _registrations
            .SelectMany(r => r.Steps.Select(s => new StepRoute(r, s, CreateStateStoreAccessor(r.StateType))))
            .GroupBy(x => (Topic: x.Step.TopicName, StateType: x.Registration.StateType))
            .ToDictionary(g => g.Key, g => (IReadOnlyList<StepRoute>)g.ToList());
        var knownMessageTypesByTopic = _registrations
            .SelectMany(r => r.Steps.Select(s => (s.TopicName, s.MessageType)))
            .GroupBy(x => x.TopicName)
            .ToDictionary(g => g.Key, g => (IReadOnlyList<Type>)g.Select(x => x.MessageType).Distinct().ToList());

        if (_deferralStore is null && stepsByTopic.Count > 0)
        {
            _logger.LogWarning(
                "No IDeferralStore is registered. Out-of-order saga messages, handler-initiated " +
                "deferrals, and delayed retries will be routed to the DLQ instead of being deferred. " +
                "Register one via UseRedisDeferralStore() or UseInMemoryDeferralStore().");
        }

        if (_outboxStore is null && DispatchRoutes.Count > 0)
        {
            throw new InvalidOperationException("Saga dispatch requires an outbox in the same persistence store.");
        }

        var tasks = stepsByTopic.Select(kvp =>
        {
            var name = EndpointName(kvp.Value[0].Registration, kvp.Key.Topic);
            health?.SetEndpoint(name, false);
            return ConsumerSupervision.RunSupervisedAsync(name,
                _ => ConsumeTopicLoopAsync(kvp.Key.Topic, kvp.Value, knownMessageTypesByTopic[kvp.Key.Topic], retryCoordinator, ct), _logger,
                _intake, ex => health?.SetEndpoint(name, false, ex.GetType().Name));
        }).ToList();

        _logger.LogInformation("Talaria Sagas: started {Count} saga topic consumer loops.", tasks.Count);

        await Task.WhenAll(tasks);
    }

    private IReadOnlyDictionary<Type, string> BuildAndValidateDispatchRoutes()
    {
        var routes = new Dictionary<Type, string>();
        foreach (var reg in _registrations)
        {
            foreach (var (type, topic) in reg.DispatchTopics)
            {
                routes[type] = topic;
            }
        }

        var consumedTopics = new HashSet<string>(_registrations.SelectMany(r => r.Steps.Select(s => s.TopicName)));
        var topicRegistry = _serviceProvider.GetService<TopicRegistry>();
        if (topicRegistry != null)
        {
            foreach (var t in topicRegistry.Registrations)
            {
                consumedTopics.Add(t.TopicName);
            }
        }

        foreach (var topic in routes.Values.Distinct())
        {
            if (!consumedTopics.Contains(topic))
            {
                _logger.LogWarning(
                    "Saga dispatch target topic '{Topic}' has no registered consumer in this host. " +
                    "If it is not consumed by another service, dispatched messages will go unread.",
                    topic);
            }
        }

        return routes;
    }

    private async Task ConsumeTopicLoopAsync(
        string topic,
        IReadOnlyList<StepRoute> routes,
        IReadOnlyList<Type> knownMessageTypes,
        RetryCoordinator retryCoordinator,
        CancellationToken ct)
    {
        await using var consumer = await _transport.CreateConsumerAsync<JsonElement>(
            topic,
            new ConsumerOptions { ConsumerGroup = EndpointName(routes[0].Registration, topic) },
            ct);

        await foreach (var env in ConsumerReader.ReadAsync(consumer, _health, EndpointName(routes[0].Registration, topic), _intake, ct))
        {
            var endpoint = EndpointName(routes[0].Registration, topic);
            if (env.Headers.TryGetValue("talaria.retry.endpoint", out var target) && target != endpoint)
            {
                await consumer.CommitAsync(env, ct);
                continue;
            }
            var resolution = ResolveStep(env, routes, knownMessageTypes);
            if (resolution.BelongsToAnotherSaga)
            {
                await consumer.CommitAsync(env, ct);
                continue;
            }

            var route = resolution.Route;
            if (route is null)
            {
                var typeHeader = env.Headers.TryGetValue(MessageHeaders.MessageTypeKey, out var v) ? v : "(none)";
                _logger.LogWarning(
                    "No saga step found for message on topic '{Topic}' (message type header: '{TypeHeader}'). Routing to DLQ.",
                    topic, typeHeader);

                env.Headers.DlqReason = "unknown_message_type";
                Diagnostics.TalariaDiagnostics.DlqRouted.Add(1, new KeyValuePair<string, object?>("messaging.destination.name", topic));

                await consumer.NackAsync(env, ct);
                continue;
            }

            await ProcessStepMessageAsync(env, route, consumer, retryCoordinator, ct);
        }
    }

    internal static string EndpointName(string application, Type stateType, string topic)
        => MessageNames.Destination("saga:" + application + ":" + MessageNames.Contract(stateType) + ":" + topic);

    private string EndpointName(SagaRegistration registration, string topic)
        => EndpointName(_options.ApplicationName, registration.StateType, topic);

    private static StepResolution ResolveStep(
        MessageEnvelope<JsonElement> env,
        IReadOnlyList<StepRoute> routes,
        IReadOnlyList<Type> knownMessageTypes)
    {
        var typeName = env.Headers.TryGetValue(MessageHeaders.MessageTypeKey, out var v) ? v : null;
        if (typeName is null)
        {
            // Preserve the legacy untagged-message behavior for a subscription with a
            // single local route. Ambiguous subscriptions still require a type header.
            return new StepResolution(routes.Count == 1 ? routes[0] : null, BelongsToAnotherSaga: false);
        }

        static bool Matches(Type messageType, string name)
            => string.Equals(messageType.FullName, name, StringComparison.Ordinal)
                || string.Equals(messageType.Name, name, StringComparison.Ordinal);

        var route = routes.FirstOrDefault(r => Matches(r.Step.MessageType, typeName));
        if (route is not null)
        {
            return new StepResolution(route, BelongsToAnotherSaga: false);
        }

        var belongsToAnotherSaga = knownMessageTypes.Any(messageType => Matches(messageType, typeName));
        return new StepResolution(null, belongsToAnotherSaga);
    }

    private async Task ProcessStepMessageAsync(
        MessageEnvelope<JsonElement> env,
        StepRoute route,
        IConsumer<JsonElement> consumer,
        RetryCoordinator retryCoordinator,
        CancellationToken ct)
    {
        var step = route.Step;
        var stateStore = route.StateStore;
        var stateType = route.Registration.StateType;

        using var activity = Diagnostics.TalariaDiagnostics.StartConsumerActivity(
            step.TopicName, step.MessageType.Name, env.Headers);
        activity?.SetTag("saga.type", stateType.Name);

        var sw = System.Diagnostics.Stopwatch.StartNew();

        try
        {
            if ((env.Headers.TryGetValue("talaria.saga.timeout_saga", out var timeoutOwnerSaga)
                    && !string.IsNullOrWhiteSpace(timeoutOwnerSaga)
                    && !string.Equals(timeoutOwnerSaga, MessageNames.Contract(stateType), StringComparison.Ordinal))
                || (env.Headers.TryGetValue("talaria.saga.timeout_application", out var timeoutOwnerApp)
                    && !string.IsNullOrWhiteSpace(timeoutOwnerApp)
                    && !string.Equals(timeoutOwnerApp, _options.ApplicationName, StringComparison.Ordinal)))
            {
                await consumer.CommitAsync(env, ct);
                return;
            }

            await using var scope = _serviceProvider.CreateAsyncScope();

            object payload;
            try
            {
                payload = env.Payload.Deserialize(step.MessageType)
                    ?? throw new JsonException($"Payload deserialized to null for {step.MessageType.Name}.");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to deserialize saga message on topic '{Topic}' as {MessageType}.", step.TopicName, step.MessageType.Name);
                env.Headers.DlqReason = "deserialization_failed";
                Diagnostics.TalariaDiagnostics.MessagesFailed.Add(1, new KeyValuePair<string, object?>("messaging.destination.name", step.TopicName));
                Diagnostics.TalariaDiagnostics.DlqRouted.Add(1, new KeyValuePair<string, object?>("messaging.destination.name", step.TopicName));
                await consumer.NackAsync(env, ct);
                return;
            }

            var correlationId = step.CorrelationResolver != null
                ? step.CorrelationResolver(payload)
                : Core.Sagas.CorrelationResolver.Resolve(payload, env.Headers);
            if (string.IsNullOrWhiteSpace(correlationId))
            {
                correlationId = env.Headers.CorrelationId;
            }


            if (_pipeline.IsHopCountExceeded(env, step.TopicName))
            {
                activity?.SetStatus(System.Diagnostics.ActivityStatusCode.Error, "Exceeded Max Hops");
                Diagnostics.TalariaDiagnostics.MessagesFailed.Add(1, new KeyValuePair<string, object?>("messaging.destination.name", step.TopicName));
                Diagnostics.TalariaDiagnostics.DlqRouted.Add(1, new KeyValuePair<string, object?>("messaging.destination.name", step.TopicName));
                await consumer.NackAsync(env, ct);
                return;
            }

            if (string.IsNullOrWhiteSpace(correlationId))
            {
                _logger.LogWarning("No correlation ID found for saga message {Type} on topic {Topic}", step.MessageType.Name, env.SourceTopic);

                env.Headers.DlqReason = "missing_correlation_id";
                activity?.SetStatus(System.Diagnostics.ActivityStatusCode.Error, "Missing Correlation Id");
                Diagnostics.TalariaDiagnostics.MessagesFailed.Add(1, new KeyValuePair<string, object?>("messaging.destination.name", step.TopicName));
                Diagnostics.TalariaDiagnostics.DlqRouted.Add(1, new KeyValuePair<string, object?>("messaging.destination.name", step.TopicName));

                await consumer.NackAsync(env, ct);
                return;
            }

            activity?.SetTag("saga.correlation_id", correlationId);

            var gate = await _pipeline.AcquireAsync(env, EndpointName(route.Registration, step.TopicName), ct);
            if (gate.IsDuplicate)
            {
                _logger.LogDebug("Saga Message {MessageId} skipped. Idempotency lock claimed by another worker or already completed.", env.Headers.MessageId);
                await consumer.CommitAsync(env, ct);
                return;
            }

            env.Headers["talaria.retry.endpoint"] = EndpointName(route.Registration, step.TopicName);
            await using var ownership = _pipeline.Renew(gate.Lock, ct);
            var messageId = env.Headers.MessageId ?? throw new InvalidOperationException("Saga deliveries require a stable message ID.");
            var snapshot = await stateStore.ReadSnapshotAsync(scope.ServiceProvider, correlationId, messageId, ct);
            if (snapshot.MessageProcessed || snapshot.IsCompleted)
            {
                await _pipeline.CompleteAsync(gate.Lock, consumer, env, ct);
                return;
            }

            if (env.Headers.TryGetValue("talaria.saga.timeout_version", out var timeoutVersion) && !string.IsNullOrWhiteSpace(timeoutVersion))
            {
                var cancelReceiptKey = $"timeout-canceled:{MessageNames.Contract(step.MessageType)}:v{timeoutVersion}";
                var cancelCheck = await stateStore.ReadSnapshotAsync(scope.ServiceProvider, correlationId, cancelReceiptKey, ct);
                if (cancelCheck.MessageProcessed)
                {
                    _logger.LogDebug(
                        "Saga {SagaType} timeout {MessageType} (v{TimeoutVersion}) for correlation {CorrelationId} was canceled or superseded; skipping.",
                        stateType.Name, step.MessageType.Name, timeoutVersion, correlationId);
                    await _pipeline.CompleteAsync(gate.Lock, consumer, env, ct);
                    return;
                }
            }

            var state = snapshot.State;

            if (state == null && !step.IsStarter)
            {
                if (step.IsTimeout || step.IsCompensationAck || (env.Headers.TryGetValue("talaria.saga.timeout", out var isTimeout) && string.Equals(isTimeout, "true", StringComparison.OrdinalIgnoreCase)))
                {
                    _logger.LogDebug(
                        "Received timeout or compensation ack message {MessageType} for Saga {SagaType} with correlation {Id}, but no active saga state exists. Completing as stale message.",
                        step.MessageType.Name, stateType.Name, correlationId);
                    await _pipeline.CompleteAsync(gate.Lock, consumer, env, ct);
                    return;
                }

                _logger.LogInformation("Received non-starter message for Saga {SagaType} but state {Id} not found. Deferring...", stateType.Name, correlationId);
                try
                {
                    await HandleDeferralAsync(env, payload, route.Registration, step, correlationId, ct);

                    // Commit the original envelope BEFORE releasing the idempotency lock.
                    // The deferred copy carries a deterministic MessageId ({original}:defer:{attempt}),
                    // so any duplicate copies produced by a redelivery are suppressed by the
                    // idempotency gate. If commit fails, release the lock best-effort so the
                    // original can redeliver promptly rather than waiting for the lock TTL to expire.
                    try
                    {
                        await consumer.CommitAsync(env, ct);
                    }
                    catch (Exception commitEx)
                    {
                        _logger.LogError(commitEx, "Failed to commit original envelope after deferring saga message {MessageId}; it remains uncommitted for redelivery.", env.Headers.MessageId);
                        await ReleaseLockBestEffortAsync(gate.Lock, ct);
                        throw;
                    }

                    await ReleaseLockBestEffortAsync(gate.Lock, ct);

                    Diagnostics.TalariaDiagnostics.MessagesDeferred.Add(1, new KeyValuePair<string, object?>("saga.type", stateType.Name));
                }
                catch (DeferralRejectedException ex)
                {
                    Diagnostics.TalariaDiagnostics.MessagesFailed.Add(1, new KeyValuePair<string, object?>("messaging.destination.name", step.TopicName));
                    Diagnostics.TalariaDiagnostics.DlqRouted.Add(1, new KeyValuePair<string, object?>("messaging.destination.name", step.TopicName));

                    await _pipeline.FailAsync(gate.Lock, consumer, env, ex, null, ct);
                }
                return;
            }

            if (state != null && step.IsStarter && env.Headers.RetryAttempt == 0)
            {
                _logger.LogWarning(
                    "Saga {SagaType} starter message {MessageId} on topic {Topic} received, but state for correlation {CorrelationId} already exists. Skipping as an idempotent replay.",
                    stateType.Name, env.Headers.MessageId, step.TopicName, correlationId);

                await _pipeline.CompleteAsync(gate.Lock, consumer, env, ct);
                return;
            }

            if (step.IsCompensationAck)
            {
                if (state is not ICompensatableState ackCompCheck
                    || ackCompCheck.Compensation is null
                    || ackCompCheck.Compensation.Phase != SagaPhase.Compensating
                    || ackCompCheck.Compensation.PendingSteps.Count == 0)
                {
                    await _pipeline.CompleteAsync(gate.Lock, consumer, env, ct);
                    return;
                }

                var activePendingCheck = ackCompCheck.Compensation.PendingSteps[0];
                if (!string.Equals(activePendingCheck.StepName, step.CompensationStepName, StringComparison.Ordinal))
                {
                    await _pipeline.CompleteAsync(gate.Lock, consumer, env, ct);
                    return;
                }

                if (env.Headers.TryGetValue("talaria.saga.compensation_operation_id", out var headerOpId)
                    && !string.IsNullOrWhiteSpace(headerOpId)
                    && (!string.Equals(headerOpId, activePendingCheck.OperationId, StringComparison.Ordinal)
                        || ackCompCheck.Compensation.CompletedSteps.Contains(headerOpId)))
                {
                    await _pipeline.CompleteAsync(gate.Lock, consumer, env, ct);
                    return;
                }

                if (step.OperationIdSelector is not null)
                {
                    var payloadOpId = step.OperationIdSelector(payload);
                    if (!string.IsNullOrWhiteSpace(payloadOpId)
                        && (!string.Equals(payloadOpId, activePendingCheck.OperationId, StringComparison.Ordinal)
                            || ackCompCheck.Compensation.CompletedSteps.Contains(payloadOpId)))
                    {
                        await _pipeline.CompleteAsync(gate.Lock, consumer, env, ct);
                        return;
                    }
                }

                if (env.Headers.TryGetValue("talaria.saga.compensation_generation", out var headerGen)
                    && int.TryParse(headerGen, out var parsedGen)
                    && parsedGen != activePendingCheck.Generation)
                {
                    await _pipeline.CompleteAsync(gate.Lock, consumer, env, ct);
                    return;
                }
            }
            else if (state is ICompensatableState activeComp
                && activeComp.Compensation is { Phase: not SagaPhase.Forward })
            {
                var hasLateArrivalHandler = activeComp.Compensation.Phase == SagaPhase.Compensating
                    && (route.Registration.LateArrivalHandlers.ContainsKey(step.MessageType)
                        || step.LateArrivalMutator is not null);

                if (!hasLateArrivalHandler)
                {
                    _logger.LogDebug(
                        "Saga {SagaType} ({CorrelationId}) is in phase {Phase}; ignoring message {MessageType} on topic '{Topic}'.",
                        stateType.Name,
                        correlationId,
                        activeComp.Compensation.Phase,
                        step.MessageType.Name,
                        step.TopicName);

                    Diagnostics.TalariaDiagnostics.CompensationLateMessageIgnored.Add(
                        1,
                        new KeyValuePair<string, object?>("saga.type", stateType.Name),
                        new KeyValuePair<string, object?>("messaging.destination.name", step.TopicName));

                    await _pipeline.CompleteAsync(gate.Lock, consumer, env, ct);
                    return;
                }
            }

            var timeProvider = scope.ServiceProvider.GetService<TimeProvider>();
            var context = new Core.Sagas.SagaContext<object>(timeProvider) { CancellationToken = ownership.Token };
            Core.Sagas.SagaResult<object> result;
            PendingCompensationEntry? pendingToStage = null;
            try
            {
                if (step.IsCompensationAck)
                {
                    var ackCompState = (ICompensatableState)state!;
                    var activePending = ackCompState.Compensation.PendingSteps[0];
                    var matchingRule = route.Registration.Compensations.FirstOrDefault(
                        c => string.Equals(c.StepName, activePending.StepName, StringComparison.Ordinal));
                    matchingRule?.OnCompensated?.Invoke(state!, payload);

                    ackCompState.Compensation.PendingSteps.RemoveAt(0);
                    ackCompState.Compensation.CompletedSteps.Add(activePending.OperationId);
                    if (!ackCompState.Compensation.CompletedSteps.Contains(activePending.StepName))
                    {
                        ackCompState.Compensation.CompletedSteps.Add(activePending.StepName);
                    }

                    if (ackCompState.Compensation.PendingSteps.Count > 0)
                    {
                        pendingToStage = ackCompState.Compensation.PendingSteps[0];
                        result = Core.Sagas.SagaResult<object>.Transition(state!, Array.Empty<object>());
                    }
                    else
                    {
                        ackCompState.Compensation.Phase = SagaPhase.Compensated;
                        Diagnostics.TalariaDiagnostics.SagaCompensationCompleted.Add(
                            1,
                            new KeyValuePair<string, object?>("saga.type", stateType.Name),
                            new KeyValuePair<string, object?>("compensation.mode", "sequential"));

                        if (route.Registration.OnCompensationCompleted is not null)
                        {
                            result = await route.Registration.OnCompensationCompleted(state!, context);
                        }
                        else
                        {
                            result = Core.Sagas.SagaResult<object>.Complete(Array.Empty<object>());
                        }
                    }
                }
                else if (state is ICompensatableState lateCompState
                    && lateCompState.Compensation is { Phase: SagaPhase.Compensating })
                {
                    var mutator = route.Registration.LateArrivalHandlers.TryGetValue(step.MessageType, out var registeredMutator)
                        ? registeredMutator
                        : step.LateArrivalMutator!;

                    var existingSteps = lateCompState.Compensation.PendingSteps
                        .Select(static p => p.StepName)
                        .Concat(lateCompState.Compensation.CompletedSteps)
                        .ToHashSet(StringComparer.Ordinal);

                    mutator(state, payload);

                    var applicableLate = SagaCompensationEvaluator.EvaluateApplicableCompensations(route.Registration, state);
                    var nextIndex = lateCompState.Compensation.PendingSteps.Count + lateCompState.Compensation.CompletedSteps.Count;
                    foreach (var (rule, cmd) in applicableLate)
                    {
                        if (rule.IsSequential && !existingSteps.Contains(rule.StepName))
                        {
                            var resolvedTopic = route.Registration.DispatchTopics.TryGetValue(rule.CommandType, out var mapped)
                                ? mapped
                                : rule.CommandTopic;
                            var opId = $"{correlationId}:comp:{lateCompState.Compensation.Generation}:{nextIndex}:{rule.StepName}";
                            nextIndex++;
                            lateCompState.Compensation.PendingSteps.Add(new PendingCompensationEntry(
                                opId,
                                rule.StepName,
                                rule.CommandType.AssemblyQualifiedName ?? rule.CommandType.FullName!,
                                resolvedTopic,
                                JsonSerializer.Serialize(cmd, rule.CommandType),
                                lateCompState.Compensation.Generation));
                            existingSteps.Add(rule.StepName);
                        }
                    }

                    result = Core.Sagas.SagaResult<object>.Transition(state, Array.Empty<object>());
                }
                else
                {
                    result = await step.Handler(state ?? stateStore.NewState(), payload, context);

                    if (result.IsCompensating)
                    {
                        if (!result.IsCompleted
                            && result.State is ICompensatableState seqCompState
                            && route.Registration.Compensations.Any(static c => c.IsSequential))
                        {
                            seqCompState.Compensation ??= new SagaCompensationState();
                            if (seqCompState.Compensation.Phase == SagaPhase.Forward)
                            {
                                seqCompState.Compensation.Generation++;
                                seqCompState.Compensation.Phase = SagaPhase.Compensating;

                                var applicable = SagaCompensationEvaluator.EvaluateApplicableCompensations(route.Registration, result.State);
                                var index = 0;
                                var mixedBatchDispatches = new List<SagaScheduledDispatch>();
                                foreach (var (rule, cmd) in applicable)
                                {
                                    if (rule.IsSequential)
                                    {
                                        var resolvedTopic = route.Registration.DispatchTopics.TryGetValue(rule.CommandType, out var mapped)
                                            ? mapped
                                            : rule.CommandTopic;
                                        var opId = $"{correlationId}:comp:{seqCompState.Compensation.Generation}:{index}:{rule.StepName}";
                                        index++;
                                        seqCompState.Compensation.PendingSteps.Add(new PendingCompensationEntry(
                                            opId,
                                            rule.StepName,
                                            rule.CommandType.AssemblyQualifiedName ?? rule.CommandType.FullName!,
                                            resolvedTopic,
                                            JsonSerializer.Serialize(cmd, rule.CommandType),
                                            seqCompState.Compensation.Generation));
                                    }
                                    else
                                    {
                                        mixedBatchDispatches.Add(new SagaScheduledDispatch(cmd)
                                        {
                                            IsCompensation = true,
                                            CompensationStepName = rule.StepName,
                                            CompensationOperationId = Guid.NewGuid().ToString("N"),
                                            CompensationGeneration = seqCompState.Compensation.Generation
                                        });
                                    }
                                }

                                if (mixedBatchDispatches.Count > 0)
                                {
                                    var combined = mixedBatchDispatches.Concat(result.Dispatches).ToArray();
                                    var combinedOutbound = combined.Select(static d => d.Message).ToArray();
                                    result = Core.Sagas.SagaResult<object>.Compensate(
                                        result.State,
                                        combinedOutbound,
                                        combined,
                                        result.CanceledTimeouts,
                                        result.RecordedSteps);
                                }

                                Diagnostics.TalariaDiagnostics.SagaCompensationStarted.Add(
                                    1,
                                    new KeyValuePair<string, object?>("saga.type", stateType.Name),
                                    new KeyValuePair<string, object?>("compensation.mode", "sequential"));

                                if (seqCompState.Compensation.PendingSteps.Count > 0)
                                {
                                    pendingToStage = seqCompState.Compensation.PendingSteps[0];
                                }
                                else
                                {
                                    seqCompState.Compensation.Phase = SagaPhase.Compensated;
                                    Diagnostics.TalariaDiagnostics.SagaCompensationCompleted.Add(
                                        1,
                                        new KeyValuePair<string, object?>("saga.type", stateType.Name),
                                        new KeyValuePair<string, object?>("compensation.mode", "sequential"));

                                    if (route.Registration.OnCompensationCompleted is not null)
                                    {
                                        var completionContext = new Core.Sagas.SagaContext<object>(timeProvider) { CancellationToken = ownership.Token };
                                        var completionResult = await route.Registration.OnCompensationCompleted(result.State!, completionContext);
                                        var mergedDispatches = result.Dispatches.Concat(completionResult.Dispatches).ToArray();
                                        var mergedOutbound = mergedDispatches.Select(static d => d.Message).ToArray();
                                        var mergedCanceled = result.CanceledTimeouts.Concat(completionResult.CanceledTimeouts).Distinct().ToArray();
                                        result = completionResult.IsCompleted
                                            ? Core.Sagas.SagaResult<object>.Complete(mergedOutbound, mergedDispatches, mergedCanceled)
                                            : Core.Sagas.SagaResult<object>.Transition(completionResult.State!, mergedOutbound, mergedDispatches, mergedCanceled);
                                    }
                                }
                            }
                        }
                        else
                        {
                            Diagnostics.TalariaDiagnostics.SagaCompensationStarted.Add(
                                1,
                                new KeyValuePair<string, object?>("saga.type", stateType.Name),
                                new KeyValuePair<string, object?>("compensation.mode", "batch"));
                            Diagnostics.TalariaDiagnostics.SagaCompensationCompleted.Add(
                                1,
                                new KeyValuePair<string, object?>("saga.type", stateType.Name),
                                new KeyValuePair<string, object?>("compensation.mode", "batch"));
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                // During shutdown the handler may observe OperationCanceledException (or any
                // exception while the loop token is already canceled). Do not DLQ in that
                // case; leave the message uncommitted so it redelivers after restart.
                ownership.ThrowIfLost();
                if (ct.IsCancellationRequested)
                {
                    _logger.LogDebug(
                        ex,
                        "Saga {Type} handler threw during shutdown; leaving message uncommitted for redelivery.",
                        stateType.Name);
                    return;
                }

                _logger.LogError(ex, "Saga {Type} threw an exception while handling message {MsgType}", stateType.Name, step.MessageType.Name);
                activity?.SetStatus(System.Diagnostics.ActivityStatusCode.Error, ex.Message);

                Diagnostics.TalariaDiagnostics.MessagesFailed.Add(1, new KeyValuePair<string, object?>("messaging.destination.name", step.TopicName));

                var outcome = await retryCoordinator.TryCoordinateSagaRetryAsync(
                    step.TopicName, step.MessageType, _pipeline, consumer, env, ex, gate.Lock, ct);

                if (outcome == RetryCoordinator.RetryOutcome.NotRetryable)
                {
                    Diagnostics.TalariaDiagnostics.DlqRouted.Add(1, new KeyValuePair<string, object?>("messaging.destination.name", step.TopicName));
                    await _pipeline.FailAsync(gate.Lock, consumer, env, ex, null, ct);
                }

                return;
            }

            ownership.ThrowIfLost();
            if (result.IsDeferred)
            {
                try
                {
                    activity?.SetTag("saga.status", "deferred");
                    await HandleDeferralAsync(env, payload, route.Registration, step, correlationId, ct);

                    // Commit the original envelope BEFORE releasing the idempotency lock.
                    // The deferred copy carries a deterministic MessageId ({original}:defer:{attempt}),
                    // so any duplicate copies produced by a redelivery are suppressed by the
                    // idempotency gate. If commit fails, release the lock best-effort so the
                    // original can redeliver promptly rather than waiting for the lock TTL to expire.
                    try
                    {
                        await consumer.CommitAsync(env, ct);
                    }
                    catch (Exception commitEx)
                    {
                        _logger.LogError(commitEx, "Failed to commit original envelope after deferring saga message {MessageId}; it remains uncommitted for redelivery.", env.Headers.MessageId);
                        await ReleaseLockBestEffortAsync(gate.Lock, ct);
                        throw;
                    }

                    await ReleaseLockBestEffortAsync(gate.Lock, ct);

                    Diagnostics.TalariaDiagnostics.MessagesDeferred.Add(1, new KeyValuePair<string, object?>("saga.type", stateType.Name));
                }
                catch (DeferralRejectedException ex)
                {
                    Diagnostics.TalariaDiagnostics.MessagesFailed.Add(1, new KeyValuePair<string, object?>("messaging.destination.name", step.TopicName));
                    Diagnostics.TalariaDiagnostics.DlqRouted.Add(1, new KeyValuePair<string, object?>("messaging.destination.name", step.TopicName));

                    await _pipeline.FailAsync(gate.Lock, consumer, env, ex, null, ct);
                }
                return;
            }

            foreach (var dispatch in result.Dispatches)
            {
                var outbound = dispatch.Message;
                if (!route.Registration.DispatchTopics.ContainsKey(outbound.GetType()))
                {
                    var outboundType = outbound.GetType();
                    var ex = new InvalidOperationException(
                        $"Saga '{stateType.Name}' dispatched message type '{outboundType.Name}' with no DispatchTo mapping. " +
                        $"Declare the route with saga.DispatchTo<{outboundType.Name}>(\"topic\").");

                    _logger.LogError(ex, "Saga {Type} dispatched an unmapped message type.", stateType.Name);
                    activity?.SetStatus(System.Diagnostics.ActivityStatusCode.Error, ex.Message);
                    Diagnostics.TalariaDiagnostics.MessagesFailed.Add(1, new KeyValuePair<string, object?>("messaging.destination.name", step.TopicName));
                    Diagnostics.TalariaDiagnostics.DlqRouted.Add(1, new KeyValuePair<string, object?>("messaging.destination.name", step.TopicName));

                    await _pipeline.FailAsync(gate.Lock, consumer, env, ex, "unmapped_dispatch", ct);
                    return;
                }
            }

            if (_outboxStore is not null || (result.Dispatches.Count == 0 && pendingToStage is null))
            {
                var now = (timeProvider ?? TimeProvider.System).GetUtcNow();
                var staged = new List<OutboxMessage>(result.Dispatches.Count + (pendingToStage is not null ? 1 : 0));

                if (pendingToStage is not null)
                {
                    var compHeaders = new MessageHeaders { MessageId = Guid.NewGuid().ToString("N") };
                    if (System.Diagnostics.Activity.Current != null)
                    {
                        compHeaders.TraceParent = System.Diagnostics.Activity.Current.Id;
                        compHeaders.TraceState = System.Diagnostics.Activity.Current.TraceStateString;
                    }

                    compHeaders["talaria.saga.compensation"] = "true";
                    compHeaders["talaria.saga.compensation_saga"] = MessageNames.Contract(stateType);
                    compHeaders["talaria.intent"] = "command";
                    if (!string.IsNullOrWhiteSpace(pendingToStage.StepName))
                    {
                        compHeaders["talaria.saga.compensation_step"] = pendingToStage.StepName;
                    }
                    if (!string.IsNullOrWhiteSpace(pendingToStage.OperationId))
                    {
                        compHeaders["talaria.saga.compensation_operation_id"] = pendingToStage.OperationId;
                    }
                    compHeaders["talaria.saga.compensation_generation"] = pendingToStage.Generation.ToString(System.Globalization.CultureInfo.InvariantCulture);
                    compHeaders.CorrelationId = correlationId;

                    Diagnostics.TalariaDiagnostics.CompensationDispatched.Add(
                        1,
                        new KeyValuePair<string, object?>("saga.type", stateType.Name),
                        new KeyValuePair<string, object?>("messaging.destination.name", pendingToStage.DestinationTopic));

                    staged.Add(new OutboxMessage(
                        Guid.NewGuid(),
                        pendingToStage.DestinationTopic,
                        pendingToStage.CommandType,
                        pendingToStage.PayloadJson,
                        compHeaders,
                        now,
                        env.PartitionKey,
                        ScheduledAt: null));
                }

                foreach (var dispatch in result.Dispatches)
                {
                    var outbound = dispatch.Message;
                    var outboundType = outbound.GetType();
                    var outboundTopic = route.Registration.DispatchTopics[outboundType];

                    var headers = new MessageHeaders { MessageId = Guid.NewGuid().ToString("N") };
                    if (System.Diagnostics.Activity.Current != null)
                    {
                        headers.TraceParent = System.Diagnostics.Activity.Current.Id;
                        headers.TraceState = System.Diagnostics.Activity.Current.TraceStateString;
                    }

                    if (dispatch.IsTimeout)
                    {
                        headers["talaria.saga.timeout"] = "true";
                        headers["talaria.saga.timeout_version"] = (snapshot.Version + 1).ToString(System.Globalization.CultureInfo.InvariantCulture);
                        headers["talaria.saga.timeout_application"] = _options.ApplicationName;
                        headers["talaria.saga.timeout_saga"] = MessageNames.Contract(stateType);
                        headers["talaria.retry.endpoint"] = EndpointName(route.Registration, outboundTopic);
                        if (string.IsNullOrWhiteSpace(headers.CorrelationId))
                        {
                            headers.CorrelationId = correlationId;
                        }
                    }

                    if (dispatch.IsCompensation)
                    {
                        headers["talaria.saga.compensation"] = "true";
                        headers["talaria.saga.compensation_saga"] = MessageNames.Contract(stateType);
                        headers["talaria.intent"] = "command";
                        if (!string.IsNullOrWhiteSpace(dispatch.CompensationStepName))
                        {
                            headers["talaria.saga.compensation_step"] = dispatch.CompensationStepName;
                        }
                        if (!string.IsNullOrWhiteSpace(dispatch.CompensationOperationId))
                        {
                            headers["talaria.saga.compensation_operation_id"] = dispatch.CompensationOperationId;
                        }
                        if (dispatch.CompensationGeneration.HasValue)
                        {
                            headers["talaria.saga.compensation_generation"] = dispatch.CompensationGeneration.Value.ToString(System.Globalization.CultureInfo.InvariantCulture);
                        }
                        if (string.IsNullOrWhiteSpace(headers.CorrelationId))
                        {
                            headers.CorrelationId = correlationId;
                        }

                        Diagnostics.TalariaDiagnostics.CompensationDispatched.Add(
                            1,
                            new KeyValuePair<string, object?>("saga.type", stateType.Name),
                            new KeyValuePair<string, object?>("messaging.destination.name", outboundTopic));
                    }

                    if (dispatch.ScheduledAt.HasValue)
                    {
                        headers["talaria.scheduled_due_at"] = dispatch.ScheduledAt.Value.ToString("O");
                        Diagnostics.TalariaDiagnostics.MessagesScheduled.Add(
                            1,
                            new KeyValuePair<string, object?>("messaging.destination.name", outboundTopic));
                    }

                    staged.Add(new OutboxMessage(
                        Guid.NewGuid(),
                        outboundTopic,
                        outboundType.AssemblyQualifiedName ?? outboundType.FullName!,
                        JsonSerializer.Serialize(outbound, outboundType),
                        headers,
                        now,
                        env.PartitionKey,
                        dispatch.ScheduledAt));
                }

                var additionalReceipts = Array.Empty<string>();
                if (result.CanceledTimeouts.Count > 0 && snapshot.Version > 0)
                {
                    var receipts = new List<string>();
                    foreach (var canceledType in result.CanceledTimeouts.Distinct())
                    {
                        var contract = MessageNames.Contract(canceledType);
                        for (long v = 1; v <= snapshot.Version; v++)
                        {
                            receipts.Add($"timeout-canceled:{contract}:v{v}");
                        }
                    }

                    additionalReceipts = receipts.ToArray();
                }

                ownership.ThrowIfLost();
                var committed = await stateStore.TryTransitionAsync(
                    scope.ServiceProvider,
                    correlationId,
                    messageId,
                    snapshot.Version,
                    result.IsCompleted ? null : result.State!,
                    staged,
                    additionalReceipts,
                    ct);
                if (committed == SagaCommitStatus.Conflict)
                {
                    await ReleaseLockBestEffortAsync(gate.Lock, ct);
                    throw new InvalidOperationException("Saga version changed during processing; replay from fresh state.");
                }
                activity?.SetTag("saga.status", result.IsCompleted ? "completed" : "transitioned");


                try
                {
                    await _pipeline.CompleteAsync(gate.Lock, consumer, env, ct);

                    Diagnostics.TalariaDiagnostics.MessagesConsumed.Add(1, new KeyValuePair<string, object?>("messaging.destination.name", step.TopicName));
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to complete saga transition; the message remains uncommitted for redelivery.");

                    await ReleaseLockBestEffortAsync(gate.Lock, ct);

                    activity?.SetStatus(System.Diagnostics.ActivityStatusCode.Error, ex.Message);
                    Diagnostics.TalariaDiagnostics.MessagesFailed.Add(1, new KeyValuePair<string, object?>("messaging.destination.name", step.TopicName));
                    throw;
                }
            }
            else
            {
                throw new InvalidOperationException("Saga dispatch requires an outbox in the same persistence store.");
            }
        }
        finally
        {
            sw.Stop();
            Diagnostics.TalariaDiagnostics.ProcessingDuration.Record(sw.Elapsed.TotalMilliseconds, new KeyValuePair<string, object?>("messaging.destination.name", step.TopicName));
        }
    }

    private sealed class DeferralRejectedException(string message) : InvalidOperationException(message);

    private async Task HandleDeferralAsync(
        MessageEnvelope<JsonElement> env,
        object payload,
        SagaRegistration registration,
        SagaStepRegistration step,
        string correlationId,
        CancellationToken ct)
    {
        if (string.IsNullOrEmpty(env.SourceTopic))
        {
            env.Headers.DlqReason = "missing_source_topic";
            throw new DeferralRejectedException($"Cannot defer saga message of type {step.MessageType.Name}: the envelope has no source topic.");
        }

        int attempt = 1;
        if (env.Headers.TryGetValue(MessageHeaders.DeferralAttemptKey, out var strVal) && int.TryParse(strVal, out var parsed))
        {
            attempt = parsed + 1;
        }

        if (attempt > _options.MaxDeferralAttempts)
        {
            env.Headers.DlqReason = "max_deferrals_exceeded";
            _logger.LogWarning("Message exceeded max deferral attempts ({Max}). Routing to DLQ.", _options.MaxDeferralAttempts);
            throw new DeferralRejectedException($"Max deferral attempts ({_options.MaxDeferralAttempts}) exceeded for Saga message of type {step.MessageType.Name}");
        }

        if (_deferralStore is null)
        {
            env.Headers.DlqReason = "deferral_unavailable";
            throw new DeferralRejectedException(
                $"Cannot defer saga message of type {step.MessageType.Name}: no IDeferralStore is registered. " +
                "Register one via UseRedisDeferralStore() or UseInMemoryDeferralStore().");
        }

        var headers = new MessageHeaders(env.Headers)
        {
            [MessageHeaders.DeferralAttemptKey] = attempt.ToString()
        };

        var rootMessageId = headers.MessageId ?? Guid.NewGuid().ToString("N");
        var targetEndpoint = headers.TryGetValue("talaria.retry.endpoint", out var target)
            ? target
            : EndpointName(registration, step.TopicName);
        headers["talaria.retry.endpoint"] = targetEndpoint;
        var deferredIdentity = RetryCoordinator.CreateDeferredIdentity(rootMessageId, targetEndpoint, attempt, "defer");
        headers.MessageId = deferredIdentity.MessageId;

        headers.HopCount = headers.HopCount + 1;

        var deferred = new DeferredMessage(
            deferredIdentity.EntryId,
            env.SourceTopic!,
            step.MessageType.AssemblyQualifiedName ?? step.MessageType.FullName!,
            JsonSerializer.Serialize(payload, step.MessageType),
            headers,
            correlationId,
            attempt,
            DateTimeOffset.UtcNow + TimeSpan.FromMilliseconds(_options.DeferralBackoff.TotalMilliseconds * attempt),
            env.PartitionKey);

        await _deferralStore.EnqueueAsync(deferred, ct);
    }

    private static IStateStoreAccessor CreateStateStoreAccessor(Type stateType)
    {
        var method = typeof(SagaConsumerEngine)
            .GetMethod(nameof(CreateStateStoreAccessorTyped), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!
            .MakeGenericMethod(stateType);

        return (IStateStoreAccessor)method.Invoke(null, null)!;
    }

    private static IStateStoreAccessor CreateStateStoreAccessorTyped<TState>() where TState : class, new()
        => new StateStoreAccessor<TState>();

    private async Task ReleaseLockBestEffortAsync(IdempotencyLock? lck, CancellationToken ct)
    {
        if (lck is null)
        {
            return;
        }

        try
        {
            await _pipeline.ReleaseAsync(lck, ct);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to release idempotency lock {MessageId}; it expires via TTL.", lck.MessageId);
        }
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
