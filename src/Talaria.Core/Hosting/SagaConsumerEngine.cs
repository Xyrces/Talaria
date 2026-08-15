// SPDX-License-Identifier: Apache-2.0

using System.Collections.Concurrent;
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
    private readonly ProducerCache _producerCache;

    private readonly ConcurrentDictionary<Type, SessionDispatcher> _sessionDispatchers = new();
    private readonly IReadOnlyDictionary<Type, string> _dispatchRoutes;

    private delegate Task SessionDispatcher(
        ITransactionalSession session, string topic, object message, MessageHeaders? headers, CancellationToken ct);

    private sealed record StepRoute(
        SagaRegistration Registration,
        SagaStepRegistration Step,
        IStateStoreAccessor StateStore);

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
        _producerCache = new ProducerCache(transport);
        _dispatchRoutes = BuildAndValidateDispatchRoutes();
    }

    public IReadOnlyDictionary<Type, string> DispatchRoutes => _dispatchRoutes;

    public ProducerCache ProducerCache => _producerCache;

    public async Task RunAsync(CancellationToken ct)
    {
        var retryCoordinator = new RetryCoordinator(_deferralStore, _options, _logger);

        var stepsByTopic = _registrations
            .SelectMany(r => r.Steps.Select(s => new StepRoute(r, s, CreateStateStoreAccessor(r.StateType))))
            .GroupBy(x => x.Step.TopicName)
            .ToDictionary(g => g.Key, g => (IReadOnlyList<StepRoute>)g.ToList());

        if (_deferralStore is null && stepsByTopic.Count > 0)
        {
            _logger.LogWarning(
                "No IDeferralStore is registered. Out-of-order saga messages, handler-initiated " +
                "deferrals, and delayed retries will be routed to the DLQ instead of being deferred. " +
                "Register one via UseRedisDeferralStore() or UseInMemoryDeferralStore().");
        }

        if (_outboxStore is null && DispatchRoutes.Count > 0)
        {
            _logger.LogWarning(
                "No IOutboxStore is registered. Saga dispatch falls back to direct transactional " +
                "produce: the state save and the message publish are not atomic, so a crash " +
                "between them can lose outbound messages. The outbox is registered automatically " +
                "by UseRedisStateStore() and UseInMemoryStateStore().");
        }

        foreach (var type in DispatchRoutes.Keys)
        {
            GetSessionDispatcher(type);
        }

        foreach (var route in stepsByTopic.Values.SelectMany(x => x))
        {
            await _producerCache.GetOrCreateAsync(route.Step.TopicName, route.Step.MessageType, ct);
        }

        var tasks = stepsByTopic.Select(kvp =>
            ConsumerSupervision.RunSupervisedAsync(
                $"saga:{kvp.Key}",
                ct => ConsumeTopicLoopAsync(kvp.Key, kvp.Value, retryCoordinator, ct),
                _logger,
                ct)).ToList();

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
        RetryCoordinator retryCoordinator,
        CancellationToken ct)
    {
        await using var consumer = await _transport.CreateConsumerAsync<JsonElement>(
            topic,
            new ConsumerOptions { ConsumerGroup = _options.ApplicationName },
            ct);

        await foreach (var env in consumer.ConsumeAsync(ct))
        {
            var route = ResolveStep(env, routes);
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

    private static StepRoute? ResolveStep(
        MessageEnvelope<JsonElement> env,
        IReadOnlyList<StepRoute> routes)
    {
        if (routes.Count == 1)
        {
            return routes[0];
        }

        var typeName = env.Headers.TryGetValue(MessageHeaders.MessageTypeKey, out var v) ? v : null;
        if (typeName is null)
        {
            return null;
        }

        return routes.FirstOrDefault(r =>
            string.Equals(r.Step.MessageType.FullName, typeName, StringComparison.Ordinal) ||
            string.Equals(r.Step.MessageType.Name, typeName, StringComparison.Ordinal));
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
            using var scope = _serviceProvider.CreateScope();

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
                _logger.LogWarning("No correlation ID found for saga message {Type} on topic {Topic}", step.MessageType.Name, env.SourceTopic);

                env.Headers.DlqReason = "missing_correlation_id";
                activity?.SetStatus(System.Diagnostics.ActivityStatusCode.Error, "Missing Correlation Id");
                Diagnostics.TalariaDiagnostics.MessagesFailed.Add(1, new KeyValuePair<string, object?>("messaging.destination.name", step.TopicName));
                Diagnostics.TalariaDiagnostics.DlqRouted.Add(1, new KeyValuePair<string, object?>("messaging.destination.name", step.TopicName));

                await consumer.NackAsync(env, ct);
                return;
            }

            activity?.SetTag("saga.correlation_id", correlationId);

            var gate = await _pipeline.AcquireAsync(env, $"{_options.ApplicationName}.{step.TopicName}", ct);
            if (gate.IsDuplicate)
            {
                _logger.LogDebug("Saga Message {MessageId} skipped. Idempotency lock claimed by another worker or already completed.", env.Headers.MessageId);
                await consumer.CommitAsync(env, ct);
                return;
            }

            var state = await stateStore.GetAsync(scope.ServiceProvider, correlationId, ct);

            if (state == null && !step.IsStarter)
            {
                _logger.LogInformation("Received non-starter message for Saga {SagaType} but state {Id} not found. Deferring...", stateType.Name, correlationId);
                try
                {
                    await HandleDeferralAsync(env, payload, step, correlationId, ct);

                    // Commit the original envelope BEFORE releasing the idempotency lock.
                    // The deferred copy carries a freshly minted MessageId, so it is NOT
                    // gated by the original lock. Committing first ensures the original
                    // cannot redeliver and re-run the handler concurrently with the deferred
                    // copy. If commit fails we leave the lock held: it expires via TTL and
                    // the transport redelivers the original, which is safe.
                    try
                    {
                        await consumer.CommitAsync(env, ct);
                    }
                    catch (Exception commitEx)
                    {
                        _logger.LogError(commitEx, "Failed to commit original envelope after deferring saga message {MessageId}; it remains uncommitted for redelivery.", env.Headers.MessageId);
                        return;
                    }

                    await ReleaseLockBestEffortAsync(gate.Lock, ct);

                    Diagnostics.TalariaDiagnostics.MessagesDeferred.Add(1, new KeyValuePair<string, object?>("saga.type", stateType.Name));
                }
                catch (InvalidOperationException ex)
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

            var context = new Core.Sagas.SagaContext<object>();
            Core.Sagas.SagaResult<object> result;
            try
            {
                result = await step.Handler(state ?? stateStore.NewState(), payload, context);
            }
            catch (Exception ex)
            {
                // During shutdown the handler may observe OperationCanceledException (or any
                // exception while the loop token is already canceled). Do not DLQ in that
                // case; leave the message uncommitted so it redelivers after restart.
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

            if (result.IsDeferred)
            {
                try
                {
                    activity?.SetTag("saga.status", "deferred");
                    await HandleDeferralAsync(env, payload, step, correlationId, ct);

                    // Commit the original envelope BEFORE releasing the idempotency lock.
                    // The deferred copy carries a freshly minted MessageId, so it is NOT
                    // gated by the original lock. Committing first ensures the original
                    // cannot redeliver and re-run the handler concurrently with the deferred
                    // copy. If commit fails we leave the lock held: it expires via TTL and
                    // the transport redelivers the original, which is safe.
                    try
                    {
                        await consumer.CommitAsync(env, ct);
                    }
                    catch (Exception commitEx)
                    {
                        _logger.LogError(commitEx, "Failed to commit original envelope after deferring saga message {MessageId}; it remains uncommitted for redelivery.", env.Headers.MessageId);
                        return;
                    }

                    await ReleaseLockBestEffortAsync(gate.Lock, ct);

                    Diagnostics.TalariaDiagnostics.MessagesDeferred.Add(1, new KeyValuePair<string, object?>("saga.type", stateType.Name));
                }
                catch (InvalidOperationException ex)
                {
                    Diagnostics.TalariaDiagnostics.MessagesFailed.Add(1, new KeyValuePair<string, object?>("messaging.destination.name", step.TopicName));
                    Diagnostics.TalariaDiagnostics.DlqRouted.Add(1, new KeyValuePair<string, object?>("messaging.destination.name", step.TopicName));

                    await _pipeline.FailAsync(gate.Lock, consumer, env, ex, null, ct);
                }
                return;
            }

            foreach (var outbound in result.OutboundMessages)
            {
                if (!DispatchRoutes.ContainsKey(outbound.GetType()))
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

            if (_outboxStore is not null)
            {
                var staged = new List<OutboxMessage>(result.OutboundMessages.Count);
                foreach (var outbound in result.OutboundMessages)
                {
                    var outboundType = outbound.GetType();
                    var outboundTopic = DispatchRoutes[outboundType];

                    var headers = new MessageHeaders { MessageId = Guid.NewGuid().ToString("N") };
                    if (System.Diagnostics.Activity.Current != null)
                    {
                        headers.TraceParent = System.Diagnostics.Activity.Current.Id;
                        headers.TraceState = System.Diagnostics.Activity.Current.TraceStateString;
                    }

                    staged.Add(new OutboxMessage(
                        Guid.NewGuid(),
                        outboundTopic,
                        outboundType.AssemblyQualifiedName ?? outboundType.FullName!,
                        JsonSerializer.Serialize(outbound, outboundType),
                        headers,
                        DateTimeOffset.UtcNow));
                }

                await stateStore.TransitionAsync(
                    scope.ServiceProvider,
                    correlationId,
                    result.IsCompleted ? null : result.State!,
                    staged,
                    ct);
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
                }
            }
            else
            {
                var offsetSource = env.SourceTopic is not null && env.Partition is int partition
                    ? new TransactionOffsetSource(env.SourceTopic, partition, env.Offset)
                    : null;

                await using var tx = await _transport.BeginTransactionAsync(_options.ApplicationName, offsetSource, ct);

                foreach (var outbound in result.OutboundMessages)
                {
                    var outboundType = outbound.GetType();
                    var outboundTopic = DispatchRoutes[outboundType];

                    var dispatcher = GetSessionDispatcher(outboundType);

                    var headers = new MessageHeaders();
                    if (System.Diagnostics.Activity.Current != null)
                    {
                        headers.TraceParent = System.Diagnostics.Activity.Current.Id;
                        headers.TraceState = System.Diagnostics.Activity.Current.TraceStateString;
                    }

                    await dispatcher(tx, outboundTopic, outbound, headers, ct);
                }

                if (result.IsCompleted)
                {
                    await stateStore.DeleteAsync(scope.ServiceProvider, correlationId, ct);
                    activity?.SetTag("saga.status", "completed");
                }
                else
                {
                    await stateStore.SaveAsync(scope.ServiceProvider, correlationId, result.State!, ct);
                    activity?.SetTag("saga.status", "transitioned");
                }

                try
                {
                    await tx.CommitAsync(ct);
                    await _pipeline.CompleteAsync(gate.Lock, consumer, env, ct);

                    Diagnostics.TalariaDiagnostics.MessagesConsumed.Add(1, new KeyValuePair<string, object?>("messaging.destination.name", step.TopicName));
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to commit saga transition; the message remains uncommitted for redelivery.");

                    await ReleaseLockBestEffortAsync(gate.Lock, ct);

                    activity?.SetStatus(System.Diagnostics.ActivityStatusCode.Error, ex.Message);
                    Diagnostics.TalariaDiagnostics.MessagesFailed.Add(1, new KeyValuePair<string, object?>("messaging.destination.name", step.TopicName));
                }
            }
        }
        finally
        {
            sw.Stop();
            Diagnostics.TalariaDiagnostics.ProcessingDuration.Record(sw.Elapsed.TotalMilliseconds, new KeyValuePair<string, object?>("messaging.destination.name", step.TopicName));
        }
    }

    private async Task HandleDeferralAsync(
        MessageEnvelope<JsonElement> env,
        object payload,
        SagaStepRegistration step,
        string correlationId,
        CancellationToken ct)
    {
        if (string.IsNullOrEmpty(env.SourceTopic))
        {
            env.Headers.DlqReason = "missing_source_topic";
            throw new InvalidOperationException($"Cannot defer saga message of type {step.MessageType.Name}: the envelope has no source topic.");
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
            throw new InvalidOperationException($"Max deferral attempts ({_options.MaxDeferralAttempts}) exceeded for Saga message of type {step.MessageType.Name}");
        }

        if (_deferralStore is null)
        {
            env.Headers.DlqReason = "deferral_unavailable";
            throw new InvalidOperationException(
                $"Cannot defer saga message of type {step.MessageType.Name}: no IDeferralStore is registered. " +
                "Register one via UseRedisDeferralStore() or UseInMemoryDeferralStore().");
        }

        var headers = new MessageHeaders(env.Headers)
        {
            [MessageHeaders.DeferralAttemptKey] = attempt.ToString()
        };

        var originalMessageId = headers.MessageId;
        if (!string.IsNullOrEmpty(originalMessageId))
        {
            headers.MessageId = $"{originalMessageId}:defer:{attempt}";
        }

        headers.HopCount = headers.HopCount + 1;

        var deferred = new DeferredMessage(
            Guid.NewGuid(),
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

    private SessionDispatcher GetSessionDispatcher(Type messageType)
    {
        if (_sessionDispatchers.TryGetValue(messageType, out var existing))
        {
            return existing;
        }

        var method = typeof(SagaConsumerEngine)
            .GetMethod(nameof(CreateSessionDispatcher), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!
            .MakeGenericMethod(messageType);

        return _sessionDispatchers.GetOrAdd(messageType, (SessionDispatcher)method.Invoke(null, null)!);
    }

    private static SessionDispatcher CreateSessionDispatcher<T>() where T : class
        => async (session, topic, message, headers, token) =>
        {
            var producer = await session.GetProducerAsync<T>(topic, token);
            await producer.ProduceAsync((T)message, headers, null, token);
        };

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

    public async ValueTask DisposeAsync()
    {
        await _producerCache.DisposeAsync();
    }
}
