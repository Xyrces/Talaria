// SPDX-License-Identifier: Apache-2.0

using System.Collections.Concurrent;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Talaria.Core.Abstractions;
using Talaria.Core.Registration;
using Talaria.Core.Sagas;

namespace Talaria.Core.Hosting;

/// <summary>
/// Hosted service that listens on all configured saga topics and runs the deferral
/// sweeper whenever an <see cref="IDeferralStore"/> is registered. One supervised consumer
/// per topic; messages are fanned out to the correct saga step via the message-type header.
/// Dispatch topics are explicit (DispatchTo) and producers are created once at startup —
/// no reflection in the per-message hot path. Also coordinates opt-in delayed retries for
/// saga step handler exceptions via <see cref="RetryCoordinator"/>.
/// </summary>
public sealed class SagaHostedService : BackgroundService
{
    private readonly SagaRegistry _registry;
    private readonly IServiceProvider _serviceProvider;
    private readonly TalariaOptions _options;
    private readonly ILogger<SagaHostedService> _logger;

    // Durable deferral store; resolved at startup. Null means deferral is unavailable.
    private IDeferralStore? _deferralStore;

    // Transactional outbox read side; resolved at startup. Null means saga dispatch
    // falls back to direct transactional produce (state save and publish not atomic).
    private IOutboxStore? _outboxStore;

    // Cached producers keyed by (topic, message type) — created once, reused per dispatch.
    private readonly ConcurrentDictionary<(string Topic, Type MessageType), ProducerInvoker> _producers = new();

    // Cached session dispatch delegates keyed by message type — built once at startup,
    // used to produce through the per-message transactional session.
    private readonly ConcurrentDictionary<Type, SessionDispatcher> _sessionDispatchers = new();

    private delegate Task SessionDispatcher(
        ITransactionalSession session, string topic, object message, MessageHeaders? headers, CancellationToken ct);

    // Merged dispatch routes across all sagas: message CLR type → topic.
    private IReadOnlyDictionary<Type, string> _dispatchRoutes = new Dictionary<Type, string>();

    private sealed record ProducerInvoker(
        Func<object, MessageHeaders?, string?, CancellationToken, Task> Produce,
        IAsyncDisposable Producer);

    private sealed record StepRoute(
        SagaRegistration Registration,
        SagaStepRegistration Step,
        IStateStoreAccessor StateStore);

    /// <summary>Non-generic facade over <see cref="IStateStore{TState}"/> resolved per DI scope.</summary>
    private interface IStateStoreAccessor
    {
        Task<object?> GetAsync(IServiceProvider scope, string correlationId, CancellationToken ct);
        Task SaveAsync(IServiceProvider scope, string correlationId, object state, CancellationToken ct);
        Task DeleteAsync(IServiceProvider scope, string correlationId, CancellationToken ct);
        Task TransitionAsync(IServiceProvider scope, string correlationId, object? newState, IReadOnlyList<OutboxMessage> outbox, CancellationToken ct);
        object NewState();
    }

    private sealed class StateStoreAccessor<TState> : IStateStoreAccessor where TState : class, new()
    {
        public async Task<object?> GetAsync(IServiceProvider scope, string correlationId, CancellationToken ct)
            => await scope.GetRequiredService<IStateStore<TState>>().GetAsync(correlationId, ct);

        public async Task SaveAsync(IServiceProvider scope, string correlationId, object state, CancellationToken ct)
            => await scope.GetRequiredService<IStateStore<TState>>().SaveAsync(correlationId, (TState)state, ct);

        public async Task DeleteAsync(IServiceProvider scope, string correlationId, CancellationToken ct)
            => await scope.GetRequiredService<IStateStore<TState>>().DeleteAsync(correlationId, ct);

        public async Task TransitionAsync(IServiceProvider scope, string correlationId, object? newState, IReadOnlyList<OutboxMessage> outbox, CancellationToken ct)
            => await scope.GetRequiredService<IStateStore<TState>>().TransitionAsync(correlationId, (TState?)newState, outbox, ct);

        public object NewState() => new TState();
    }

    public SagaHostedService(
        SagaRegistry registry,
        IServiceProvider serviceProvider,
        IOptions<TalariaOptions> options,
        ILogger<SagaHostedService> logger)
    {
        _registry = registry;
        _serviceProvider = serviceProvider;
        _options = options.Value;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var transport = _serviceProvider.GetRequiredService<ITransport>();
        var pipeline = new MessageProcessingPipeline(
            _serviceProvider.GetService<IIdempotencyStore>(),
            _options,
            _logger);

        // Seal and snapshot the registry before consumers spin up — late registrations throw.
        _registry.Seal();

        // Group every step of every saga by topic — one consumer per topic.
        var stepsByTopic = _registry.Registrations
            .SelectMany(r => r.Steps.Select(s => new StepRoute(r, s, CreateStateStoreAccessor(r.StateType))))
            .GroupBy(x => x.Step.TopicName)
            .ToDictionary(g => g.Key, g => (IReadOnlyList<StepRoute>)g.ToList());

        _dispatchRoutes = BuildAndValidateDispatchRoutes(stepsByTopic.Keys);

        _deferralStore = _serviceProvider.GetService<IDeferralStore>();
        if (_deferralStore is null && stepsByTopic.Count > 0)
        {
            _logger.LogWarning(
                "No IDeferralStore is registered. Out-of-order saga messages, handler-initiated " +
                "deferrals, and delayed retries will be routed to the DLQ instead of being deferred. " +
                "Register one via UseRedisDeferralStore() or UseInMemoryDeferralStore().");
        }

        var retryCoordinator = new RetryCoordinator(_deferralStore, _options, _logger);

        _outboxStore = _serviceProvider.GetService<IOutboxStore>();
        if (_outboxStore is null && _dispatchRoutes.Count > 0)
        {
            _logger.LogWarning(
                "No IOutboxStore is registered. Saga dispatch falls back to direct transactional " +
                "produce: the state save and the message publish are not atomic, so a crash " +
                "between them can lose outbound messages. The outbox is registered automatically " +
                "by UseRedisStateStore() and UseInMemoryStateStore().");
        }

        // Pre-create producers for all step topics (used for deferral republishing) and
        // pre-build the session dispatchers for all declared dispatch routes (one-time
        // generic dispatch at startup — never in the per-message hot path).
        foreach (var type in _dispatchRoutes.Keys)
        {
            GetSessionDispatcher(type);
        }
        foreach (var route in stepsByTopic.Values.SelectMany(x => x))
        {
            await GetOrCreateProducerAsync(transport, route.Step.TopicName, route.Step.MessageType, stoppingToken);
        }

        var tasks = stepsByTopic.Select(kvp =>
            ConsumerSupervision.RunSupervisedAsync(
                $"saga:{kvp.Key}",
                ct => ConsumeTopicLoopAsync(kvp.Key, kvp.Value, transport, pipeline, retryCoordinator, ct),
                _logger,
                stoppingToken)).ToList();

        // NOTE: the deferral sweeper lives in SagaHostedService. Hosts that use delayed
        // retries or saga deferrals must not omit SagaHostedService — AddTalaria registers
        // both hosted services by default.
        if (_deferralStore != null)
        {
            tasks.Add(ConsumerSupervision.RunSupervisedAsync(
                "deferral-sweeper",
                ct => SweepDeferralsLoopAsync(transport, ct),
                _logger,
                stoppingToken));
        }

        if (_outboxStore != null && _dispatchRoutes.Count > 0)
        {
            tasks.Add(ConsumerSupervision.RunSupervisedAsync(
                "outbox-relay",
                ct => OutboxRelayLoopAsync(transport, ct),
                _logger,
                stoppingToken));
        }

        _logger.LogInformation("Talaria Sagas: started {Count} topic consumers.", tasks.Count);

        await Task.WhenAll(tasks);
    }

    /// <summary>
    /// Merges all sagas' DispatchTo mappings and warns about declared targets that have no
    /// registered consumer in this host (they may still be consumed by other services).
    /// </summary>
    private IReadOnlyDictionary<Type, string> BuildAndValidateDispatchRoutes(ICollection<string> sagaTopics)
    {
        var routes = new Dictionary<Type, string>();
        foreach (var reg in _registry.Registrations)
        {
            foreach (var (type, topic) in reg.DispatchTopics)
            {
                routes[type] = topic;
            }
        }

        var consumedTopics = new HashSet<string>(sagaTopics);
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
        ITransport transport,
        MessageProcessingPipeline pipeline,
        RetryCoordinator retryCoordinator,
        CancellationToken ct)
    {
        await using var consumer = await transport.CreateConsumerAsync<JsonElement>(
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

            await ProcessStepMessageAsync(env, route, transport, pipeline, retryCoordinator, consumer, ct);
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
        ITransport transport,
        MessageProcessingPipeline pipeline,
        RetryCoordinator retryCoordinator,
        IConsumer<JsonElement> consumer,
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

            // 1. Deserialize the payload to the step's message type.
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

            // 2. Resolve correlation ID.
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

            // 3. Idempotency gate, keyed per saga topic.
            var gate = await pipeline.AcquireAsync(env, $"{_options.ApplicationName}.{step.TopicName}", ct);
            if (gate.IsDuplicate)
            {
                _logger.LogDebug("Saga Message {MessageId} skipped. Idempotency lock claimed by another worker or already completed.", env.Headers.MessageId);
                // We immediately commit the message to suppress further polling!
                await consumer.CommitAsync(env, ct);
                return;
            }

            // 4. Load state.
            var state = await stateStore.GetAsync(scope.ServiceProvider, correlationId, ct);

            // 5a. Defer out-of-order message (no state yet for a non-starter step).
            if (state == null && !step.IsStarter)
            {
                _logger.LogInformation("Received non-starter message for Saga {SagaType} but state {Id} not found. Deferring...", stateType.Name, correlationId);
                try
                {
                    await HandleDeferralAsync(env, payload, step, correlationId, ct);

                    // The deferred copy is durably scheduled — safe to commit the original and release the
                    // idempotency lock so the deferred copy (new MessageId) is not skipped as a duplicate.
                    await ReleaseLockBestEffortAsync(pipeline, gate.Lock, ct);
                    await consumer.CommitAsync(env, ct);

                    Diagnostics.TalariaDiagnostics.MessagesDeferred.Add(1, new KeyValuePair<string, object?>("saga.type", stateType.Name));
                }
                catch (InvalidOperationException ex)
                {
                    Diagnostics.TalariaDiagnostics.MessagesFailed.Add(1, new KeyValuePair<string, object?>("messaging.destination.name", step.TopicName));
                    Diagnostics.TalariaDiagnostics.DlqRouted.Add(1, new KeyValuePair<string, object?>("messaging.destination.name", step.TopicName));

                    // The DLQ reason header was set by HandleDeferralAsync before throwing.
                    await pipeline.FailAsync(gate.Lock, consumer, env, ex, null, ct);
                }
                return;
            }

            // 5b. Starter replay: state already exists AND this is the original attempt
            // (RetryAttempt == 0) — idempotent replay, skip and commit. A retry copy
            // (RetryAttempt > 0) means the original starter FAILED, so it must run; otherwise
            // the saga stalls because the failed starter never transitions state.
            if (state != null && step.IsStarter && env.Headers.RetryAttempt == 0)
            {
                _logger.LogWarning(
                    "Saga {SagaType} starter message {MessageId} on topic {Topic} received, but state for correlation {CorrelationId} already exists. Skipping as an idempotent replay.",
                    stateType.Name, env.Headers.MessageId, step.TopicName, correlationId);

                await pipeline.CompleteAsync(gate.Lock, consumer, env, ct);
                return;
            }

            // 6. Run the saga step (pure function).
            var context = new Core.Sagas.SagaContext<object>();
            Core.Sagas.SagaResult<object> result;
            try
            {
                result = await step.Handler(state ?? stateStore.NewState(), payload, context);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Saga {Type} threw an exception while handling message {MsgType}", stateType.Name, step.MessageType.Name);
                activity?.SetStatus(System.Diagnostics.ActivityStatusCode.Error, ex.Message);

                Diagnostics.TalariaDiagnostics.MessagesFailed.Add(1, new KeyValuePair<string, object?>("messaging.destination.name", step.TopicName));

                var outcome = await retryCoordinator.TryCoordinateSagaRetryAsync(
                    step.TopicName, step.MessageType, pipeline, consumer, env, ex, gate.Lock, ct);

                if (outcome == RetryCoordinator.RetryOutcome.NotRetryable)
                {
                    Diagnostics.TalariaDiagnostics.DlqRouted.Add(1, new KeyValuePair<string, object?>("messaging.destination.name", step.TopicName));
                    await pipeline.FailAsync(gate.Lock, consumer, env, ex, null, ct);
                }

                return;
            }

            // 7. Handler-initiated deferral.
            if (result.IsDeferred)
            {
                try
                {
                    activity?.SetTag("saga.status", "deferred");
                    await HandleDeferralAsync(env, payload, step, correlationId, ct);

                    await ReleaseLockBestEffortAsync(pipeline, gate.Lock, ct);
                    await consumer.CommitAsync(env, ct);

                    Diagnostics.TalariaDiagnostics.MessagesDeferred.Add(1, new KeyValuePair<string, object?>("saga.type", stateType.Name));
                }
                catch (InvalidOperationException ex)
                {
                    Diagnostics.TalariaDiagnostics.MessagesFailed.Add(1, new KeyValuePair<string, object?>("messaging.destination.name", step.TopicName));
                    Diagnostics.TalariaDiagnostics.DlqRouted.Add(1, new KeyValuePair<string, object?>("messaging.destination.name", step.TopicName));

                    // The DLQ reason header was set by HandleDeferralAsync before throwing.
                    await pipeline.FailAsync(gate.Lock, consumer, env, ex, null, ct);
                }
                return;
            }

            // 8a. Validate dispatch routes BEFORE starting the transaction. An undeclared
            //     dispatch type is a saga configuration bug: dead-letter the message like a
            //     handler failure (releasing the idempotency lock) instead of letting the
            //     exception escape the loop — which would drop the message silently and leak
            //     the lock until TTL.
            foreach (var outbound in result.OutboundMessages)
            {
                if (!_dispatchRoutes.ContainsKey(outbound.GetType()))
                {
                    var outboundType = outbound.GetType();
                    var ex = new InvalidOperationException(
                        $"Saga '{stateType.Name}' dispatched message type '{outboundType.Name}' with no DispatchTo mapping. " +
                        $"Declare the route with saga.DispatchTo<{outboundType.Name}>(\"topic\").");

                    _logger.LogError(ex, "Saga {Type} dispatched an unmapped message type.", stateType.Name);
                    activity?.SetStatus(System.Diagnostics.ActivityStatusCode.Error, ex.Message);
                    Diagnostics.TalariaDiagnostics.MessagesFailed.Add(1, new KeyValuePair<string, object?>("messaging.destination.name", step.TopicName));
                    Diagnostics.TalariaDiagnostics.DlqRouted.Add(1, new KeyValuePair<string, object?>("messaging.destination.name", step.TopicName));

                    await pipeline.FailAsync(gate.Lock, consumer, env, ex, "unmapped_dispatch", ct);
                    return;
                }
            }

            if (_outboxStore is not null)
            {
                // 8b. Transactional outbox: stage outbound messages atomically with the
                //     state transition. Each staged message carries a freshly minted
                //     MessageId so the relay's at-least-once publication is deduplicated
                //     downstream by the idempotency gate.
                var staged = new List<OutboxMessage>(result.OutboundMessages.Count);
                foreach (var outbound in result.OutboundMessages)
                {
                    var outboundType = outbound.GetType();
                    var outboundTopic = _dispatchRoutes[outboundType];

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

                // 9. Atomic transition: state save/purge + outbox staging in one store
                //    operation. A crash after this point loses nothing — the relay
                //    publishes whatever was staged.
                await stateStore.TransitionAsync(
                    scope.ServiceProvider,
                    correlationId,
                    result.IsCompleted ? null : result.State!,
                    staged,
                    ct);
                activity?.SetTag("saga.status", result.IsCompleted ? "completed" : "transitioned");

                // 10. Mark idempotency and commit the offset. A crash before this commits
                //     means redelivery: the replay hits transitioned state, so step handlers
                //     must be idempotent (starters are safe by construction via the replay guard).
                try
                {
                    await pipeline.CompleteAsync(gate.Lock, consumer, env, ct);

                    Diagnostics.TalariaDiagnostics.MessagesConsumed.Add(1, new KeyValuePair<string, object?>("messaging.destination.name", step.TopicName));
                }
                catch (Exception ex)
                {
                    // Infrastructure failure — do NOT dead-letter a healthy message. The lock
                    // is released and the offset stays uncommitted so the transport redelivers.
                    _logger.LogError(ex, "Failed to complete saga transition; the message remains uncommitted for redelivery.");

                    await ReleaseLockBestEffortAsync(pipeline, gate.Lock, ct);

                    activity?.SetStatus(System.Diagnostics.ActivityStatusCode.Error, ex.Message);
                    Diagnostics.TalariaDiagnostics.MessagesFailed.Add(1, new KeyValuePair<string, object?>("messaging.destination.name", step.TopicName));
                }
            }
            else
            {
                // Legacy fallback (no IOutboxStore registered): direct transactional dispatch.
                //     The consumed message's offset joins the transaction when the transport
                //     supports it (Kafka exactly-once); InMemory buffers until commit.
                var offsetSource = env.SourceTopic is not null && env.Partition is int partition
                    ? new TransactionOffsetSource(env.SourceTopic, partition, env.Offset)
                    : null;

                await using var tx = await transport.BeginTransactionAsync(_options.ApplicationName, offsetSource, ct);

                foreach (var outbound in result.OutboundMessages)
                {
                    var outboundType = outbound.GetType();
                    var outboundTopic = _dispatchRoutes[outboundType];

                    var dispatcher = GetSessionDispatcher(outboundType);

                    // Propagate trace context
                    var headers = new MessageHeaders();
                    if (System.Diagnostics.Activity.Current != null)
                    {
                        headers.TraceParent = System.Diagnostics.Activity.Current.Id;
                        headers.TraceState = System.Diagnostics.Activity.Current.TraceStateString;
                    }

                    await dispatcher(tx, outboundTopic, outbound, headers, ct);
                }

                // 9. Save or purge state.
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

                // 10. Commit the transition, the offset, and the idempotency marker.
                try
                {
                    await tx.CommitAsync(ct);
                    await pipeline.CompleteAsync(gate.Lock, consumer, env, ct);

                    Diagnostics.TalariaDiagnostics.MessagesConsumed.Add(1, new KeyValuePair<string, object?>("messaging.destination.name", step.TopicName));
                }
                catch (Exception ex)
                {
                    // Infrastructure failure — do NOT dead-letter a healthy message. The lock is
                    // released, the session's disposal aborts the transaction, and the offset stays
                    // uncommitted so the transport redelivers the message. Note the state save may
                    // already have happened: replay then hits transitioned state, so step handlers
                    // must be idempotent (starters are safe by construction via the replay guard).
                    _logger.LogError(ex, "Failed to commit saga transition; the message remains uncommitted for redelivery.");

                    await ReleaseLockBestEffortAsync(pipeline, gate.Lock, ct);

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

    /// <summary>
    /// Durably schedules a deferred copy of the message in the <see cref="IDeferralStore"/>.
    /// On return the caller commits the original delivery and releases the idempotency lock;
    /// the sweeper republishes the deferred copy when it falls due. Sets the DLQ reason header
    /// and throws <see cref="InvalidOperationException"/> when deferral is impossible
    /// (no store registered, missing source topic, or max attempts exceeded).
    /// </summary>
    /// <remarks>
    /// Interaction with delayed retries: a retry copy of a saga message that arrives out-of-order
    /// (state not yet present for a non-starter) enters this deferral path using the DEFERRAL
    /// attempt counter and can dead-letter as <c>max_deferrals_exceeded</c> independently of the
    /// retry policy's <see cref="RetryPolicy.MaxRetryAttempts"/>. The two counters are intentionally
    /// separate: retries track handler failures, deferrals track ordering gaps.
    /// </remarks>
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

        // Clone the headers so the deferred copy never shares mutable state with the original delivery.
        var headers = new MessageHeaders(env.Headers)
        {
            [MessageHeaders.DeferralAttemptKey] = attempt.ToString()
        };

        // Mint a new MessageId per deferral attempt. The original delivery's idempotency lock is
        // released on commit; reusing the original MessageId would let a still-active lock (or a
        // COMPLETED marker) suppress the deferred copy as a false duplicate.
        var originalMessageId = headers.MessageId;
        if (!string.IsNullOrEmpty(originalMessageId))
        {
            headers.MessageId = $"{originalMessageId}:defer:{attempt}";
        }

        // Engine-owned hop counter so cyclic deferrals/forwards trip the max-hop guard.
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

    /// <summary>
    /// Polls the deferral store and republishes due messages. Entries are leased (hidden
    /// from other sweepers) for <see cref="TalariaOptions.DeferralLeaseTimeout"/> rather
    /// than removed, so a crash or shutdown mid-sweep never loses a message — the lease
    /// expires and a later sweep re-acquires it. A duplicate republication caused by
    /// lease expiry is absorbed downstream by the idempotency store.
    /// </summary>
    private async Task SweepDeferralsLoopAsync(ITransport transport, CancellationToken ct)
    {
        var interval = _options.DeferralBackoff < TimeSpan.FromSeconds(5)
            ? _options.DeferralBackoff
            : TimeSpan.FromSeconds(5);

        while (!ct.IsCancellationRequested)
        {
            IReadOnlyList<LeasedDeferral> due;
            try
            {
                due = await _deferralStore!.AcquireDueAsync(
                    DateTimeOffset.UtcNow,
                    _options.DeferralLeaseTimeout,
                    maxBatch: 64,
                    ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Deferral sweep failed to acquire due messages; retrying next interval.");
                due = Array.Empty<LeasedDeferral>();
            }

            Diagnostics.TalariaDiagnostics.DeferralActiveLeases.Add(due.Count);
            foreach (var leased in due)
            {
                if (leased.Lease.Token > 1)
                {
                    // Re-acquisition: a previous sweeper crashed (lease expired) or abandoned.
                    Diagnostics.TalariaDiagnostics.DeferralReacquired.Add(1, new KeyValuePair<string, object?>("messaging.destination.name", leased.Message.Topic));
                }

                await RepublishDeferredAsync(transport, leased, ct);
            }

            await Task.Delay(interval, ct);
        }
    }

    private async Task RepublishDeferredAsync(ITransport transport, LeasedDeferral leased, CancellationToken ct)
    {
        var message = leased.Message;
        var topicTag = new KeyValuePair<string, object?>("messaging.destination.name", message.Topic);
        try
        {
            var type = Type.GetType(message.MessageType);
            if (type is null)
            {
                // Poison entry — the type can never be resolved; drop it rather than retry forever.
                _logger.LogError("Deferred message {Id} has unresolvable payload type '{MessageType}'; dropping.", message.Id, message.MessageType);
                await _deferralStore!.CompleteAsync(leased.Lease, ct);
                Diagnostics.TalariaDiagnostics.DeferralActiveLeases.Add(-1);
                return;
            }

            var payload = JsonSerializer.Deserialize(message.PayloadJson, type)
                ?? throw new JsonException($"Deferred payload deserialized to null for {type.Name}.");

            var invoker = await GetOrCreateProducerAsync(transport, message.Topic, type, ct);
            await invoker.Produce(payload, new MessageHeaders(message.Headers), message.PartitionKey, ct);

            await _deferralStore!.CompleteAsync(leased.Lease, ct);

            Diagnostics.TalariaDiagnostics.DeferralRepublished.Add(1, topicTag);
            Diagnostics.TalariaDiagnostics.DeferralLag.Record(
                Math.Max(0, (DateTimeOffset.UtcNow - message.DueAt).TotalMilliseconds), topicTag);
            Diagnostics.TalariaDiagnostics.DeferralActiveLeases.Add(-1);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // Host shutting down mid-sweep — the lease simply expires and the entry is
            // re-acquired on the next sweep. Not an error.
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to republish deferred message {Id}; releasing the lease for retry.", message.Id);
            Diagnostics.TalariaDiagnostics.DeferralRepublishFailed.Add(1, topicTag);
            Diagnostics.TalariaDiagnostics.DeferralActiveLeases.Add(-1);
            try
            {
                await _deferralStore!.AbandonAsync(leased.Lease, DateTimeOffset.UtcNow + _options.DeferralBackoff, ct);
            }
            catch (Exception abandonEx)
            {
                // Not fatal: the lease expires on its own and the entry is retried then.
                _logger.LogError(abandonEx, "Failed to abandon deferral lease for message {Id}; it will retry when the lease expires.", message.Id);
            }
        }
    }

    /// <summary>
    /// Publishes staged outbox entries to the transport. Entries are leased (hidden from
    /// other relays) for <see cref="TalariaOptions.OutboxLeaseTimeout"/> rather than
    /// removed, so a crash or shutdown mid-publish never loses a staged message — the
    /// lease expires and a later relay re-acquires it. The duplicate publish that lease
    /// expiry can produce carries the same minted MessageId and is deduplicated
    /// downstream by the idempotency gate.
    /// </summary>
    private async Task OutboxRelayLoopAsync(ITransport transport, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            IReadOnlyList<LeasedOutboxMessage> pending;
            try
            {
                pending = await _outboxStore!.AcquirePendingAsync(
                    DateTimeOffset.UtcNow,
                    _options.OutboxLeaseTimeout,
                    maxBatch: 64,
                    ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Outbox relay failed to acquire pending messages; retrying next interval.");
                pending = Array.Empty<LeasedOutboxMessage>();
            }

            Diagnostics.TalariaDiagnostics.OutboxActiveLeases.Add(pending.Count);
            foreach (var leased in pending)
            {
                if (leased.Lease.Token > 1)
                {
                    // Re-acquisition: a previous relay crashed (lease expired) or abandoned.
                    Diagnostics.TalariaDiagnostics.OutboxReacquired.Add(1, new KeyValuePair<string, object?>("messaging.destination.name", leased.Message.Topic));
                }

                await PublishOutboxAsync(transport, leased, ct);
            }

            // Drain continuously while work remains; poll gently when idle.
            if (pending.Count == 0)
            {
                await Task.Delay(_options.OutboxRelayInterval, ct);
            }
        }
    }

    private async Task PublishOutboxAsync(ITransport transport, LeasedOutboxMessage leased, CancellationToken ct)
    {
        var message = leased.Message;
        var topicTag = new KeyValuePair<string, object?>("messaging.destination.name", message.Topic);
        try
        {
            var type = Type.GetType(message.MessageType);
            if (type is null)
            {
                // Poison entry — the type can never be resolved; drop it rather than retry forever.
                _logger.LogError("Outbox message {Id} has unresolvable payload type '{MessageType}'; dropping.", message.Id, message.MessageType);
                await _outboxStore!.CompleteAsync(leased.Lease, ct);
                Diagnostics.TalariaDiagnostics.OutboxActiveLeases.Add(-1);
                return;
            }

            var payload = JsonSerializer.Deserialize(message.PayloadJson, type)
                ?? throw new JsonException($"Outbox payload deserialized to null for {type.Name}.");

            var invoker = await GetOrCreateProducerAsync(transport, message.Topic, type, ct);
            await invoker.Produce(payload, new MessageHeaders(message.Headers), null, ct);

            await _outboxStore!.CompleteAsync(leased.Lease, ct);

            Diagnostics.TalariaDiagnostics.OutboxPublished.Add(1, topicTag);
            Diagnostics.TalariaDiagnostics.OutboxLag.Record(
                Math.Max(0, (DateTimeOffset.UtcNow - message.CreatedAt).TotalMilliseconds), topicTag);
            Diagnostics.TalariaDiagnostics.OutboxActiveLeases.Add(-1);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // Host shutting down mid-publish — the lease simply expires and the entry is
            // re-acquired on the next relay pass. Not an error.
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to publish outbox message {Id}; releasing the lease for retry.", message.Id);
            Diagnostics.TalariaDiagnostics.OutboxPublishFailed.Add(1, topicTag);
            Diagnostics.TalariaDiagnostics.OutboxActiveLeases.Add(-1);
            try
            {
                await _outboxStore!.AbandonAsync(leased.Lease, DateTimeOffset.UtcNow + _options.OutboxRelayInterval, ct);
            }
            catch (Exception abandonEx)
            {
                // Not fatal: the lease expires on its own and the entry is retried then.
                _logger.LogError(abandonEx, "Failed to abandon outbox lease for message {Id}; it will retry when the lease expires.", message.Id);
            }
        }
    }

    private async Task<ProducerInvoker> GetOrCreateProducerAsync(
        ITransport transport,
        string topic,
        Type messageType,
        CancellationToken ct)
    {
        if (_producers.TryGetValue((topic, messageType), out var existing))
        {
            return existing;
        }

        // One-time generic dispatch per producer — never in the per-message hot path.
        var method = typeof(SagaHostedService)
            .GetMethod(nameof(CreateProducerInvokerAsync), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!
            .MakeGenericMethod(messageType);

        var invoker = await (Task<ProducerInvoker>)method.Invoke(null, [transport, topic, ct])!;
        return _producers.GetOrAdd((topic, messageType), invoker);
    }

    private static async Task<ProducerInvoker> CreateProducerInvokerAsync<T>(ITransport transport, string topic, CancellationToken ct)
        where T : class
    {
        var producer = await transport.CreateProducerAsync<T>(topic, new ProducerOptions(), ct);
        return new ProducerInvoker(
            async (msg, headers, partitionKey, token) => await producer.ProduceAsync((T)msg, headers, partitionKey, token),
            producer);
    }

    private SessionDispatcher GetSessionDispatcher(Type messageType)
    {
        if (_sessionDispatchers.TryGetValue(messageType, out var existing))
        {
            return existing;
        }

        // One-time generic dispatch per message type — never in the per-message hot path.
        var method = typeof(SagaHostedService)
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
        // One-time generic dispatch per saga registration — never per message.
        var method = typeof(SagaHostedService)
            .GetMethod(nameof(CreateStateStoreAccessorTyped), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!
            .MakeGenericMethod(stateType);

        return (IStateStoreAccessor)method.Invoke(null, null)!;
    }

    private static IStateStoreAccessor CreateStateStoreAccessorTyped<TState>() where TState : class, new()
        => new StateStoreAccessor<TState>();

    private async Task ReleaseLockBestEffortAsync(MessageProcessingPipeline pipeline, IdempotencyLock? lck, CancellationToken ct)
    {
        if (lck is null)
        {
            return;
        }

        // Release failures must not mask the successful deferral path; the lock expires via TTL.
        try
        {
            await pipeline.ReleaseAsync(lck, ct);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to release idempotency lock {MessageId}; it expires via TTL.", lck.MessageId);
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        await base.StopAsync(cancellationToken);

        foreach (var invoker in _producers.Values)
        {
            await invoker.Producer.DisposeAsync();
        }

        _producers.Clear();
        _logger.LogInformation("Talaria Sagas: all producers disposed.");
    }
}
