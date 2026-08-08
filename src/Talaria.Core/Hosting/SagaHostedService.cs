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
/// Hosted service that listens on all configured saga topics.
/// One supervised consumer per topic; messages are fanned out to the correct saga step
/// via the message-type header. Dispatch topics are explicit (DispatchTo) and producers
/// are created once at startup — no reflection in the per-message hot path.
/// </summary>
public sealed class SagaHostedService : BackgroundService
{
    private readonly SagaRegistry _registry;
    private readonly IServiceProvider _serviceProvider;
    private readonly TalariaOptions _options;
    private readonly ILogger<SagaHostedService> _logger;

    // Durable deferral store; resolved at startup. Null means deferral is unavailable.
    private IDeferralStore? _deferralStore;

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
        Func<object, MessageHeaders?, CancellationToken, Task> Produce,
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
                "No IDeferralStore is registered. Out-of-order saga messages and handler-initiated " +
                "deferrals will be routed to the DLQ instead of being deferred. " +
                "Register one via UseRedisDeferralStore() or UseInMemoryDeferralStore().");
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
                ct => ConsumeTopicLoopAsync(kvp.Key, kvp.Value, transport, pipeline, ct),
                _logger,
                stoppingToken)).ToList();

        if (_deferralStore != null)
        {
            tasks.Add(ConsumerSupervision.RunSupervisedAsync(
                "deferral-sweeper",
                ct => SweepDeferralsLoopAsync(transport, ct),
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

            await ProcessStepMessageAsync(env, route, transport, pipeline, consumer, ct);
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

            // 5b. Starter replay: state already exists — idempotent replay, skip and commit.
            if (state != null && step.IsStarter)
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
                Diagnostics.TalariaDiagnostics.DlqRouted.Add(1, new KeyValuePair<string, object?>("messaging.destination.name", step.TopicName));

                await pipeline.FailAsync(gate.Lock, consumer, env, ex, null, ct);
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

            // 8. Transactional dispatch of outbound messages via explicit routes.
            //    The consumed message's offset joins the transaction when the transport
            //    supports it (Kafka exactly-once); InMemory buffers until commit.
            var offsetSource = env.SourceTopic is not null && env.Partition is int partition
                ? new TransactionOffsetSource(env.SourceTopic, partition, env.Offset)
                : null;

            await using var tx = await transport.BeginTransactionAsync(_options.ApplicationName, offsetSource, ct);

            foreach (var outbound in result.OutboundMessages)
            {
                var outboundType = outbound.GetType();
                if (!_dispatchRoutes.TryGetValue(outboundType, out var outboundTopic))
                {
                    throw new InvalidOperationException(
                        $"Saga '{stateType.Name}' dispatched message type '{outboundType.Name}' with no DispatchTo mapping. " +
                        $"Declare the route with saga.DispatchTo<{outboundType.Name}>(\"topic\").");
                }

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
            DateTimeOffset.UtcNow + TimeSpan.FromMilliseconds(_options.DeferralBackoff.TotalMilliseconds * attempt));

        await _deferralStore.EnqueueAsync(deferred, ct);
    }

    /// <summary>
    /// Polls the deferral store and republishes due messages. Entries are claimed atomically
    /// on pop, so concurrent sweepers across nodes cannot double-publish. A failed
    /// republication is requeued with a fresh backoff.
    /// </summary>
    private async Task SweepDeferralsLoopAsync(ITransport transport, CancellationToken ct)
    {
        var interval = _options.DeferralBackoff < TimeSpan.FromSeconds(5)
            ? _options.DeferralBackoff
            : TimeSpan.FromSeconds(5);

        while (!ct.IsCancellationRequested)
        {
            IReadOnlyList<DeferredMessage> due;
            try
            {
                due = await _deferralStore!.PopDueAsync(DateTimeOffset.UtcNow, maxBatch: 64, ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Deferral sweep failed to pop due messages; retrying next interval.");
                due = Array.Empty<DeferredMessage>();
            }

            foreach (var message in due)
            {
                await RepublishDeferredAsync(transport, message, ct);
            }

            await Task.Delay(interval, ct);
        }
    }

    private async Task RepublishDeferredAsync(ITransport transport, DeferredMessage message, CancellationToken ct)
    {
        try
        {
            var type = Type.GetType(message.MessageType);
            if (type is null)
            {
                // Poison entry — the type can never be resolved; drop it rather than requeue forever.
                _logger.LogError("Deferred message {Id} has unresolvable payload type '{MessageType}'; dropping.", message.Id, message.MessageType);
                await _deferralStore!.CompleteAsync(message.Id, ct);
                return;
            }

            var payload = JsonSerializer.Deserialize(message.PayloadJson, type)
                ?? throw new JsonException($"Deferred payload deserialized to null for {type.Name}.");

            var invoker = await GetOrCreateProducerAsync(transport, message.Topic, type, ct);
            await invoker.Produce(payload, new MessageHeaders(message.Headers), ct);

            await _deferralStore!.CompleteAsync(message.Id, ct);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // Host shutting down mid-sweep — the entry stays claimed; it expires with the store
            // or (InMemory) is discarded with the process. Not an error.
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to republish deferred message {Id}; requeueing.", message.Id);
            try
            {
                await _deferralStore!.RequeueAsync(message, DateTimeOffset.UtcNow + _options.DeferralBackoff, ct);
            }
            catch (Exception requeueEx)
            {
                _logger.LogError(requeueEx, "Failed to requeue deferred message {Id}; it is lost.", message.Id);
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
            async (msg, headers, token) => await producer.ProduceAsync((T)msg, headers, null, token),
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
