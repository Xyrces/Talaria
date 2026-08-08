using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Talaria.Core.Abstractions;
using Talaria.Core.Sagas;

namespace Talaria.Core.Hosting;

/// <summary>
/// Hosted service that listens on all configured saga topics.
/// </summary>
public sealed class SagaHostedService : BackgroundService
{
    private readonly SagaRegistry _registry;
    private readonly IServiceProvider _serviceProvider;
    private readonly TalariaOptions _options;
    private readonly ILogger<SagaHostedService> _logger;
    private readonly List<IAsyncDisposable> _consumers = new();
    private readonly List<Task> _consumerTasks = new();

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
        // 1. Get the transport
        var transport = _serviceProvider.GetRequiredService<ITransport>();
        
        // 2. Discover all topics sagas subscribe to
        var consumerTasksGroupedByTopic = new Dictionary<string, Func<Task>>();
        
        foreach (var sagaReg in _registry.Registrations)
        {
            var stateType = sagaReg.StateType;
            var stateStoreType = typeof(IStateStore<>).MakeGenericType(stateType);

            foreach (var step in sagaReg.Steps)
            {
                var topic = step.TopicName;
                var messageType = step.MessageType;
                var resolver = step.CorrelationResolver;

                if (consumerTasksGroupedByTopic.ContainsKey(topic))
                {
                    throw new InvalidOperationException(
                        $"Multiple saga steps are mapped to topic '{topic}' (saga state '{sagaReg.StateType.Name}', message '{messageType.Name}'). " +
                        "Only one saga step per topic is currently supported; map each step to a distinct topic.");
                }

                var consumerOpts = new ConsumerOptions
                {
                    ConsumerGroup = _options.ApplicationName,
                    BufferCapacity = 1 // Process sagas one at a time per consumer for now
                };

                // Build a strongly typed CreateConsumer call for the topic
                var createConsumerMethod = typeof(ITransport)
                    .GetMethod(nameof(ITransport.CreateConsumerAsync))!
                    .MakeGenericMethod(messageType);

                consumerTasksGroupedByTopic[topic] = async () =>
                {
                    var consumerTask = (Task)createConsumerMethod.Invoke(transport, new object[] { topic, consumerOpts, stoppingToken })!;
                    await consumerTask.ConfigureAwait(false);
                    var consumer = consumerTask.GetType().GetProperty("Result")!.GetValue(consumerTask)!;
                    
                    _consumers.Add((IAsyncDisposable)consumer);
                    
                    var loopMethod = typeof(SagaHostedService)
                        .GetMethod(nameof(ConsumeLoopAsync), BindingFlags.NonPublic | BindingFlags.Instance)!
                        .MakeGenericMethod(messageType, stateType);

                    var loopTask = (Task)loopMethod.Invoke(this, new[]
                    {
                        consumer,
                        step,
                        stateStoreType,
                        transport,
                        stoppingToken
                    })!;
                    
                    _consumerTasks.Add(loopTask);
                };
            }
        }

        foreach (var factory in consumerTasksGroupedByTopic.Values)
        {
            await factory();
        }
        
        _logger.LogInformation("Talaria Sagas: started {Count} topic consumers.", _consumerTasks.Count);
        
        await Task.WhenAll(_consumerTasks);
    }

    private async Task ConsumeLoopAsync<TMessage, TState>(
        IConsumer<TMessage> consumer,
        SagaStepRegistration step,
        Type stateStoreType,
        ITransport transport,
        CancellationToken ct) where TMessage : class where TState : class, new()
    {
        await foreach (var env in consumer.ConsumeAsync(ct))
        {
            using var activity = Diagnostics.TalariaDiagnostics.StartConsumerActivity(
                step.TopicName, typeof(TMessage).Name, env.Headers);
            activity?.SetTag("saga.type", typeof(TState).Name);

            var sw = System.Diagnostics.Stopwatch.StartNew();
            
            using var scope = _serviceProvider.CreateScope();
            var stateStore = (IStateStore<TState>)scope.ServiceProvider.GetRequiredService(stateStoreType);
            
            // 1. Resolve Correlation Id
            var correlationId = step.CorrelationResolver != null 
                ? step.CorrelationResolver(env.Payload) 
                : Core.Sagas.CorrelationResolver.Resolve(env.Payload, env.Headers);

            if (string.IsNullOrWhiteSpace(correlationId))
            {
                _logger.LogWarning("No correlation ID found for saga message {Type} on topic {Topic}", typeof(TMessage).Name, env.SourceTopic);
                
                env.Headers.DlqReason = "missing_correlation_id";
                activity?.SetStatus(System.Diagnostics.ActivityStatusCode.Error, "Missing Correlation Id");
                Diagnostics.TalariaDiagnostics.MessagesFailed.Add(1, new KeyValuePair<string, object?>("messaging.destination.name", step.TopicName));
                Diagnostics.TalariaDiagnostics.DlqRouted.Add(1, new KeyValuePair<string, object?>("messaging.destination.name", step.TopicName));

                await consumer.NackAsync(env, ct);
                continue;
            }
            
            activity?.SetTag("saga.correlation_id", correlationId);

            var idempotencyStore = (IIdempotencyStore?)_serviceProvider.GetService(typeof(IIdempotencyStore));
            var msgId = env.Headers.MessageId;
            IdempotencyLock? idempotencyLock = null;

            if (idempotencyStore != null && !string.IsNullOrEmpty(msgId))
            {
                // Expiration is generous to allow for slow processing without immediate concurrent retry overlaps
                idempotencyLock = await idempotencyStore.TryAcquireLockAsync(msgId, _options.ApplicationName, _options.IdempotencyLockTtl, ct);
                
                if (idempotencyLock is null)
                {
                    _logger.LogDebug("Saga Message {MessageId} skipped. Idempotency lock claimed by another worker or already completed.", msgId);
                    // We immediately commit the message to suppress further polling!
                    await consumer.CommitAsync(env, ct);
                    continue;
                }
            }

            // 2. Load State
            var state = await stateStore.GetAsync(correlationId, ct);

            // 3. Guards / Starter checking
            if (state == null && !step.IsStarter)
            {
                // Defer out-of-order message
                _logger.LogInformation("Received non-starter message for Saga {SagaType} but state {Id} not found. Deferring...", typeof(TState).Name, correlationId);
                try 
                {
                    await HandleDeferralAsync(env, transport, ct);

                    // The deferred copy is published — safe to commit the original and release the
                    // idempotency lock so the deferred copy (new MessageId) is not skipped as a duplicate.
                    if (idempotencyLock is not null && idempotencyStore != null)
                    {
                        await idempotencyStore.ReleaseLockAsync(idempotencyLock, ct);
                    }
                    await consumer.CommitAsync(env, ct);

                    Diagnostics.TalariaDiagnostics.MessagesDeferred.Add(1, new KeyValuePair<string, object?>("saga.type", typeof(TState).Name));
                }
                catch (InvalidOperationException ex)
                {
                    env.Headers.DlqException = ex.Message;
                    env.Headers.DlqReason = "max_deferrals_exceeded";

                    if (idempotencyLock is not null && idempotencyStore != null)
                    {
                        await idempotencyStore.ReleaseLockAsync(idempotencyLock, ct);
                    }

                    Diagnostics.TalariaDiagnostics.MessagesFailed.Add(1, new KeyValuePair<string, object?>("messaging.destination.name", step.TopicName));
                    Diagnostics.TalariaDiagnostics.DlqRouted.Add(1, new KeyValuePair<string, object?>("messaging.destination.name", step.TopicName));

                    await consumer.NackAsync(env, ct);
                }
                finally
                {
                    sw.Stop();
                    Diagnostics.TalariaDiagnostics.ProcessingDuration.Record(sw.Elapsed.TotalMilliseconds, new KeyValuePair<string, object?>("messaging.destination.name", step.TopicName));
                }
                continue;
            }
            if (state != null && step.IsStarter)
            {
                // Starter message but state already exists — idempotent replay (e.g. redelivery after a
                // crash between state save and offset commit). Do not re-run the starter handler:
                // acknowledge the message and move on.
                _logger.LogWarning(
                    "Saga {SagaType} starter message {MessageId} on topic {Topic} received, but state for correlation {CorrelationId} already exists. Skipping as an idempotent replay.",
                    typeof(TState).Name, msgId, step.TopicName, correlationId);

                if (idempotencyLock is not null && idempotencyStore != null)
                {
                    await idempotencyStore.MarkCompleteAsync(idempotencyLock, ct);
                }
                await consumer.CommitAsync(env, ct);
                continue;
            }

            var runState = state ?? new TState();
            
            // 4. Run pure function
            var context = new Core.Sagas.SagaContext<object>();
            Core.Sagas.SagaResult<object> result;
            try
            {
                result = await step.Handler(runState, env.Payload, context);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Saga {Type} threw an exception while handling message {MsgType}", typeof(TState).Name, typeof(TMessage).Name);
                env.Headers.DlqException = ex.Message;
                activity?.SetStatus(System.Diagnostics.ActivityStatusCode.Error, ex.Message);
                
                if (idempotencyLock is not null && idempotencyStore != null)
                {
                    await idempotencyStore.ReleaseLockAsync(idempotencyLock, ct);
                }

                Diagnostics.TalariaDiagnostics.MessagesFailed.Add(1, new KeyValuePair<string, object?>("messaging.destination.name", step.TopicName));
                Diagnostics.TalariaDiagnostics.DlqRouted.Add(1, new KeyValuePair<string, object?>("messaging.destination.name", step.TopicName));

                await consumer.NackAsync(env, ct);
                sw.Stop();
                Diagnostics.TalariaDiagnostics.ProcessingDuration.Record(sw.Elapsed.TotalMilliseconds, new KeyValuePair<string, object?>("messaging.destination.name", step.TopicName));
                continue;
            }

            // If function deferred manually
            if (result.IsDeferred)
            {
                try 
                {
                    activity?.SetTag("saga.status", "deferred");
                    await HandleDeferralAsync(env, transport, ct);

                    // The deferred copy is published — safe to commit the original and release the
                    // idempotency lock so the deferred copy (new MessageId) is not skipped as a duplicate.
                    if (idempotencyLock is not null && idempotencyStore != null)
                    {
                        await idempotencyStore.ReleaseLockAsync(idempotencyLock, ct);
                    }
                    await consumer.CommitAsync(env, ct);

                    Diagnostics.TalariaDiagnostics.MessagesDeferred.Add(1, new KeyValuePair<string, object?>("saga.type", typeof(TState).Name));
                }
                catch (InvalidOperationException ex)
                {
                    env.Headers.DlqException = ex.Message;
                    env.Headers.DlqReason = "max_deferrals_exceeded";

                    if (idempotencyLock is not null && idempotencyStore != null)
                    {
                        await idempotencyStore.ReleaseLockAsync(idempotencyLock, ct);
                    }

                    Diagnostics.TalariaDiagnostics.MessagesFailed.Add(1, new KeyValuePair<string, object?>("messaging.destination.name", step.TopicName));
                    Diagnostics.TalariaDiagnostics.DlqRouted.Add(1, new KeyValuePair<string, object?>("messaging.destination.name", step.TopicName));

                    await consumer.NackAsync(env, ct);
                }
                finally
                {
                    sw.Stop();
                    Diagnostics.TalariaDiagnostics.ProcessingDuration.Record(sw.Elapsed.TotalMilliseconds, new KeyValuePair<string, object?>("messaging.destination.name", step.TopicName));
                }
                continue;
            }

            // 5. Transactional dispatch
            await using var tx = await transport.BeginTransactionAsync(ct);
            
            foreach (var outbound in result.OutboundMessages)
            {
                var producerMethod = typeof(ITransport)
                    .GetMethod(nameof(ITransport.CreateProducerAsync))!
                    .MakeGenericMethod(outbound.GetType());
                
                var topic = outbound.GetType().Name.ToLowerInvariant();
                var prodTask = (Task)producerMethod.Invoke(transport, new object?[] { topic, new ProducerOptions(), ct })!;
                await prodTask.ConfigureAwait(false);
                var dynProducer = prodTask.GetType().GetProperty("Result")!.GetValue(prodTask)!;
                
                // Propagate trace context!
                var headers = new MessageHeaders();
                if (System.Diagnostics.Activity.Current != null)
                {
                    headers.TraceParent = System.Diagnostics.Activity.Current.Id;
                    headers.TraceState = System.Diagnostics.Activity.Current.TraceStateString;
                }

                var produceMethod = dynProducer.GetType().GetMethod(nameof(IProducer<object>.ProduceAsync))!;
                var produceTask = (Task)produceMethod.Invoke(dynProducer, new object?[] { outbound, headers, null, ct })!;
                await produceTask.ConfigureAwait(false);
            }

            // 6. Save or purge state
            if (result.IsCompleted)
            {
                await stateStore.DeleteAsync(correlationId, ct);
                activity?.SetTag("saga.status", "completed");
            }
            else
            {
                // Safe cast since result.State is definitely TState based on our generic wrapper
                await stateStore.SaveAsync(correlationId, (TState)result.State!, ct);
                activity?.SetTag("saga.status", "transitioned");
            }

            try 
            {
                await tx.CommitAsync(ct);
                await consumer.CommitAsync(env, ct);
                
                if (idempotencyLock is not null && idempotencyStore != null)
                {
                    await idempotencyStore.MarkCompleteAsync(idempotencyLock, ct);
                }

                Diagnostics.TalariaDiagnostics.MessagesConsumed.Add(1, new KeyValuePair<string, object?>("messaging.destination.name", step.TopicName));
            }
            catch(Exception ex)
            {
                _logger.LogError(ex, "Failed to commit saga transition");
                
                if (idempotencyLock is not null && idempotencyStore != null)
                {
                    await idempotencyStore.ReleaseLockAsync(idempotencyLock, ct);
                }

                activity?.SetStatus(System.Diagnostics.ActivityStatusCode.Error, ex.Message);
                Diagnostics.TalariaDiagnostics.MessagesFailed.Add(1, new KeyValuePair<string, object?>("messaging.destination.name", step.TopicName));
                
                await tx.AbortAsync(ct);
                await consumer.NackAsync(env, ct);
            }
            finally
            {
                sw.Stop();
                Diagnostics.TalariaDiagnostics.ProcessingDuration.Record(sw.Elapsed.TotalMilliseconds, new KeyValuePair<string, object?>("messaging.destination.name", step.TopicName));
            }
        }
    }

    private async Task HandleDeferralAsync<TMessage>(MessageEnvelope<TMessage> env, ITransport transport, CancellationToken ct) where TMessage : class
    {
        if (string.IsNullOrEmpty(env.SourceTopic))
        {
            throw new InvalidOperationException($"Cannot defer saga message of type {typeof(TMessage).Name}: the envelope has no source topic.");
        }

        int attempt = 1;
        if (env.Headers.TryGetValue(MessageHeaders.DeferralAttemptKey, out var strVal) && int.TryParse(strVal, out var parsed))
        {
            attempt = parsed + 1;
        }

        if (attempt > _options.MaxDeferralAttempts)
        {
            _logger.LogWarning("Message exceeded max deferral attempts ({Max}). Routing to DLQ.", _options.MaxDeferralAttempts);
            throw new InvalidOperationException($"Max deferral attempts ({_options.MaxDeferralAttempts}) exceeded for Saga message of type {typeof(TMessage).Name}");
        }

        env.Headers[MessageHeaders.DeferralAttemptKey] = attempt.ToString();

        // Mint a new MessageId per deferral attempt. The original delivery's idempotency lock is
        // released on commit; reusing the original MessageId would let a still-active lock (or a
        // COMPLETED marker) suppress the deferred copy as a false duplicate.
        var originalMessageId = env.Headers.MessageId;
        if (!string.IsNullOrEmpty(originalMessageId))
        {
            env.Headers.MessageId = $"{originalMessageId}:defer:{attempt}";
        }

        // Engine-owned hop counter so cyclic deferrals/forwards trip the max-hop guard.
        env.Headers.HopCount = env.Headers.HopCount + 1;

        var delay = TimeSpan.FromMilliseconds(_options.DeferralBackoff.TotalMilliseconds * attempt);

        // Run background task to delay and reproduce
        _ = Task.Run(async () =>
        {
            try
            {
                await Task.Delay(delay, ct);
                
                var producerMethod = typeof(ITransport)
                    .GetMethod(nameof(ITransport.CreateProducerAsync))!
                    .MakeGenericMethod(typeof(TMessage));
                
                var prodTask = (Task)producerMethod.Invoke(transport, new object?[] { env.SourceTopic, new ProducerOptions(), ct })!;
                await prodTask.ConfigureAwait(false);
                var dynProducer = prodTask.GetType().GetProperty("Result")!.GetValue(prodTask)!;
                
                if (System.Diagnostics.Activity.Current != null)
                {
                    env.Headers.TraceParent = System.Diagnostics.Activity.Current.Id;
                    env.Headers.TraceState = System.Diagnostics.Activity.Current.TraceStateString;
                }

                var produceMethod = dynProducer.GetType().GetMethod(nameof(IProducer<object>.ProduceAsync))!;
                var produceTask = (Task)produceMethod.Invoke(dynProducer, new object?[] { env.Payload, env.Headers, null, ct })!;
                await produceTask.ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                // Host shutting down mid-deferral — expected, not an error.
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to execute deferred message dispatch");
            }
        }, ct);
    }

    public override void Dispose()
    {
        foreach (var consumer in _consumers)
        {
            consumer.DisposeAsync().AsTask().GetAwaiter().GetResult();
        }
        base.Dispose();
    }
}
