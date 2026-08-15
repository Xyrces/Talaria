// SPDX-License-Identifier: Apache-2.0

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Talaria.Core.Abstractions;
using Talaria.Core.Registration;

namespace Talaria.Core.Hosting;

/// <summary>
/// Host-agnostic engine that runs supervised consumer loops for all <c>MapTopic</c> and <c>MapRequest</c> registrations.
/// </summary>
internal sealed class TopicConsumerEngine : IAsyncDisposable
{
    private readonly ITransport _transport;
    private readonly IReadOnlyList<TopicRegistration> _registrations;
    private readonly TalariaOptions _options;
    private readonly IDeferralStore? _deferralStore;
    private readonly MessageProcessingPipeline _pipeline;
    private readonly ILogger _logger;
    private readonly IServiceProvider? _serviceProvider;
    private readonly ProducerCache _producerCache;

    public TopicConsumerEngine(
        ITransport transport,
        TopicRegistry registry,
        TalariaOptions options,
        IDeferralStore? deferralStore,
        MessageProcessingPipeline pipeline,
        ILogger logger,
        IServiceProvider? serviceProvider = null)
    {
        _transport = transport;
        _registrations = registry.Registrations;
        _options = options;
        _deferralStore = deferralStore;
        _pipeline = pipeline;
        _logger = logger;
        _serviceProvider = serviceProvider;
        _producerCache = new ProducerCache(transport);

        var classConsumerWithoutProvider = _registrations.FirstOrDefault(r => r.ConsumerType is not null && serviceProvider is null);
        if (classConsumerWithoutProvider is not null)
        {
            throw new InvalidOperationException(
                $"Topic '{classConsumerWithoutProvider.TopicName}' uses a class-based consumer but no IServiceProvider was supplied to {nameof(TopicConsumerEngine)}. " +
                "A service provider is required to resolve ITopicConsumer<T> instances and create per-message scopes.");
        }

        var requestConsumerWithoutProvider = _registrations.FirstOrDefault(r => r.RequestConsumerType is not null && serviceProvider is null);
        if (requestConsumerWithoutProvider is not null)
        {
            throw new InvalidOperationException(
                $"Topic '{requestConsumerWithoutProvider.TopicName}' uses a class-based request consumer but no IServiceProvider was supplied to {nameof(TopicConsumerEngine)}. " +
                "A service provider is required to resolve IRequestConsumer<TRequest, TResponse> instances and create per-message scopes.");
        }
    }

    public async Task RunAsync(CancellationToken ct)
    {
        var retryCoordinator = new RetryCoordinator(_deferralStore, _options, _logger);

        if (_deferralStore is null && _registrations.Any(r => IsRetryEnabled(r)))
        {
            _logger.LogWarning(
                "One or more topic registrations have delayed retries enabled but no IDeferralStore is registered. " +
                "Retry attempts will be routed to the DLQ with reason 'retry_unavailable'. " +
                "Register a deferral store via UseInMemoryDeferralStore() or UseRedisDeferralStore().");
        }

        var tasks = _registrations.Select(registration =>
            ConsumerSupervision.RunSupervisedAsync(
                $"topic:{registration.TopicName}",
                ct => ConsumeTopicAsync(registration, retryCoordinator, ct),
                _logger,
                ct)).ToList();

        if (tasks.Count > 0)
        {
            await Task.WhenAll(tasks);
        }
    }

    private bool IsRetryEnabled(TopicRegistration registration)
    {
        var policy = registration.RetryPolicy ?? _options.DefaultRetryPolicy;
        return RetryPolicy.IsEnabled(policy);
    }

    private async Task ConsumeTopicAsync(
        TopicRegistration registration,
        RetryCoordinator retryCoordinator,
        CancellationToken ct)
    {
        var consumerGroup = registration.ConsumerGroup
            ?? _options.ConsumerGroupOverride
            ?? $"{_options.ApplicationName}.{registration.TopicName}";

        var method = typeof(TopicConsumerEngine)
            .GetMethod(nameof(ConsumeTopicTypedAsync),
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!
            .MakeGenericMethod(registration.MessageType);

        await (Task)method.Invoke(this, [registration, retryCoordinator, consumerGroup, ct])!;
    }

    private async Task ConsumeTopicTypedAsync<T>(
        TopicRegistration registration,
        RetryCoordinator retryCoordinator,
        string consumerGroup,
        CancellationToken ct)
    {
        await using var consumer = await _transport.CreateConsumerAsync<T>(
            registration.TopicName,
            new ConsumerOptions { ConsumerGroup = consumerGroup },
            ct);

        _logger.LogInformation(
            "Talaria: consuming topic '{Topic}' (group: {Group}, transport: {Transport})",
            registration.TopicName, consumerGroup, _transport.Name);

        var pipeline = _pipeline;
        var isRequest = registration.RequestHandler is not null || registration.RequestConsumerType is not null;

        await foreach (var envelope in consumer.ConsumeAsync(ct))
        {
            using var activity = Diagnostics.TalariaDiagnostics.StartConsumerActivity(
                registration.TopicName, typeof(T).Name, envelope.Headers);

            var sw = System.Diagnostics.Stopwatch.StartNew();

            try
            {
                if (pipeline.IsHopCountExceeded(envelope, registration.TopicName))
                {
                    Diagnostics.TalariaDiagnostics.DlqRouted.Add(1,
                        new KeyValuePair<string, object?>("messaging.destination.name", registration.TopicName),
                        new KeyValuePair<string, object?>("messaging.system", "talaria"));

                    await consumer.NackAsync(envelope, ct);
                    continue;
                }

                var gate = await pipeline.AcquireAsync(envelope, consumerGroup, ct);
                if (gate.IsDuplicate)
                {
                    _logger.LogDebug("Message {MessageId} skipped. Idempotency lock claimed by another worker or already completed.", envelope.Headers.MessageId);
                    await consumer.CommitAsync(envelope, ct);
                    continue;
                }

                activity?.SetTag("talaria.consumer.type", registration.ConsumerType?.FullName ?? registration.RequestConsumerType?.FullName ?? "delegate");

                Exception? handlerException = null;
                object? response = null;
                try
                {
                    if (registration.ConsumerType is not null)
                    {
                        var scope = _serviceProvider!.CreateAsyncScope();
                        try
                        {
                            var topicConsumer = (ITopicConsumer<T>)scope.ServiceProvider.GetRequiredService(registration.ConsumerType);
                            var context = new ConsumeContext<T>
                            {
                                Envelope = envelope,
                                CancellationToken = ct,
                                Services = scope.ServiceProvider,
                            };
                            await topicConsumer.ConsumeAsync(context);
                        }
                        catch (Exception ex)
                        {
                            handlerException = ex;
                        }
                        finally
                        {
                            try
                            {
                                await scope.DisposeAsync();
                            }
                            catch (Exception disposeEx)
                            {
                                if (handlerException is not null)
                                {
                                    _logger.LogError(disposeEx,
                                        "Scope disposal for topic '{Topic}' failed while a handler exception was already in flight; preserving the original handler exception.",
                                        registration.TopicName);
                                }
                                else
                                {
                                    _logger.LogError(disposeEx,
                                        "Scope disposal for topic '{Topic}' failed after the handler succeeded; the message will still be committed.",
                                        registration.TopicName);
                                }
                            }
                        }
                    }
                    else if (isRequest)
                    {
                        response = await InvokeRequestHandlerAsync(registration, envelope, ct);
                    }
                    else
                    {
                        var metadata = new EnvelopeMetadata(
                            envelope.PartitionKey,
                            envelope.Partition,
                            envelope.Offset,
                            envelope.Timestamp,
                            envelope.CorrelationId);
                        await registration.Handler!(envelope.Payload!, envelope.Headers, metadata, ct);
                    }
                }
                catch (Exception ex)
                {
                    handlerException = ex;
                }

                if (handlerException is not null)
                {
                    // During shutdown the handler may observe OperationCanceledException (or any
                    // exception while the loop token is already canceled). Do not DLQ in that
                    // case; leave the message uncommitted so it redelivers after restart.
                    if (ct.IsCancellationRequested)
                    {
                        _logger.LogDebug(
                            handlerException,
                            "Handler for topic '{Topic}' threw during shutdown; leaving message uncommitted for redelivery.",
                            registration.TopicName);
                        continue;
                    }

                    _logger.LogError(handlerException,
                        "Handler for topic '{Topic}' failed. Evaluating delayed retry policy.",
                        registration.TopicName);

                    activity?.SetStatus(System.Diagnostics.ActivityStatusCode.Error, handlerException.Message);

                    Diagnostics.TalariaDiagnostics.MessagesFailed.Add(1,
                        new KeyValuePair<string, object?>("messaging.destination.name", registration.TopicName));

                    var outcome = await retryCoordinator.TryCoordinateTopicRetryAsync(
                        registration, pipeline, consumer, envelope, handlerException, gate.Lock, ct);

                    if (outcome == RetryCoordinator.RetryOutcome.NotRetryable)
                    {
                        _logger.LogError(handlerException,
                            "Handler for topic '{Topic}' failed. Routing to DLQ.",
                            registration.TopicName);

                        await PublishFaultAsync(registration, envelope, handlerException, ct);

                        Diagnostics.TalariaDiagnostics.DlqRouted.Add(1,
                            new KeyValuePair<string, object?>("messaging.destination.name", registration.TopicName));
                        await pipeline.FailAsync(gate.Lock, consumer, envelope, handlerException, null, ct);
                    }
                    else if (outcome == RetryCoordinator.RetryOutcome.Exhausted || outcome == RetryCoordinator.RetryOutcome.Unavailable)
                    {
                        await PublishFaultAsync(registration, envelope, handlerException, ct);
                    }

                    continue;
                }

                if (isRequest && response is not null)
                {
                    var replyTo = envelope.Headers.ReplyTo;
                    if (string.IsNullOrEmpty(replyTo))
                    {
                        _logger.LogWarning(
                            "Request on topic '{Topic}' has no '{ReplyToHeader}' header; no response will be published.",
                            registration.TopicName, MessageHeaders.ReplyToKey);
                    }
                    else
                    {
                        await PublishResponseAsync(registration, replyTo, response, envelope.Headers, ct);
                    }
                }

                await pipeline.CompleteAsync(gate.Lock, consumer, envelope, ct);

                Diagnostics.TalariaDiagnostics.MessagesConsumed.Add(1,
                    new KeyValuePair<string, object?>("messaging.destination.name", registration.TopicName));
            }
            finally
            {
                sw.Stop();
                Diagnostics.TalariaDiagnostics.ProcessingDuration.Record(sw.Elapsed.TotalMilliseconds,
                    new KeyValuePair<string, object?>("messaging.destination.name", registration.TopicName));
            }
        }

        _logger.LogInformation("Talaria: consumer for '{Topic}' shut down.", registration.TopicName);
    }

    private async Task<object?> InvokeRequestHandlerAsync<T>(
        TopicRegistration registration,
        MessageEnvelope<T> envelope,
        CancellationToken ct)
    {
        if (registration.RequestConsumerType is not null)
        {
            var scope = _serviceProvider!.CreateAsyncScope();
            try
            {
                var method = typeof(TopicConsumerEngine)
                    .GetMethod(nameof(InvokeClassRequestConsumerAsync), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!
                    .MakeGenericMethod(typeof(T), registration.ResponseType!);

                return await (Task<object?>)method.Invoke(this, [scope.ServiceProvider, registration.RequestConsumerType, envelope, ct])!;
            }
            finally
            {
                try
                {
                    await scope.DisposeAsync();
                }
                catch (Exception disposeEx)
                {
                    _logger.LogError(disposeEx,
                        "Scope disposal for request topic '{Topic}' failed; the response will still be published if the handler succeeded.",
                        registration.TopicName);
                }
            }
        }

        var metadata = new EnvelopeMetadata(
            envelope.PartitionKey,
            envelope.Partition,
            envelope.Offset,
            envelope.Timestamp,
            envelope.CorrelationId);

        return await registration.RequestHandler!(envelope.Payload!, envelope.Headers, metadata, ct);
    }

    private async Task<object?> InvokeClassRequestConsumerAsync<TRequest, TResponse>(
        IServiceProvider scopedServices,
        Type consumerType,
        MessageEnvelope<TRequest> envelope,
        CancellationToken ct)
        where TRequest : class
        where TResponse : class
    {
        var consumer = (IRequestConsumer<TRequest, TResponse>)scopedServices.GetRequiredService(consumerType);
        var context = new ConsumeContext<TRequest>
        {
            Envelope = envelope,
            CancellationToken = ct,
            Services = scopedServices,
        };
        return await consumer.ConsumeAsync(context, ct);
    }

    private async Task PublishResponseAsync(
        TopicRegistration registration,
        string replyTo,
        object response,
        MessageHeaders requestHeaders,
        CancellationToken ct)
    {
        var invoker = await _producerCache.GetOrCreateAsync(replyTo, registration.ResponseType!, ct);

        var headers = new MessageHeaders(requestHeaders)
        {
            RequestId = requestHeaders.RequestId,
        };
        headers.HopCount++;

        await invoker.Produce(response, headers, null, ct);
    }

    private async Task PublishFaultAsync<T>(
        TopicRegistration registration,
        MessageEnvelope<T> envelope,
        Exception ex,
        CancellationToken ct)
    {
        var replyTo = envelope.Headers.ReplyTo;
        var requestId = envelope.Headers.RequestId;
        if (string.IsNullOrEmpty(replyTo) || string.IsNullOrEmpty(requestId))
        {
            return;
        }

        try
        {
            var invoker = await _producerCache.GetOrCreateAsync(replyTo, typeof(RequestFaultInfo), ct);

            var headers = new MessageHeaders
            {
                RequestId = requestId,
                RequestFault = true,
            };
            headers[RequestClientFaultHeaders.ExceptionTypeKey] = ex.GetType().FullName ?? "Unknown";
            if (_options.IncludeExceptionDetailsInDlq)
            {
                headers[RequestClientFaultHeaders.ExceptionMessageKey] = ex.Message;
            }
            headers.HopCount++;

            await invoker.Produce(new RequestFaultInfo(), headers, null, ct);
        }
        catch (Exception publishEx)
        {
            _logger.LogError(publishEx,
                "Failed to publish fault response for request {RequestId} to '{ReplyTo}'.",
                requestId, replyTo);
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _producerCache.DisposeAsync();
    }
}
