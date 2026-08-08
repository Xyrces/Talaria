using System.Text.Json;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Talaria.Core.Abstractions;
using Talaria.Core.Registration;

namespace Talaria.Core.Hosting;

/// <summary>
/// Background service that manages the lifecycle of all topic consumers.
/// On start: creates a consumer for each topic registration and dispatches messages to handlers.
/// On stop: disposes all consumers gracefully.
/// </summary>
public sealed class TalariaHostedService : BackgroundService
{
    private readonly ITransport _transport;
    private readonly TopicRegistry _registry;
    private readonly TalariaOptions _options;
    private readonly ILogger<TalariaHostedService> _logger;
    private readonly IServiceProvider _services;
    private readonly List<IAsyncDisposable> _consumers = new();

    public TalariaHostedService(
        ITransport transport,
        TopicRegistry registry,
        IOptions<TalariaOptions> options,
        ILogger<TalariaHostedService> logger,
        IServiceProvider services)
    {
        _transport = transport;
        _registry = registry;
        _options = options.Value;
        _logger = logger;
        _services = services;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var tasks = new List<Task>();

        foreach (var registration in _registry.Registrations)
        {
            var task = ConsumeTopicAsync(registration, stoppingToken);
            tasks.Add(task);
        }

        if (tasks.Count > 0)
        {
            await Task.WhenAll(tasks);
        }
    }

    private async Task ConsumeTopicAsync(
        TopicRegistration registration,
        CancellationToken ct)
    {
        var consumerGroup = registration.ConsumerGroup
            ?? _options.ConsumerGroupOverride
            ?? $"{_options.ApplicationName}.{registration.TopicName}";

        var method = typeof(TalariaHostedService)
            .GetMethod(nameof(ConsumeTopicTypedAsync),
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!
            .MakeGenericMethod(registration.MessageType);

        await (Task)method.Invoke(this, [registration, consumerGroup, ct])!;
    }

    private async Task ConsumeTopicTypedAsync<T>(
        TopicRegistration registration,
        string consumerGroup,
        CancellationToken ct)
    {
        var consumer = await _transport.CreateConsumerAsync<T>(
            registration.TopicName,
            new ConsumerOptions { ConsumerGroup = consumerGroup },
            ct);

        _consumers.Add(consumer);

        _logger.LogInformation(
            "Talaria: consuming topic '{Topic}' (group: {Group}, transport: {Transport})",
            registration.TopicName, consumerGroup, _transport.Name);

        try
        {
            await foreach (var envelope in consumer.ConsumeAsync(ct))
            {
                using var activity = Diagnostics.TalariaDiagnostics.StartConsumerActivity(
                    registration.TopicName, typeof(T).Name, envelope.Headers);

                var sw = System.Diagnostics.Stopwatch.StartNew();

                // Hop count guard
                if (envelope.Headers.HopCount >= _options.MaxHopCount)
                {
                    _logger.LogWarning(
                        "Message on '{Topic}' exceeded max hop count ({HopCount}/{Max}). Routing to DLQ.",
                        registration.TopicName, envelope.Headers.HopCount, _options.MaxHopCount);

                    envelope.Headers.DlqReason = "max_hops_exceeded";
                    
                    Diagnostics.TalariaDiagnostics.DlqRouted.Add(1, 
                        new KeyValuePair<string, object?>("messaging.destination.name", registration.TopicName),
                        new KeyValuePair<string, object?>("messaging.system", "talaria"));

                    await consumer.NackAsync(envelope, ct);
                    continue;
                }

                var idempotencyStore = (IIdempotencyStore?)_services.GetService(typeof(IIdempotencyStore));
                var msgId = envelope.Headers.MessageId;
                IdempotencyLock? idempotencyLock = null;

                try
                {
                    if (idempotencyStore != null && !string.IsNullOrEmpty(msgId))
                    {
                        // Expiration is configurable via options to allow for slow processing without immediate concurrent retry overlaps
                        idempotencyLock = await idempotencyStore.TryAcquireLockAsync(msgId, consumerGroup, _options.IdempotencyLockTtl, ct);

                        if (idempotencyLock is null)
                        {
                            _logger.LogDebug("Message {MessageId} skipped. Idempotency lock claimed by another worker or already completed.", msgId);
                            // We immediately commit the message to suppress further polling!
                            await consumer.CommitAsync(envelope, ct);
                            continue;
                        }
                    }

                    await registration.Handler(envelope.Payload!, envelope.Headers, ct);

                    if (idempotencyLock is not null && idempotencyStore != null)
                    {
                        await idempotencyStore.MarkCompleteAsync(idempotencyLock, ct);
                    }

                    await consumer.CommitAsync(envelope, ct);

                    Diagnostics.TalariaDiagnostics.MessagesConsumed.Add(1, 
                        new KeyValuePair<string, object?>("messaging.destination.name", registration.TopicName));
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex,
                        "Handler for topic '{Topic}' failed. Routing to DLQ.",
                        registration.TopicName);
                        
                    if (idempotencyLock is not null && idempotencyStore != null)
                    {
                        await idempotencyStore.ReleaseLockAsync(idempotencyLock, ct);
                    }
                    
                    envelope.Headers.DlqException = ex.Message;
                    activity?.SetStatus(System.Diagnostics.ActivityStatusCode.Error, ex.Message);
                    
                    Diagnostics.TalariaDiagnostics.MessagesFailed.Add(1, 
                        new KeyValuePair<string, object?>("messaging.destination.name", registration.TopicName));
                    Diagnostics.TalariaDiagnostics.DlqRouted.Add(1, 
                        new KeyValuePair<string, object?>("messaging.destination.name", registration.TopicName));

                    await consumer.NackAsync(envelope, ct);
                }
                finally
                {
                    sw.Stop();
                    Diagnostics.TalariaDiagnostics.ProcessingDuration.Record(sw.Elapsed.TotalMilliseconds, 
                        new KeyValuePair<string, object?>("messaging.destination.name", registration.TopicName));
                }
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            _logger.LogInformation("Talaria: consumer for '{Topic}' shutting down.", registration.TopicName);
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        await base.StopAsync(cancellationToken);

        foreach (var consumer in _consumers)
        {
            await consumer.DisposeAsync();
        }

        _consumers.Clear();
        _logger.LogInformation("Talaria: all consumers disposed.");
    }
}
