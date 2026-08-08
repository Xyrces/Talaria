using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Talaria.Core.Abstractions;
using Talaria.Core.Registration;

namespace Talaria.Core.Hosting;

/// <summary>
/// Background service that manages the lifecycle of all topic consumers.
/// On start: creates a supervised consumer loop for each topic registration and dispatches messages to handlers.
/// On stop: cancels the loops; each consumer is disposed as its loop unwinds.
/// </summary>
public sealed class TalariaHostedService : BackgroundService
{
    private readonly ITransport _transport;
    private readonly TopicRegistry _registry;
    private readonly TalariaOptions _options;
    private readonly ILogger<TalariaHostedService> _logger;
    private readonly IServiceProvider _services;
    private MessageProcessingPipeline? _pipeline;

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
        // The idempotency store is a singleton — resolve it once, not per message.
        var pipeline = new MessageProcessingPipeline(
            _services.GetService<IIdempotencyStore>(),
            _options,
            _logger);
        _pipeline = pipeline;

        // Snapshot the registry before consumers spin up — late registrations are ignored.
        var registrations = _registry.Registrations.ToList();

        var tasks = registrations.Select(registration =>
            ConsumerSupervision.RunSupervisedAsync(
                $"topic:{registration.TopicName}",
                ct => ConsumeTopicAsync(registration, ct),
                _logger,
                stoppingToken)).ToList();

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

        // One-time generic dispatch per consumer (re)creation — not in the per-message hot path.
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
        await using var consumer = await _transport.CreateConsumerAsync<T>(
            registration.TopicName,
            new ConsumerOptions { ConsumerGroup = consumerGroup },
            ct);

        _logger.LogInformation(
            "Talaria: consuming topic '{Topic}' (group: {Group}, transport: {Transport})",
            registration.TopicName, consumerGroup, _transport.Name);

        var pipeline = _pipeline!;

        await foreach (var envelope in consumer.ConsumeAsync(ct))
        {
            using var activity = Diagnostics.TalariaDiagnostics.StartConsumerActivity(
                registration.TopicName, typeof(T).Name, envelope.Headers);

            var sw = System.Diagnostics.Stopwatch.StartNew();

            try
            {
                // Hop count guard
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
                    // We immediately commit the message to suppress further polling!
                    await consumer.CommitAsync(envelope, ct);
                    continue;
                }

                try
                {
                    await registration.Handler(envelope.Payload!, envelope.Headers, ct);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex,
                        "Handler for topic '{Topic}' failed. Routing to DLQ.",
                        registration.TopicName);

                    activity?.SetStatus(System.Diagnostics.ActivityStatusCode.Error, ex.Message);

                    Diagnostics.TalariaDiagnostics.MessagesFailed.Add(1,
                        new KeyValuePair<string, object?>("messaging.destination.name", registration.TopicName));
                    Diagnostics.TalariaDiagnostics.DlqRouted.Add(1,
                        new KeyValuePair<string, object?>("messaging.destination.name", registration.TopicName));

                    await pipeline.FailAsync(gate.Lock, consumer, envelope, ex, null, ct);
                    continue;
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
}
