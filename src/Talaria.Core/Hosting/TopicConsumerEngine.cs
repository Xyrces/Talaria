// SPDX-License-Identifier: Apache-2.0

using Microsoft.Extensions.Logging;
using Talaria.Core.Abstractions;
using Talaria.Core.Registration;

namespace Talaria.Core.Hosting;

/// <summary>
/// Host-agnostic engine that runs supervised consumer loops for all topic registrations.
/// </summary>
internal sealed class TopicConsumerEngine
{
    private readonly ITransport _transport;
    private readonly IReadOnlyList<TopicRegistration> _registrations;
    private readonly TalariaOptions _options;
    private readonly IDeferralStore? _deferralStore;
    private readonly MessageProcessingPipeline _pipeline;
    private readonly ILogger _logger;

    public TopicConsumerEngine(
        ITransport transport,
        TopicRegistry registry,
        TalariaOptions options,
        IDeferralStore? deferralStore,
        MessageProcessingPipeline pipeline,
        ILogger logger)
    {
        _transport = transport;
        _registrations = registry.Registrations;
        _options = options;
        _deferralStore = deferralStore;
        _pipeline = pipeline;
        _logger = logger;
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

                try
                {
                    await registration.Handler(envelope.Payload!, envelope.Headers, ct);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex,
                        "Handler for topic '{Topic}' failed. Evaluating delayed retry policy.",
                        registration.TopicName);

                    activity?.SetStatus(System.Diagnostics.ActivityStatusCode.Error, ex.Message);

                    Diagnostics.TalariaDiagnostics.MessagesFailed.Add(1,
                        new KeyValuePair<string, object?>("messaging.destination.name", registration.TopicName));

                    var outcome = await retryCoordinator.TryCoordinateTopicRetryAsync(
                        registration, pipeline, consumer, envelope, ex, gate.Lock, ct);

                    if (outcome == RetryCoordinator.RetryOutcome.NotRetryable)
                    {
                        _logger.LogError(ex,
                            "Handler for topic '{Topic}' failed. Routing to DLQ.",
                            registration.TopicName);

                        Diagnostics.TalariaDiagnostics.DlqRouted.Add(1,
                            new KeyValuePair<string, object?>("messaging.destination.name", registration.TopicName));
                        await pipeline.FailAsync(gate.Lock, consumer, envelope, ex, null, ct);
                    }

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
