// SPDX-License-Identifier: Apache-2.0

using Microsoft.Extensions.Logging;
using Talaria.Core.Abstractions;

namespace Talaria.Core.Hosting;

/// <summary>
/// Host-agnostic engine that publishes staged outbox entries to the transport.
/// Entries are leased rather than removed so a crash or shutdown mid-publish never
/// loses a staged message.
/// </summary>
internal sealed class OutboxRelayEngine : IAsyncDisposable
{
    private readonly IOutboxStore _outboxStore;
    private readonly ITransport _transport;
    private readonly TalariaOptions _options;
    private readonly ILogger _logger;
    private readonly ProducerCache _producerCache;

    public OutboxRelayEngine(
        IOutboxStore outboxStore,
        ITransport transport,
        TalariaOptions options,
        ILogger logger)
    {
        _outboxStore = outboxStore;
        _transport = transport;
        _options = options;
        _logger = logger;
        _producerCache = new ProducerCache(transport);
    }

    public async Task RunAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            IReadOnlyList<LeasedOutboxMessage> pending;
            try
            {
                pending = await _outboxStore.AcquirePendingAsync(
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
                    Diagnostics.TalariaDiagnostics.OutboxReacquired.Add(1, new KeyValuePair<string, object?>("messaging.destination.name", leased.Message.Topic));
                }

                await PublishOutboxAsync(leased, ct);
            }

            if (pending.Count == 0)
            {
                await Task.Delay(_options.OutboxRelayInterval, ct);
            }
        }
    }

    private async Task PublishOutboxAsync(LeasedOutboxMessage leased, CancellationToken ct)
    {
        var message = leased.Message;
        var topicTag = new KeyValuePair<string, object?>("messaging.destination.name", message.Topic);
        try
        {
            var type = Type.GetType(message.MessageType);
            if (type is null)
            {
                _logger.LogError("Outbox message {Id} has unresolvable payload type '{MessageType}'; dropping.", message.Id, message.MessageType);
                await _outboxStore.CompleteAsync(leased.Lease, ct);
                Diagnostics.TalariaDiagnostics.OutboxActiveLeases.Add(-1);
                return;
            }

            var payload = System.Text.Json.JsonSerializer.Deserialize(message.PayloadJson, type)
                ?? throw new System.Text.Json.JsonException($"Outbox payload deserialized to null for {type.Name}.");

            var invoker = await _producerCache.GetOrCreateAsync(message.Topic, type, ct);
            await invoker.Produce(payload, new MessageHeaders(message.Headers), message.PartitionKey, ct);

            await _outboxStore.CompleteAsync(leased.Lease, ct);

            Diagnostics.TalariaDiagnostics.OutboxPublished.Add(1, topicTag);
            Diagnostics.TalariaDiagnostics.OutboxLag.Record(
                Math.Max(0, (DateTimeOffset.UtcNow - message.CreatedAt).TotalMilliseconds), topicTag);
            Diagnostics.TalariaDiagnostics.OutboxActiveLeases.Add(-1);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to publish outbox message {Id}; releasing the lease for retry.", message.Id);
            Diagnostics.TalariaDiagnostics.OutboxPublishFailed.Add(1, topicTag);
            Diagnostics.TalariaDiagnostics.OutboxActiveLeases.Add(-1);
            try
            {
                await _outboxStore.AbandonAsync(leased.Lease, DateTimeOffset.UtcNow + _options.OutboxRelayInterval, ct);
            }
            catch (Exception abandonEx)
            {
                _logger.LogError(abandonEx, "Failed to abandon outbox lease for message {Id}; it will retry when the lease expires.", message.Id);
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _producerCache.DisposeAsync();
    }
}
