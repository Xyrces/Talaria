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
    private readonly IFailedMessageStore? _failures;
    private readonly TalariaHealth? _health;
    private readonly ProducerCache _producerCache;

    public OutboxRelayEngine(
        IOutboxStore outboxStore,
        ITransport transport,
        TalariaOptions options,
        ILogger logger, IFailedMessageStore? failures = null, TalariaHealth? health = null)
    {
        _outboxStore = outboxStore;
        _transport = transport;
        _options = options;
        _logger = logger;
        _failures = failures;
        _health = health;
        _producerCache = new ProducerCache(transport);
    }

    public async Task RunAsync(CancellationToken ct)
    {
        _health?.SetEndpoint("outbox", false);
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
                _health?.SetEndpoint("outbox", true);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                _health?.SetEndpoint("outbox", false, ex.GetType().Name);
                _logger.LogError(ex, "Outbox relay failed to acquire pending messages; retrying next interval.");
                pending = Array.Empty<LeasedOutboxMessage>();
            }
            foreach (var leased in pending)
            {
                if (leased.IsReacquired)
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
        Diagnostics.TalariaDiagnostics.OutboxActiveLeases.Add(1);
        var message = leased.Message;
        var topicTag = new KeyValuePair<string, object?>("messaging.destination.name", message.Topic);
        try
        {
            // Stored JSON can be forwarded after a deployment without loading its old CLR assembly.
            var payload = System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(message.PayloadJson);
            var headers = new MessageHeaders(message.Headers);
            if (!headers.ContainsKey(MessageHeaders.MessageTypeKey))
                headers[MessageHeaders.MessageTypeKey] = message.MessageType.Split(',')[0];
            var invoker = await _producerCache.GetOrCreateAsync(message.Topic, typeof(System.Text.Json.JsonElement), ct);
            await invoker.Produce(payload, headers, message.PartitionKey, ct);
            await _outboxStore.CompleteAsync(leased.Lease, ct);

            Diagnostics.TalariaDiagnostics.OutboxPublished.Add(1, topicTag);
            Diagnostics.TalariaDiagnostics.OutboxLag.Record(
                Math.Max(0, (DateTimeOffset.UtcNow - message.CreatedAt).TotalMilliseconds), topicTag);
        }
        catch (System.Text.Json.JsonException) when (_failures is not null)
        {
            // Retain the complete payload before removing an entry that cannot be published.
            await _failures.SaveAsync(new FailedMessage(message.Id, message.Topic, message.MessageType,
                message.PayloadJson, new MessageHeaders(message.Headers), message.PartitionKey,
                "invalid_stored_json", DateTimeOffset.UtcNow), ct);
            await _outboxStore.CompleteAsync(leased.Lease, ct);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _health?.SetEndpoint("outbox", false, ex.GetType().Name);
            _logger.LogError(ex, "Failed to publish outbox message {Id}; releasing the lease for retry.", message.Id);
            Diagnostics.TalariaDiagnostics.OutboxPublishFailed.Add(1, topicTag);
            try
            {
                await _outboxStore.AbandonAsync(leased.Lease, DateTimeOffset.UtcNow + _options.OutboxRelayInterval, ct);
            }
            catch (Exception abandonEx)
            {
                _logger.LogError(abandonEx, "Failed to abandon outbox lease for message {Id}; it will retry when the lease expires.", message.Id);
            }
        }
        finally { Diagnostics.TalariaDiagnostics.OutboxActiveLeases.Add(-1); }
    }

    public async ValueTask DisposeAsync()
    {
        await _producerCache.DisposeAsync();
    }
}
