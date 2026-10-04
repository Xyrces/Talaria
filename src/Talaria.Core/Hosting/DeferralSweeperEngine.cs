// SPDX-License-Identifier: Apache-2.0

using Microsoft.Extensions.Logging;
using Talaria.Core.Abstractions;

namespace Talaria.Core.Hosting;

/// <summary>
/// Host-agnostic engine that polls the <see cref="IDeferralStore"/> and republishes due
/// messages. Entries are leased rather than removed so a crash or shutdown mid-sweep
/// never loses a message.
/// </summary>
internal sealed class DeferralSweeperEngine : IAsyncDisposable
{
    private readonly IDeferralStore _deferralStore;
    private readonly ITransport _transport;
    private readonly TalariaOptions _options;
    private readonly ILogger _logger;
    private readonly IFailedMessageStore? _failures;
    private readonly TalariaHealth? _health;
    private readonly ProducerCache _producerCache;

    public DeferralSweeperEngine(
        IDeferralStore deferralStore,
        ITransport transport,
        TalariaOptions options,
        ILogger logger, IFailedMessageStore? failures = null, TalariaHealth? health = null)
    {
        _deferralStore = deferralStore;
        _transport = transport;
        _options = options;
        _logger = logger;
        _failures = failures;
        _health = health;
        _producerCache = new ProducerCache(transport);
    }

    public async Task RunAsync(CancellationToken ct)
    {
        _health?.SetEndpoint("deferrals", false);
        var interval = _options.DeferralBackoff < TimeSpan.FromSeconds(5)
            ? _options.DeferralBackoff
            : TimeSpan.FromSeconds(5);

        while (!ct.IsCancellationRequested)
        {
            IReadOnlyList<LeasedDeferral> due;
            try
            {
                due = await _deferralStore.AcquireDueAsync(
                    DateTimeOffset.UtcNow,
                    _options.DeferralLeaseTimeout,
                    maxBatch: 64,
                    ct);
                _health?.SetEndpoint("deferrals", true);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                _health?.SetEndpoint("deferrals", false, ex.GetType().Name);
                _logger.LogError(ex, "Deferral sweep failed to acquire due messages; retrying next interval.");
                due = Array.Empty<LeasedDeferral>();
            }
            foreach (var leased in due)
            {
                if (leased.IsReacquired)
                {
                    Diagnostics.TalariaDiagnostics.DeferralReacquired.Add(1, new KeyValuePair<string, object?>("messaging.destination.name", leased.Message.Topic));
                }

                await RepublishDeferredAsync(leased, ct);
            }

            await Task.Delay(interval, ct);
        }
    }

    private async Task RepublishDeferredAsync(LeasedDeferral leased, CancellationToken ct)
    {
        Diagnostics.TalariaDiagnostics.DeferralActiveLeases.Add(1);
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
            await _deferralStore.CompleteAsync(leased.Lease, ct);

            Diagnostics.TalariaDiagnostics.DeferralRepublished.Add(1, topicTag);
            Diagnostics.TalariaDiagnostics.DeferralLag.Record(
                Math.Max(0, (DateTimeOffset.UtcNow - message.DueAt).TotalMilliseconds), topicTag);
        }
        catch (System.Text.Json.JsonException) when (_failures is not null)
        {
            // Retain the complete payload before removing an entry that cannot be published.
            await _failures.SaveAsync(new FailedMessage(message.Id, message.Topic, message.MessageType,
                message.PayloadJson, new MessageHeaders(message.Headers), message.PartitionKey,
                "invalid_stored_json", DateTimeOffset.UtcNow), ct);
            await _deferralStore.CompleteAsync(leased.Lease, ct);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _health?.SetEndpoint("deferrals", false, ex.GetType().Name);
            _logger.LogError(ex, "Failed to republish deferred message {Id}; releasing the lease for retry.", message.Id);
            Diagnostics.TalariaDiagnostics.DeferralRepublishFailed.Add(1, topicTag);
            try
            {
                await _deferralStore.AbandonAsync(leased.Lease, DateTimeOffset.UtcNow + _options.DeferralBackoff, ct);
            }
            catch (Exception abandonEx)
            {
                _logger.LogError(abandonEx, "Failed to abandon deferral lease for message {Id}; it will retry when the lease expires.", message.Id);
            }
        }
        finally { Diagnostics.TalariaDiagnostics.DeferralActiveLeases.Add(-1); }
    }

    public async ValueTask DisposeAsync()
    {
        await _producerCache.DisposeAsync();
    }
}
