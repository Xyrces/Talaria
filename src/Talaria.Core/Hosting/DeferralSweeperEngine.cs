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
    private readonly ProducerCache _producerCache;

    public DeferralSweeperEngine(
        IDeferralStore deferralStore,
        ITransport transport,
        TalariaOptions options,
        ILogger logger)
    {
        _deferralStore = deferralStore;
        _transport = transport;
        _options = options;
        _logger = logger;
        _producerCache = new ProducerCache(transport);
    }

    public async Task RunAsync(CancellationToken ct)
    {
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
                    Diagnostics.TalariaDiagnostics.DeferralReacquired.Add(1, new KeyValuePair<string, object?>("messaging.destination.name", leased.Message.Topic));
                }

                await RepublishDeferredAsync(leased, ct);
            }

            await Task.Delay(interval, ct);
        }
    }

    private async Task RepublishDeferredAsync(LeasedDeferral leased, CancellationToken ct)
    {
        var message = leased.Message;
        var topicTag = new KeyValuePair<string, object?>("messaging.destination.name", message.Topic);
        try
        {
            var type = Type.GetType(message.MessageType);
            if (type is null)
            {
                _logger.LogError("Deferred message {Id} has unresolvable payload type '{MessageType}'; dropping.", message.Id, message.MessageType);
                await _deferralStore.CompleteAsync(leased.Lease, ct);
                Diagnostics.TalariaDiagnostics.DeferralActiveLeases.Add(-1);
                return;
            }

            var payload = System.Text.Json.JsonSerializer.Deserialize(message.PayloadJson, type)
                ?? throw new System.Text.Json.JsonException($"Deferred payload deserialized to null for {type.Name}.");

            var invoker = await _producerCache.GetOrCreateAsync(message.Topic, type, ct);
            await invoker.Produce(payload, new MessageHeaders(message.Headers), message.PartitionKey, ct);

            await _deferralStore.CompleteAsync(leased.Lease, ct);

            Diagnostics.TalariaDiagnostics.DeferralRepublished.Add(1, topicTag);
            Diagnostics.TalariaDiagnostics.DeferralLag.Record(
                Math.Max(0, (DateTimeOffset.UtcNow - message.DueAt).TotalMilliseconds), topicTag);
            Diagnostics.TalariaDiagnostics.DeferralActiveLeases.Add(-1);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to republish deferred message {Id}; releasing the lease for retry.", message.Id);
            Diagnostics.TalariaDiagnostics.DeferralRepublishFailed.Add(1, topicTag);
            Diagnostics.TalariaDiagnostics.DeferralActiveLeases.Add(-1);
            try
            {
                await _deferralStore.AbandonAsync(leased.Lease, DateTimeOffset.UtcNow + _options.DeferralBackoff, ct);
            }
            catch (Exception abandonEx)
            {
                _logger.LogError(abandonEx, "Failed to abandon deferral lease for message {Id}; it will retry when the lease expires.", message.Id);
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _producerCache.DisposeAsync();
    }
}
