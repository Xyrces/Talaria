// SPDX-License-Identifier: AGPL-3.0-or-later

using System.Collections.Generic;
using Talaria.Core.Abstractions;

namespace Talaria.Transports.AzureServiceBus.Deferral;

/// <summary>
/// Adapter that splits <see cref="IDeferralStore.EnqueueAsync"/> calls into two paths:
/// <list type="bullet">
///   <item>
///     <b>Short/medium deferrals</b> \u2014 the wait is &lt;= <see cref="DeferralAdapterOptions.ShortTermCutoff"/>
///     AND the JSON payload fits within <see cref="DeferralAdapterOptions.MaxPayloadBytes"/>. The adapter
///     hands the message directly to Azure Service Bus via <see cref="Azure.Messaging.ServiceBus.ServiceBusMessage.ScheduledEnqueueTime"/>
///     so the broker holds and then publishes it. Nothing is stored in the durable
///     store, so acquire/complete/abandon on the short path are no-ops with respect to
///     the broker's scheduled queue.
///   </item>
///   <item>
///     <b>Long/deadline deferrals</b> \u2014 everything else. The adapter passes the entry
///     through to the durable <see cref="IDeferralStore"/> supplied at construction.
///     The saga hosted service sweeper republishes those entries via the regular
///     <see cref="Talaria.Core.Abstractions.IProducer{T}"/> exactly as before the adapter existed.
///   </item>
/// </list>
/// This split lets the engine stay agnostic about how a deferral is realised while
/// still letting the ASB transport turn the common, fast backoffs into broker-side
/// holds (no client polling, no leased sweep).
/// </summary>
/// <remarks>
/// The adapter implements <see cref="IDeferralStore"/> directly so registration in
/// the DI container is the same one-liner the engine already uses for any other
/// <c>IDeferralStore</c>. <see cref="AcquireDueAsync"/>, <see cref="CompleteAsync"/>,
/// and <see cref="AbandonAsync"/> are pure pass-throughs to the durable backing
/// store \u2014 the broker's own scheduled-queue holds short-term entries, so the sweeper
/// only ever sees long-term ones. Pairing the adapter with <c>UseInMemoryDeferralStore()</c>
/// or <c>UseRedisDeferralStore()</c> works without engine changes.
/// </remarks>
/// <since>1.0.0</since>
public sealed class DeferralAdapter : IDeferralStore
{
    private readonly IServiceBusMessageScheduler _scheduler;
    private readonly IDeferralStore _longTermStore;
    private readonly DeferralAdapterOptions _adapterOptions;
    private readonly TimeProvider _clock;

    /// <summary>
    /// Constructs the adapter. <paramref name="longTermStore"/> must not be null \u2014 the
    /// adapter composes it for any deferral that does not fit the short-term window.
    /// </summary>
    /// <param name="scheduler">Broker-side send/schedule seam.</param>
    /// <param name="longTermStore">Durable store for long/deadline deferrals and for all lease acquire/complete/abandon flows.</param>
    /// <param name="adapterOptions">Routing thresholds.</param>
    /// <param name="clock">Time provider \u2014 injected so tests can pin "now".</param>
    internal DeferralAdapter(
        IServiceBusMessageScheduler scheduler,
        IDeferralStore longTermStore,
        DeferralAdapterOptions adapterOptions,
        TimeProvider? clock = null)
    {
        ArgumentNullException.ThrowIfNull(scheduler);
        ArgumentNullException.ThrowIfNull(longTermStore);
        ArgumentNullException.ThrowIfNull(adapterOptions);

        _scheduler = scheduler;
        _longTermStore = longTermStore;
        _adapterOptions = adapterOptions;
        _clock = clock ?? TimeProvider.System;
    }

    /// <inheritdoc />
    public async Task EnqueueAsync(DeferredMessage message, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(message);

        if (ShouldScheduleBroker(message))
        {
            await ScheduleShortTermAsync(message, ct).ConfigureAwait(false);
        }
        else
        {
            await _longTermStore.EnqueueAsync(message, ct).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public Task<IReadOnlyList<LeasedDeferral>> AcquireDueAsync(
        DateTimeOffset now,
        TimeSpan leaseDuration,
        int maxBatch,
        CancellationToken ct = default)
        => _longTermStore.AcquireDueAsync(now, leaseDuration, maxBatch, ct);

    /// <inheritdoc />
    public Task<bool> CompleteAsync(DeferralLease lease, CancellationToken ct = default)
        => _longTermStore.CompleteAsync(lease, ct);

    /// <inheritdoc />
    public Task<bool> AbandonAsync(DeferralLease lease, DateTimeOffset? visibleAt = null, CancellationToken ct = default)
        => _longTermStore.AbandonAsync(lease, visibleAt, ct);

    private bool ShouldScheduleBroker(DeferredMessage message)
    {
        // Lower bound (cutoff) gates how far in the future the entry may be.
        var wait = message.DueAt - _clock.GetUtcNow();
        if (wait > _adapterOptions.ShortTermCutoff)
        {
            return false;
        }

        // Size threshold: ASB Standard tier tops out at 256KB per message. The adapter
        // measures the JSON byte size via UTF-8 to match how ASB counts on the wire.
        var payloadBytes = System.Text.Encoding.UTF8.GetByteCount(message.PayloadJson ?? string.Empty);
        if (payloadBytes > _adapterOptions.MaxPayloadBytes)
        {
            return false;
        }

        return true;
    }

    private async Task ScheduleShortTermAsync(DeferredMessage message, CancellationToken ct)
    {
        var properties = new Dictionary<string, object>(StringComparer.Ordinal);
        foreach (var kvp in message.Headers)
        {
            // ASB ApplicationProperties only accepts non-null values; null-value
            // headers are an engine convention (e.g. a soft "no correlation id yet")
            // and we drop them rather than coerce an empty string.
            if (kvp.Value is null)
            {
                continue;
            }

            properties[kvp.Key] = kvp.Value;
        }

        var body = BinaryData.FromString(message.PayloadJson ?? string.Empty);

        await _scheduler
            .ScheduleAsync(message.Topic, body, properties, message.DueAt, ct)
            .ConfigureAwait(false);
    }
}
