// SPDX-License-Identifier: AGPL-3.0-or-later

namespace Talaria.Transports.AzureServiceBus.Deferral;

/// <summary>
/// Tuning for <see cref="DeferralAdapter"/>. The adapter is a routing-only shim that
/// decides whether a deferral is scheduled broker-side (short/medium) or persisted
/// in the durable <see cref="Talaria.Core.Abstractions.IDeferralStore"/> (long/deadline); the only
/// knobs are the cutoff and the payload size threshold.
/// </summary>
/// <since>1.0.0</since>
public sealed class DeferralAdapterOptions
{
    /// <summary>
    /// Maximum wait between <see cref="Talaria.Core.Abstractions.DeferredMessage.DueAt"/> and the
    /// present for which <see cref="DeferralAdapter"/> will route the deferral through
    /// Azure Service Bus's native <c>ScheduledEnqueueTime</c>. Defaults to ten minutes,
    /// which comfortably fits ASB's scheduled-message horizon (the broker accepts
    /// schedules well beyond ten minutes; the cutoff is purely a routing decision).
    /// Past-dated entries (DueAt &lt; now) are still eligible for the short path because
    /// ASB treats them as immediate publishes.
    /// </summary>
    public TimeSpan ShortTermCutoff { get; set; } = TimeSpan.FromMinutes(10);

    /// <summary>
    /// Maximum payload size (in bytes) the adapter is willing to schedule as a
    /// short-term deferral. Larger payloads always fall through to the durable store
    /// so the broker never rejects an oversize message \u2014 the durable sweeper paths
    /// inherits the same JSON-byte size limit when it republishes via
    /// <see cref="Talaria.Core.Abstractions.IProducer{T}"/>, but the host gets to choose where
    /// (short store vs long store) based on this threshold. Defaults to 256 KB (the
    /// ASB Standard tier per-message limit).
    /// </summary>
    public int MaxPayloadBytes { get; set; } = 256 * 1024;
}
