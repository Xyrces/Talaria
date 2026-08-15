// SPDX-License-Identifier: Apache-2.0

using System.Reflection;

namespace Talaria.Core;

/// <summary>
/// Global configuration for a Talaria messaging host.
/// </summary>
/// <since>1.0.0</since>
public sealed class TalariaOptions
{
    /// <summary>
    /// Maximum number of hops a message can take before being routed to the DLQ.
    /// Provides runtime protection against cyclic message loops. Must be greater than zero.
    /// </summary>
    public int MaxHopCount { get; set; } = 32;

    /// <summary>
    /// Maximum number of deferral attempts for saga out-of-order messages
    /// before routing to the DLQ. Must be greater than zero.
    /// </summary>
    public int MaxDeferralAttempts { get; set; } = 5;

    /// <summary>
    /// Base backoff delay for saga message deferrals.
    /// Uses linear backoff: the delay is this value multiplied by the attempt number.
    /// </summary>
    public TimeSpan DeferralBackoff { get; set; } = TimeSpan.FromMilliseconds(100);

    /// <summary>
    /// How long a swept deferral stays leased (hidden from other sweepers) while it is
    /// being republished. If the sweeper crashes before completing, the entry becomes
    /// acquirable again after this duration. Must be greater than zero.
    /// </summary>
    public TimeSpan DeferralLeaseTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// How long an outbox entry stays leased (hidden from other relays) while it is
    /// being published. If the relay crashes before completing, the entry becomes
    /// acquirable again after this duration. Must be greater than zero.
    /// </summary>
    public TimeSpan OutboxLeaseTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Poll interval of the transactional outbox relay when no entries are pending.
    /// Lower values reduce the latency the outbox adds to saga dispatches; higher
    /// values reduce load on the store. Must be greater than zero.
    /// </summary>
    public TimeSpan OutboxRelayInterval { get; set; } = TimeSpan.FromMilliseconds(250);

    /// <summary>
    /// Application name used for consumer group auto-generation and DLQ naming.
    /// Defaults to the entry assembly name.
    /// </summary>
    public string ApplicationName { get; set; } =
        Assembly.GetEntryAssembly()?.GetName().Name ?? "talaria";

    /// <summary>
    /// When set, overrides the auto-generated consumer group for all topics.
    /// </summary>
    public string? ConsumerGroupOverride { get; set; }

    /// <summary>
    /// Expiration duration for idempotency processing locks.
    /// Defaults to 2 minutes. Must be greater than zero — a zero/negative TTL causes
    /// every acquire to fail and messages to be skipped without processing.
    /// </summary>
    public TimeSpan IdempotencyLockTtl { get; set; } = TimeSpan.FromMinutes(2);

    /// <summary>
    /// When false (default), DLQ messages carry a generic exception note instead of the
    /// raw exception message, which may contain sensitive payload data. Enable in
    /// non-production environments for easier debugging.
    /// </summary>
    public bool IncludeExceptionDetailsInDlq { get; set; }

    /// <summary>
    /// Default retry policy applied to all topic handlers and saga steps that do not
    /// declare their own. The default value has zero attempts, which preserves the
    /// legacy immediate-DLQ behavior.
    /// </summary>
    public RetryPolicy DefaultRetryPolicy { get; set; } = new RetryPolicy();

    /// <summary>
    /// Hard floor for any computed retry delay. Prevents sub-millisecond delays from
    /// spinning the sweeper. Must be greater than zero.
    /// </summary>
    public TimeSpan MinRetryDelay { get; set; } = TimeSpan.FromMilliseconds(100);

}
