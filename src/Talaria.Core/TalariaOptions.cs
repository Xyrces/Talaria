using System.Reflection;

namespace Talaria.Core;

/// <summary>
/// Global configuration for a Talaria messaging host.
/// </summary>
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
}
