using System.Reflection;

namespace Talaria.Core;

/// <summary>
/// Global configuration for a Talaria messaging host.
/// </summary>
public sealed class TalariaOptions
{
    /// <summary>
    /// Maximum number of hops a message can take before being routed to the DLQ.
    /// Provides runtime protection against cyclic message loops.
    /// </summary>
    public int MaxHopCount { get; set; } = 32;

    /// <summary>
    /// Maximum number of deferral attempts for saga out-of-order messages
    /// before routing to the DLQ.
    /// </summary>
    public int MaxDeferralAttempts { get; set; } = 5;

    /// <summary>
    /// Base backoff delay for saga message deferrals. Uses exponential backoff.
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
    /// Suffix appended to topic names for dead-letter queues.
    /// </summary>
    public string DlqSuffix { get; set; } = ".dlq";

    /// <summary>
    /// Expiration duration for idempotency processing locks.
    /// Defaults to 2 minutes.
    /// </summary>
    public TimeSpan IdempotencyLockTtl { get; set; } = TimeSpan.FromMinutes(2);
}
