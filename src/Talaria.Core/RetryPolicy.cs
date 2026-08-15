// SPDX-License-Identifier: Apache-2.0

namespace Talaria.Core;

/// <summary>
/// Describes the backoff shape used by <see cref="RetryPolicy"/>.
/// </summary>
/// <since>1.0.0</since>
public enum RetryBackoffType
{
    /// <summary>A fixed delay between retry attempts.</summary>
    Fixed,

    /// <summary>An exponential delay that doubles on each attempt.</summary>
    Exponential,
}

/// <summary>
/// Per-topic retry policy. Zero attempts means delayed retries are disabled and the
/// existing immediate-DLQ behavior applies.
/// </summary>
/// <since>1.0.0</since>
public sealed class RetryPolicy
{
    /// <summary>
    /// Maximum number of delayed retry attempts before the message is routed to the DLQ.
    /// </summary>
    public int MaxRetryAttempts { get; set; } = 0;

    /// <summary>
    /// Base delay between retry attempts. For <see cref="RetryBackoffType.Fixed"/> this is
    /// the exact delay; for <see cref="RetryBackoffType.Exponential"/> it is doubled on
    /// each attempt, optionally capped by <see cref="MaxRetryInterval"/>.
    /// </summary>
    public TimeSpan RetryInterval { get; set; } = TimeSpan.Zero;

    /// <summary>
    /// Backoff shape applied to <see cref="RetryInterval"/> across attempts.
    /// </summary>
    public RetryBackoffType BackoffType { get; set; } = RetryBackoffType.Fixed;

    /// <summary>
    /// Optional ceiling for the computed retry delay. Ignored when null.
    /// </summary>
    public TimeSpan? MaxRetryInterval { get; set; }
}
