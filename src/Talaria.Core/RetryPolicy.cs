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
/// Per-topic retry policy. Delayed retries are disabled when
/// <see cref="MaxRetryAttempts"/> is less than or equal to zero OR when
/// <see cref="RetryInterval"/> is less than or equal to <see cref="TimeSpan.Zero"/>;
/// the disabled path preserves the existing immediate-DLQ behavior.
/// </summary>
/// <since>1.0.0</since>
public sealed class RetryPolicy
{
    /// <summary>
    /// Maximum number of delayed retry attempts before the message is routed to the DLQ.
    /// Defaults to <c>0</c> (retries disabled).
    /// </summary>
    public int MaxRetryAttempts { get; init; } = 0;

    /// <summary>
    /// Base delay between retry attempts. Defaults to <see cref="TimeSpan.Zero"/>
    /// (retries disabled). For <see cref="RetryBackoffType.Fixed"/> this is the exact
    /// delay; for <see cref="RetryBackoffType.Exponential"/> it is doubled on each
    /// attempt, optionally capped by <see cref="MaxRetryInterval"/>.
    /// </summary>
    public TimeSpan RetryInterval { get; init; } = TimeSpan.Zero;

    /// <summary>
    /// Backoff shape applied to <see cref="RetryInterval"/> across attempts.
    /// Defaults to <see cref="RetryBackoffType.Fixed"/>.
    /// </summary>
    public RetryBackoffType BackoffType { get; init; } = RetryBackoffType.Fixed;

    /// <summary>
    /// Optional ceiling for the computed retry delay. Ignored when null.
    /// Defaults to <c>null</c>.
    /// </summary>
    public TimeSpan? MaxRetryInterval { get; init; }

    /// <summary>
    /// Returns true when the policy has retries enabled: a positive number of attempts
    /// AND a positive retry interval.
    /// </summary>
    public static bool IsEnabled(RetryPolicy? policy)
        => policy is { MaxRetryAttempts: > 0 } && policy.RetryInterval > TimeSpan.Zero;
}
