// SPDX-License-Identifier: Apache-2.0

using Microsoft.Extensions.Options;

namespace Talaria.Core;

/// <summary>
/// Validates <see cref="TalariaOptions"/> values that would otherwise cause silent
/// message loss or runtime failures (e.g. a zero idempotency lock TTL makes every
/// acquire fail, so messages would be committed without ever being processed).
/// </summary>
internal sealed class TalariaOptionsValidator : IValidateOptions<TalariaOptions>
{
    public ValidateOptionsResult Validate(string? name, TalariaOptions options)
    {
        if (options.MaxHopCount <= 0)
        {
            return ValidateOptionsResult.Fail($"{nameof(TalariaOptions.MaxHopCount)} must be greater than zero.");
        }

        if (options.MaxDeferralAttempts <= 0)
        {
            return ValidateOptionsResult.Fail($"{nameof(TalariaOptions.MaxDeferralAttempts)} must be greater than zero.");
        }

        if (options.IdempotencyLockTtl <= TimeSpan.Zero)
        {
            return ValidateOptionsResult.Fail($"{nameof(TalariaOptions.IdempotencyLockTtl)} must be greater than zero.");
        }

        if (options.DeferralBackoff < TimeSpan.Zero)
        {
            return ValidateOptionsResult.Fail($"{nameof(TalariaOptions.DeferralBackoff)} must not be negative.");
        }

        if (options.DeferralLeaseTimeout <= TimeSpan.Zero)
        {
            return ValidateOptionsResult.Fail($"{nameof(TalariaOptions.DeferralLeaseTimeout)} must be greater than zero.");
        }

        if (options.OutboxLeaseTimeout <= TimeSpan.Zero)
        {
            return ValidateOptionsResult.Fail($"{nameof(TalariaOptions.OutboxLeaseTimeout)} must be greater than zero.");
        }

        if (options.OutboxRelayInterval <= TimeSpan.Zero)
        {
            return ValidateOptionsResult.Fail($"{nameof(TalariaOptions.OutboxRelayInterval)} must be greater than zero.");
        }

        var retryValidation = ValidateRetryPolicy(options.DefaultRetryPolicy, $"{nameof(TalariaOptions.DefaultRetryPolicy)}");
        if (retryValidation != null)
        {
            return retryValidation;
        }

        if (options.MinRetryDelay <= TimeSpan.Zero)
        {
            return ValidateOptionsResult.Fail($"{nameof(TalariaOptions.MinRetryDelay)} must be greater than zero.");
        }

        if (options.DefaultRequestTimeout <= TimeSpan.Zero)
        {
            return ValidateOptionsResult.Fail($"{nameof(TalariaOptions.DefaultRequestTimeout)} must be greater than zero.");
        }

        return ValidateOptionsResult.Success;
    }

    /// <summary>
    /// Validates a <see cref="RetryPolicy"/> instance. Returns null when valid.
    /// </summary>
    /// <param name="policy">The policy to validate.</param>
    /// <param name="path">Property path prefix used in error messages.</param>
    internal static ValidateOptionsResult? ValidateRetryPolicy(RetryPolicy? policy, string path)
    {
        if (policy is null)
        {
            return ValidateOptionsResult.Fail($"{path} must not be null.");
        }

        if (policy.MaxRetryAttempts < 0)
        {
            return ValidateOptionsResult.Fail($"{path}.{nameof(RetryPolicy.MaxRetryAttempts)} must not be negative.");
        }

        if (policy.MaxRetryAttempts > 0 && policy.RetryInterval <= TimeSpan.Zero)
        {
            return ValidateOptionsResult.Fail($"{path}.{nameof(RetryPolicy.RetryInterval)} must be greater than zero when {nameof(RetryPolicy.MaxRetryAttempts)} is greater than zero.");
        }

        if (policy.RetryInterval < TimeSpan.Zero)
        {
            return ValidateOptionsResult.Fail($"{path}.{nameof(RetryPolicy.RetryInterval)} must not be negative.");
        }

        if (policy.MaxRetryInterval.HasValue && policy.MaxRetryInterval.Value < policy.RetryInterval)
        {
            return ValidateOptionsResult.Fail($"{path}.{nameof(RetryPolicy.MaxRetryInterval)} must not be less than {nameof(RetryPolicy.RetryInterval)}.");
        }

        return null;
    }
}
