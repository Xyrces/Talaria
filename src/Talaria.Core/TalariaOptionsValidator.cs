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

        return ValidateOptionsResult.Success;
    }
}
