// SPDX-License-Identifier: Apache-2.0

using Microsoft.Extensions.Options;

namespace Talaria.Core.Tests;

public class RequestResponseOptionsValidatorTests
{
    private readonly TalariaOptionsValidator _validator = new();

    [Fact]
    public void Validate_DefaultRequestTimeoutZero_Fails()
    {
        var options = CreateValidOptions();
        options.DefaultRequestTimeout = TimeSpan.Zero;

        var result = _validator.Validate(null, options);

        Assert.True(result.Failed);
        Assert.Contains("DefaultRequestTimeout", result.FailureMessage, StringComparison.Ordinal);
    }

    [Fact]
    public void Validate_DefaultRequestTimeoutNegative_Fails()
    {
        var options = CreateValidOptions();
        options.DefaultRequestTimeout = TimeSpan.FromMilliseconds(-1);

        var result = _validator.Validate(null, options);

        Assert.True(result.Failed);
        Assert.Contains("DefaultRequestTimeout", result.FailureMessage, StringComparison.Ordinal);
    }

    [Fact]
    public void Validate_DefaultRequestTimeoutPositive_Passes()
    {
        var options = CreateValidOptions();
        options.DefaultRequestTimeout = TimeSpan.FromSeconds(30);

        var result = _validator.Validate(null, options);

        Assert.True(result.Succeeded);
    }

    private static TalariaOptions CreateValidOptions()
    {
        return new TalariaOptions
        {
            MaxHopCount = 1,
            MaxDeferralAttempts = 1,
            DeferralBackoff = TimeSpan.FromMilliseconds(100),
            DeferralLeaseTimeout = TimeSpan.FromSeconds(1),
            OutboxLeaseTimeout = TimeSpan.FromSeconds(1),
            OutboxRelayInterval = TimeSpan.FromMilliseconds(100),
            IdempotencyLockTtl = TimeSpan.FromSeconds(1),
            MinRetryDelay = TimeSpan.FromMilliseconds(100),
            DefaultRequestTimeout = TimeSpan.FromSeconds(1),
            DefaultRetryPolicy = new RetryPolicy
            {
                MaxRetryAttempts = 1,
                RetryInterval = TimeSpan.FromSeconds(1),
            },
        };
    }
}
