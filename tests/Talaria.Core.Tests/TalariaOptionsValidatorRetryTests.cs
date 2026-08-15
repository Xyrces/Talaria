using Microsoft.Extensions.Options;

namespace Talaria.Core.Tests;

public class TalariaOptionsValidatorRetryTests
{
    private readonly TalariaOptionsValidator _validator = new();

    [Fact]
    public void Validate_NegativeMaxRetryAttempts_Fails()
    {
        var options = CreateValidOptions();
        options.DefaultRetryPolicy.MaxRetryAttempts = -1;

        var result = _validator.Validate(null, options);

        Assert.True(result.Failed);
        Assert.Contains("MaxRetryAttempts", result.FailureMessage, StringComparison.Ordinal);
    }

    [Fact]
    public void Validate_NegativeRetryInterval_Fails()
    {
        var options = CreateValidOptions();
        options.DefaultRetryPolicy.RetryInterval = TimeSpan.FromMilliseconds(-1);

        var result = _validator.Validate(null, options);

        Assert.True(result.Failed);
        Assert.Contains("RetryInterval", result.FailureMessage, StringComparison.Ordinal);
    }

    [Fact]
    public void Validate_MaxRetryIntervalLessThanRetryInterval_Fails()
    {
        var options = CreateValidOptions();
        options.DefaultRetryPolicy.RetryInterval = TimeSpan.FromSeconds(5);
        options.DefaultRetryPolicy.MaxRetryInterval = TimeSpan.FromSeconds(1);

        var result = _validator.Validate(null, options);

        Assert.True(result.Failed);
        Assert.Contains("MaxRetryInterval", result.FailureMessage, StringComparison.Ordinal);
    }

    [Fact]
    public void Validate_MaxRetryIntervalEqualToRetryInterval_Passes()
    {
        var options = CreateValidOptions();
        options.DefaultRetryPolicy.RetryInterval = TimeSpan.FromSeconds(5);
        options.DefaultRetryPolicy.MaxRetryInterval = TimeSpan.FromSeconds(5);

        var result = _validator.Validate(null, options);

        Assert.True(result.Succeeded);
    }

    [Fact]
    public void Validate_MinRetryDelayZero_Fails()
    {
        var options = CreateValidOptions();
        options.MinRetryDelay = TimeSpan.Zero;

        var result = _validator.Validate(null, options);

        Assert.True(result.Failed);
        Assert.Contains("MinRetryDelay", result.FailureMessage, StringComparison.Ordinal);
    }

    [Fact]
    public void Validate_MinRetryDelayNegative_Fails()
    {
        var options = CreateValidOptions();
        options.MinRetryDelay = TimeSpan.FromMilliseconds(-1);

        var result = _validator.Validate(null, options);

        Assert.True(result.Failed);
        Assert.Contains("MinRetryDelay", result.FailureMessage, StringComparison.Ordinal);
    }

    [Fact]
    public void Validate_DisabledRetryPolicy_Passes()
    {
        var options = CreateValidOptions();
        options.DefaultRetryPolicy = new RetryPolicy();

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
            DefaultRetryPolicy = new RetryPolicy
            {
                MaxRetryAttempts = 1,
                RetryInterval = TimeSpan.FromSeconds(1),
            },
        };
    }
}
