using Microsoft.Extensions.Options;

namespace Talaria.Core.Tests;

public class TalariaOptionsValidatorRetryTests
{
    private readonly TalariaOptionsValidator _validator = new();

    [Fact]
    public void Validate_NegativeMaxRetryAttempts_Fails()
    {
        var options = CreateValidOptions();
        options.DefaultRetryPolicy = new RetryPolicy
        {
            MaxRetryAttempts = -1,
            RetryInterval = TimeSpan.FromSeconds(1),
        };

        var result = _validator.Validate(null, options);

        Assert.True(result.Failed);
        Assert.Contains("MaxRetryAttempts", result.FailureMessage, StringComparison.Ordinal);
    }

    [Fact]
    public void Validate_NegativeRetryInterval_Fails()
    {
        var options = CreateValidOptions();
        options.DefaultRetryPolicy = new RetryPolicy
        {
            MaxRetryAttempts = 1,
            RetryInterval = TimeSpan.FromMilliseconds(-1),
        };

        var result = _validator.Validate(null, options);

        Assert.True(result.Failed);
        Assert.Contains("RetryInterval", result.FailureMessage, StringComparison.Ordinal);
    }

    [Fact]
    public void Validate_MaxRetryIntervalLessThanRetryInterval_Fails()
    {
        var options = CreateValidOptions();
        options.DefaultRetryPolicy = new RetryPolicy
        {
            MaxRetryAttempts = 1,
            RetryInterval = TimeSpan.FromSeconds(5),
            MaxRetryInterval = TimeSpan.FromSeconds(1),
        };

        var result = _validator.Validate(null, options);

        Assert.True(result.Failed);
        Assert.Contains("MaxRetryInterval", result.FailureMessage, StringComparison.Ordinal);
    }

    [Fact]
    public void Validate_MaxRetryIntervalEqualToRetryInterval_Passes()
    {
        var options = CreateValidOptions();
        options.DefaultRetryPolicy = new RetryPolicy
        {
            MaxRetryAttempts = 1,
            RetryInterval = TimeSpan.FromSeconds(5),
            MaxRetryInterval = TimeSpan.FromSeconds(5),
        };

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

    [Fact]
    public void Validate_NullDefaultRetryPolicy_Fails()
    {
        var options = CreateValidOptions();
        options.DefaultRetryPolicy = null!;

        var result = _validator.Validate(null, options);

        Assert.True(result.Failed);
        Assert.Contains("DefaultRetryPolicy", result.FailureMessage, StringComparison.Ordinal);
    }

    [Fact]
    public void Validate_PositiveAttemptsWithZeroRetryInterval_Fails()
    {
        var options = CreateValidOptions();
        options.DefaultRetryPolicy = new RetryPolicy
        {
            MaxRetryAttempts = 3,
            RetryInterval = TimeSpan.Zero,
        };

        var result = _validator.Validate(null, options);

        Assert.True(result.Failed);
        Assert.Contains("RetryInterval", result.FailureMessage, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateRetryPolicy_NegativeMaxRetryAttempts_Fails()
    {
        var policy = new RetryPolicy { MaxRetryAttempts = -1, RetryInterval = TimeSpan.FromSeconds(1) };

        var result = TalariaOptionsValidator.ValidateRetryPolicy(policy, "policy");

        Assert.NotNull(result);
        Assert.Contains("MaxRetryAttempts", result.FailureMessage, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateRetryPolicy_PositiveAttemptsWithZeroRetryInterval_Fails()
    {
        var policy = new RetryPolicy { MaxRetryAttempts = 3, RetryInterval = TimeSpan.Zero };

        var result = TalariaOptionsValidator.ValidateRetryPolicy(policy, "policy");

        Assert.NotNull(result);
        Assert.Contains("RetryInterval", result.FailureMessage, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateRetryPolicy_MaxRetryIntervalLessThanRetryInterval_Fails()
    {
        var policy = new RetryPolicy
        {
            MaxRetryAttempts = 3,
            RetryInterval = TimeSpan.FromSeconds(5),
            MaxRetryInterval = TimeSpan.FromSeconds(1),
        };

        var result = TalariaOptionsValidator.ValidateRetryPolicy(policy, "policy");

        Assert.NotNull(result);
        Assert.Contains("MaxRetryInterval", result.FailureMessage, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateRetryPolicy_Valid_Passes()
    {
        var policy = new RetryPolicy
        {
            MaxRetryAttempts = 3,
            RetryInterval = TimeSpan.FromSeconds(1),
            MaxRetryInterval = TimeSpan.FromSeconds(5),
        };

        var result = TalariaOptionsValidator.ValidateRetryPolicy(policy, "policy");

        Assert.Null(result);
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
