using Talaria.Core.Hosting;

namespace Talaria.Core.Tests;

public class RetryPolicyDelayTests
{
    [Fact]
    public void ComputeDelay_Fixed_ReturnsRetryInterval()
    {
        var policy = new RetryPolicy
        {
            MaxRetryAttempts = 3,
            RetryInterval = TimeSpan.FromSeconds(2),
            BackoffType = RetryBackoffType.Fixed,
        };

        var delay = RetryCoordinator.ComputeDelay(policy, currentAttempt: 0, TimeSpan.FromMilliseconds(100));

        Assert.Equal(TimeSpan.FromSeconds(2), delay);
    }

    [Theory]
    [InlineData(0, 1000)]
    [InlineData(1, 2000)]
    [InlineData(2, 4000)]
    [InlineData(3, 8000)]
    public void ComputeDelay_Exponential_DoublesEachAttempt(int currentAttempt, long expectedMs)
    {
        var policy = new RetryPolicy
        {
            MaxRetryAttempts = 5,
            RetryInterval = TimeSpan.FromSeconds(1),
            BackoffType = RetryBackoffType.Exponential,
        };

        var delay = RetryCoordinator.ComputeDelay(policy, currentAttempt, TimeSpan.FromMilliseconds(100));

        Assert.Equal(TimeSpan.FromMilliseconds(expectedMs), delay);
    }

    [Fact]
    public void ComputeDelay_Exponential_AppliesMaxRetryIntervalCap()
    {
        var policy = new RetryPolicy
        {
            MaxRetryAttempts = 5,
            RetryInterval = TimeSpan.FromSeconds(1),
            BackoffType = RetryBackoffType.Exponential,
            MaxRetryInterval = TimeSpan.FromSeconds(3),
        };

        var delay = RetryCoordinator.ComputeDelay(policy, currentAttempt: 3, TimeSpan.FromMilliseconds(100));

        Assert.Equal(TimeSpan.FromSeconds(3), delay);
    }

    [Fact]
    public void ComputeDelay_Fixed_BelowMinRetryDelay_IsFloored()
    {
        var policy = new RetryPolicy
        {
            MaxRetryAttempts = 3,
            RetryInterval = TimeSpan.FromMilliseconds(10),
            BackoffType = RetryBackoffType.Fixed,
        };

        var delay = RetryCoordinator.ComputeDelay(policy, currentAttempt: 0, TimeSpan.FromMilliseconds(100));

        Assert.Equal(TimeSpan.FromMilliseconds(100), delay);
    }

    [Fact]
    public void ComputeDelay_Exponential_BelowMinRetryDelay_IsFloored()
    {
        var policy = new RetryPolicy
        {
            MaxRetryAttempts = 5,
            RetryInterval = TimeSpan.FromMilliseconds(10),
            BackoffType = RetryBackoffType.Exponential,
        };

        var delay = RetryCoordinator.ComputeDelay(policy, currentAttempt: 0, TimeSpan.FromMilliseconds(100));

        Assert.Equal(TimeSpan.FromMilliseconds(100), delay);
    }

    [Fact]
    public void ComputeDelay_CappedDelayStillAboveMin_DoesNotFloorToMin()
    {
        var policy = new RetryPolicy
        {
            MaxRetryAttempts = 5,
            RetryInterval = TimeSpan.FromSeconds(1),
            BackoffType = RetryBackoffType.Exponential,
            MaxRetryInterval = TimeSpan.FromSeconds(2),
        };

        var delay = RetryCoordinator.ComputeDelay(policy, currentAttempt: 5, TimeSpan.FromMilliseconds(100));

        Assert.Equal(TimeSpan.FromSeconds(2), delay);
    }
}
