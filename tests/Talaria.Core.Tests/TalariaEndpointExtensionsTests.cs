using Microsoft.Extensions.DependencyInjection;
using Talaria.Core.Abstractions;
using Talaria.Core.Registration;

namespace Talaria.Core.Tests;

public class TalariaEndpointExtensionsTests
{
    private IServiceProvider CreateProvider()
    {
        var services = new ServiceCollection();
        services.AddSingleton<TopicRegistry>();
        return services.BuildServiceProvider();
    }

    [Fact]
    public async Task MapTopic_ShouldAddRegistration()
    {
        var provider = CreateProvider();
        bool called = false;
        
        provider.MapTopic<string>("test", (m, c) => 
        {
            called = true;
            return Task.CompletedTask;
        });

        var registry = provider.GetRequiredService<TopicRegistry>();
        var reg = registry.Registrations.First();
        
        Assert.Equal("test", reg.TopicName);
        Assert.Equal(typeof(string), reg.MessageType);
        
        await reg.Handler("payload", new MessageHeaders(), CancellationToken.None);
        Assert.True(called);
    }

    [Fact]
    public async Task MapTopicWithEnvelope_ShouldCreateEnvelopeAndDelegate()
    {
        var provider = CreateProvider();
        MessageEnvelope<string>? received = null;
        
        provider.MapTopicWithEnvelope<string>("test-env", (e, c) => 
        {
            received = e;
            return Task.CompletedTask;
        });

        var registry = provider.GetRequiredService<TopicRegistry>();
        var reg = registry.Registrations.First();
        
        var headers = new MessageHeaders();
        headers["some-key"] = "test";
        await reg.Handler("hello", headers, CancellationToken.None);
        
        Assert.NotNull(received);
        Assert.Equal("hello", received!.Payload);
        Assert.Equal("test", received.Headers["some-key"]);
        Assert.Equal("test-env", received.SourceTopic);
    }

    [Fact]
    public async Task MapTopic_Sync_ShouldAddRegistration()
    {
        var provider = CreateProvider();
        bool called = false;
        
        provider.MapTopic<string>("test-sync", m => 
        {
            called = true;
        });

        var registry = provider.GetRequiredService<TopicRegistry>();
        var reg = registry.Registrations.First();
        
        await reg.Handler("payload", new MessageHeaders(), CancellationToken.None);
        Assert.True(called);
    }

    [Fact]
    public void MapTopic_WithRetryPolicy_LandsOnRegistration()
    {
        var provider = CreateProvider();
        var policy = new RetryPolicy { MaxRetryAttempts = 3, RetryInterval = TimeSpan.FromSeconds(1) };

        provider.MapTopic<string>("test-retry", (m, c) => Task.CompletedTask, policy);

        var registry = provider.GetRequiredService<TopicRegistry>();
        var reg = registry.Registrations.First();
        Assert.Same(policy, reg.RetryPolicy);
    }

    [Fact]
    public void MapTopic_WithConsumerGroupAndRetryPolicy_LandsOnRegistration()
    {
        var provider = CreateProvider();
        var policy = new RetryPolicy { MaxRetryAttempts = 2, RetryInterval = TimeSpan.FromMilliseconds(100) };

        provider.MapTopic<string>("test-retry-cg", "cg-1", (m, c) => Task.CompletedTask, policy);

        var registry = provider.GetRequiredService<TopicRegistry>();
        var reg = registry.Registrations.First();
        Assert.Equal("cg-1", reg.ConsumerGroup);
        Assert.Same(policy, reg.RetryPolicy);
    }

    [Fact]
    public void MapTopicWithEnvelope_WithRetryPolicy_LandsOnRegistration()
    {
        var provider = CreateProvider();
        var policy = new RetryPolicy { MaxRetryAttempts = 1, RetryInterval = TimeSpan.FromMinutes(1) };

        provider.MapTopicWithEnvelope<string>("test-retry-env", (e, c) => Task.CompletedTask, policy);

        var registry = provider.GetRequiredService<TopicRegistry>();
        var reg = registry.Registrations.First();
        Assert.Same(policy, reg.RetryPolicy);
    }

    [Fact]
    public void MapTopic_Sync_WithRetryPolicy_LandsOnRegistration()
    {
        var provider = CreateProvider();
        var policy = new RetryPolicy { MaxRetryAttempts = 4, RetryInterval = TimeSpan.FromSeconds(5) };

        provider.MapTopic<string>("test-retry-sync", m => { }, policy);

        var registry = provider.GetRequiredService<TopicRegistry>();
        var reg = registry.Registrations.First();
        Assert.Same(policy, reg.RetryPolicy);
    }

    [Theory]
    [InlineData(-1, 1000, null)]
    [InlineData(1, 0, null)]
    [InlineData(1, 1000, 500)]
    public void MapTopic_WithInvalidRetryPolicy_ThrowsArgumentException(int maxAttempts, int intervalMs, int? maxIntervalMs)
    {
        var provider = CreateProvider();
        var policy = new RetryPolicy
        {
            MaxRetryAttempts = maxAttempts,
            RetryInterval = TimeSpan.FromMilliseconds(intervalMs),
            MaxRetryInterval = maxIntervalMs.HasValue ? TimeSpan.FromMilliseconds(maxIntervalMs.Value) : null,
        };

        var ex = Assert.Throws<ArgumentException>(() =>
            provider.MapTopic<string>("test-retry", (m, c) => Task.CompletedTask, policy));

        Assert.Equal("retryPolicy", ex.ParamName);
    }
}
