// SPDX-License-Identifier: Apache-2.0

using Microsoft.Extensions.DependencyInjection;
using Talaria.Core.Abstractions;
using Talaria.Core.Registration;

namespace Talaria.Core.Tests;

public class TopicConsumerRegistrationTests
{
    private class TestMessage { public string Id { get; set; } = ""; }

    private class TestConsumer : ITopicConsumer<TestMessage>
    {
        public Task ConsumeAsync(ConsumeContext<TestMessage> context) => Task.CompletedTask;
    }

    [Fact]
    public void MapTopic_WithClassConsumer_LandsConsumerTypeOnRegistration()
    {
        var registry = new TopicRegistry();

        registry.MapTopic<TestMessage, TestConsumer>("test.topic");

        var reg = Assert.Single(registry.Registrations);
        Assert.Equal(typeof(TestConsumer), reg.ConsumerType);
        Assert.Null(reg.Handler);
    }

    [Fact]
    public void MapTopic_WithClassConsumerAndConsumerGroup_LandsConsumerGroupAndConsumerType()
    {
        var registry = new TopicRegistry();
        var policy = new RetryPolicy { MaxRetryAttempts = 2, RetryInterval = TimeSpan.FromMilliseconds(100) };

        registry.MapTopic<TestMessage, TestConsumer>("test.topic", "cg-1", policy);

        var reg = Assert.Single(registry.Registrations);
        Assert.Equal(typeof(TestConsumer), reg.ConsumerType);
        Assert.Equal("cg-1", reg.ConsumerGroup);
        Assert.Same(policy, reg.RetryPolicy);
        Assert.Null(reg.Handler);
    }

    [Fact]
    public void MapTopic_WithClassConsumerAndRetryPolicy_LandsRetryPolicy()
    {
        var registry = new TopicRegistry();
        var policy = new RetryPolicy { MaxRetryAttempts = 3, RetryInterval = TimeSpan.FromSeconds(1) };

        registry.MapTopic<TestMessage, TestConsumer>("test.topic", policy);

        var reg = Assert.Single(registry.Registrations);
        Assert.Equal(typeof(TestConsumer), reg.ConsumerType);
        Assert.Same(policy, reg.RetryPolicy);
    }

    [Fact]
    public void AddTopicRegistration_NeitherHandlerNorConsumerType_ThrowsArgumentException()
    {
        var registry = new TopicRegistry();

        var ex = Assert.Throws<ArgumentException>(() =>
            TopicRegistryExtensions.AddTopicRegistration(
                registry,
                "test.topic",
                typeof(TestMessage),
                null,
                null,
                null,
                null));

        Assert.Contains("delegate handler", ex.Message);
    }

    [Fact]
    public void MapTopic_ClassConsumer_BothHandlerAndConsumerType_ThrowsArgumentException()
    {
        var registry = new TopicRegistry();

        var ex = Assert.Throws<ArgumentException>(() =>
            TopicRegistryExtensions.AddTopicRegistration(
                registry,
                "test.topic",
                typeof(TestMessage),
                null,
                null,
                typeof(TestConsumer),
                (_, _, _, _) => Task.CompletedTask));

        Assert.Contains("both", ex.Message);
    }

    [Fact]
    public void IServiceProvider_MapTopic_ClassConsumer_Unregistered_ThrowsInvalidOperationException()
    {
        var services = new ServiceCollection()
            .AddSingleton<TopicRegistry>()
            .BuildServiceProvider();

        var ex = Assert.Throws<InvalidOperationException>(() =>
            services.MapTopic<TestMessage, TestConsumer>("test.topic"));

        Assert.Contains(typeof(TestConsumer).FullName!, ex.Message);
        Assert.Contains("Register it before calling MapTopic", ex.Message);
    }

    [Fact]
    public void IServiceProvider_MapTopic_ClassConsumer_Registered_DoesNotThrow()
    {
        var services = new ServiceCollection()
            .AddSingleton<TopicRegistry>()
            .AddScoped<TestConsumer>()
            .BuildServiceProvider();

        services.MapTopic<TestMessage, TestConsumer>("test.topic");

        var registry = services.GetRequiredService<TopicRegistry>();
        var reg = Assert.Single(registry.Registrations);
        Assert.Equal(typeof(TestConsumer), reg.ConsumerType);
    }

    [Fact]
    public void TopicRegistry_Seal_Blocks_LateClassRegistration()
    {
        var registry = new TopicRegistry();
        registry.Seal();

        var ex = Assert.Throws<InvalidOperationException>(() =>
            registry.MapTopic<TestMessage, TestConsumer>("late.topic"));

        Assert.Contains("MapTopic", ex.Message);
        Assert.Contains("before the host runs", ex.Message);
    }
}
