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
}
