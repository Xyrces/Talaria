// SPDX-License-Identifier: Apache-2.0

using Talaria.Core.Abstractions;
using Talaria.Core.Registration;

namespace Talaria.Core.Tests;

public class RequestResponseRegistrationTests
{
    private class Ping { public string Value { get; set; } = ""; }
    private class Pong { public string Echo { get; set; } = ""; }

    private class PingConsumer : IRequestConsumer<Ping, Pong>
    {
        public Task<Pong> ConsumeAsync(ConsumeContext<Ping> context, CancellationToken ct = default)
            => Task.FromResult(new Pong { Echo = context.Message.Value });
    }

    [Fact]
    public void MapRequest_WithDelegate_LandsRequestHandlerAndResponseType()
    {
        var registry = new TopicRegistry();

        registry.MapRequest<Ping, Pong>("ping.topic", async (msg, _, _, ct) => new Pong { Echo = msg.Value });

        var reg = Assert.Single(registry.Registrations);
        Assert.NotNull(reg.RequestHandler);
        Assert.Equal(typeof(Pong), reg.ResponseType);
        Assert.Null(reg.Handler);
        Assert.Null(reg.ConsumerType);
        Assert.Null(reg.RequestConsumerType);
    }

    [Fact]
    public void MapRequest_WithClassConsumer_LandsRequestConsumerTypeAndResponseType()
    {
        var registry = new TopicRegistry();

        registry.MapRequest<Ping, PingConsumer, Pong>("ping.topic");

        var reg = Assert.Single(registry.Registrations);
        Assert.Equal(typeof(PingConsumer), reg.RequestConsumerType);
        Assert.Equal(typeof(Pong), reg.ResponseType);
        Assert.Null(reg.Handler);
        Assert.Null(reg.ConsumerType);
        Assert.Null(reg.RequestHandler);
    }

    [Fact]
    public void AddTopicRegistration_RequestHandler_NeitherRequestHandlerNorRequestConsumerType_ThrowsArgumentException()
    {
        var registry = new TopicRegistry();

        var ex = Assert.Throws<ArgumentException>(() =>
            TopicRegistryExtensions.AddTopicRegistration(
                registry,
                "ping.topic",
                typeof(Ping),
                null,
                null,
                null,
                null,
                typeof(Pong),
                null));

        Assert.Contains("delegate request handler", ex.Message);
    }

    [Fact]
    public void AddTopicRegistration_RequestHandler_BothRequestHandlerAndRequestConsumerType_ThrowsArgumentException()
    {
        var registry = new TopicRegistry();

        var ex = Assert.Throws<ArgumentException>(() =>
            TopicRegistryExtensions.AddTopicRegistration(
                registry,
                "ping.topic",
                typeof(Ping),
                null,
                null,
                null,
                typeof(PingConsumer),
                typeof(Pong),
                (_, _, _, _) => Task.FromResult<object>(new Pong())));

        Assert.Contains("both", ex.Message);
    }

    [Fact]
    public void AddTopicRegistration_RequestHandler_WithPlainConsumerType_ThrowsArgumentException()
    {
        var registry = new TopicRegistry();

        var ex = Assert.Throws<ArgumentException>(() =>
            TopicRegistryExtensions.AddTopicRegistration(
                registry,
                "ping.topic",
                typeof(Ping),
                null,
                null,
                typeof(PingConsumer),
                null,
                typeof(Pong),
                (_, _, _, _) => Task.FromResult<object>(new Pong())));

        Assert.Contains("plain class consumer type", ex.Message);
    }


    [Fact]
    public void MapRequest_PostSeal_ThrowsInvalidOperationException()
    {
        var registry = new TopicRegistry();
        registry.Seal();

        var ex = Assert.Throws<InvalidOperationException>(() =>
            registry.MapRequest<Ping, Pong>("late.topic", async (msg, _, _, ct) => new Pong()));

        Assert.Contains("MapTopic/MapRequest", ex.Message);
    }

    [Fact]
    public void MapRequest_Over_Existing_MapRequest_On_Same_Topic_Throws()
    {
        var registry = new TopicRegistry();
        registry.MapRequest<Ping, Pong>("ping.topic", async (msg, _, _, ct) => new Pong { Echo = msg.Value });

        var ex = Assert.Throws<InvalidOperationException>(() =>
            registry.MapRequest<Ping, Pong>("ping.topic", async (msg, _, _, ct) => new Pong { Echo = msg.Value }));

        Assert.Contains("ping.topic", ex.Message);
        Assert.Contains("cannot have both plain and request/response registrations", ex.Message);
    }

    [Fact]
    public void MapTopic_Over_Existing_MapRequest_On_Same_Topic_Throws()
    {
        var registry = new TopicRegistry();
        registry.MapRequest<Ping, Pong>("ping.topic", async (msg, _, _, ct) => new Pong { Echo = msg.Value });

        var ex = Assert.Throws<InvalidOperationException>(() =>
            registry.MapTopic<Ping>("ping.topic", (msg, ct) => Task.CompletedTask));

        Assert.Contains("ping.topic", ex.Message);
    }

    [Fact]
    public void MapRequest_Over_Existing_MapTopic_On_Same_Topic_Throws()
    {
        var registry = new TopicRegistry();
        registry.MapTopic<Ping>("ping.topic", (msg, ct) => Task.CompletedTask);

        var ex = Assert.Throws<InvalidOperationException>(() =>
            registry.MapRequest<Ping, Pong>("ping.topic", async (msg, _, _, ct) => new Pong { Echo = msg.Value }));

        Assert.Contains("ping.topic", ex.Message);
    }

    [Fact]
    public void MapTopic_Twice_On_Same_Topic_Is_Allowed_For_FanOut()
    {
        var registry = new TopicRegistry();
        registry.MapTopic<Ping>("ping.topic", (msg, ct) => Task.CompletedTask);
        registry.MapTopic<Ping>("ping.topic", (msg, ct) => Task.CompletedTask);

        Assert.Equal(2, registry.Registrations.Count);
    }
}
