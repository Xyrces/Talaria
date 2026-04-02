using System.Threading.Channels;
using Talaria.Core.Abstractions;
using Talaria.Transports.InMemory;
using Xunit;

namespace Talaria.InMemory.Tests;

public class InMemoryProducerTests
{
    [Fact]
    public async Task ProduceAsync_WithHeaders_WritesToChannel()
    {
        var channel = Channel.CreateUnbounded<InMemoryMessage>();
        var options = new InMemoryTransportOptions();
        var producer = new InMemoryProducer<string>(channel, "test-topic", options);

        var headers = new MessageHeaders { ["X-Test"] = "True" };
        await producer.ProduceAsync("hello", headers);

        var msg = await channel.Reader.ReadAsync();
        Assert.Equal("\"hello\"", msg.PayloadJson);
        Assert.Equal("True", msg.Headers["X-Test"]);
    }

    [Fact]
    public async Task DisposeAsync_CompletesSuccessfully()
    {
        var channel = Channel.CreateUnbounded<InMemoryMessage>();
        var producer = new InMemoryProducer<string>(channel, "topic", new InMemoryTransportOptions());
        await producer.DisposeAsync();
        Assert.True(true);
    }
}
