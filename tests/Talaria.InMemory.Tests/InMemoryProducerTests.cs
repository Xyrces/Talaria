using Talaria.Core.Abstractions;
using Talaria.Transports.InMemory;
using Xunit;

namespace Talaria.InMemory.Tests;

public class InMemoryProducerTests
{
    [Fact]
    public async Task ProduceAsync_WithHeaders_WritesToChannel()
    {
        var bus = new InMemoryTransport.TopicBus(100, unbounded: false);
        var channel = bus.GetOrCreateGroupChannel("test");
        var producer = new InMemoryProducer<string>(bus, "test-topic", new InMemoryTransportOptions());

        var headers = new MessageHeaders { ["X-Test"] = "True" };
        await producer.ProduceAsync("hello", headers);

        var msg = await channel.Reader.ReadAsync();
        Assert.Equal("\"hello\"", msg.PayloadJson);
        Assert.Equal("True", msg.Headers["X-Test"]);
    }
}
