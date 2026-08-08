using System.Text.Json;
using System.Threading.Channels;
using Talaria.Core.Abstractions;
using Talaria.Transports.InMemory;
using Xunit;

namespace Talaria.InMemory.Tests;

public class InMemoryConsumerTests
{
    private (Channel<InMemoryMessage>, InMemoryTransport.TopicBus, InMemoryTransport.TopicBus, InMemoryConsumer<string>) CreateSut(TimeSpan latency = default)
    {
        var ch = Channel.CreateUnbounded<InMemoryMessage>();
        var dlqBus = new InMemoryTransport.TopicBus(100, unbounded: true);
        var appDlqBus = new InMemoryTransport.TopicBus(100, unbounded: true);
        var options = new InMemoryTransportOptions { SimulatedLatency = latency };

        var consumer = new InMemoryConsumer<string>("test-topic", ch, dlqBus, appDlqBus, options, includeDlqExceptionDetails: true);
        return (ch, dlqBus, appDlqBus, consumer);
    }

    [Fact]
    public async Task ConsumeAsync_NoLatency_YieldsMessagesImmediately()
    {
        var (ch, _, _, consumer) = CreateSut();
        await ch.Writer.WriteAsync(new InMemoryMessage { PayloadJson = "\"test\"", Headers = new MessageHeaders() });
        ch.Writer.Complete();

        var messages = new List<MessageEnvelope<string>>();
        await foreach (var env in consumer.ConsumeAsync())
        {
            messages.Add(env);
        }

        Assert.Single(messages);
        Assert.Equal("test", messages[0].Payload);
    }

    [Fact]
    public async Task ConsumeAsync_WithLatency_DelaysYield()
    {
        var (ch, _, _, consumer) = CreateSut(TimeSpan.FromMilliseconds(50));
        await ch.Writer.WriteAsync(new InMemoryMessage { PayloadJson = "\"delay\"", Headers = new MessageHeaders() });
        ch.Writer.Complete();

        var start = DateTime.UtcNow;
        var enumerator = consumer.ConsumeAsync().GetAsyncEnumerator();
        Assert.True(await enumerator.MoveNextAsync());
        var duration = DateTime.UtcNow - start;
        
        Assert.True(duration.TotalMilliseconds >= 25);
        Assert.Equal("delay", enumerator.Current.Payload);
    }

    [Fact]
    public async Task NackAsync_SendsToDlqAndAppDlq()
    {
        var (_, dlqBus, appDlqBus, consumer) = CreateSut();
        var dlqReader = dlqBus.GetOrCreateGroupChannel("test").Reader;
        var appDlqReader = appDlqBus.GetOrCreateGroupChannel("test").Reader;
        var env = new MessageEnvelope<string> { Payload = "failed", Headers = new MessageHeaders() };

        await consumer.NackAsync(env);

        var dlqMsg = await dlqReader.ReadAsync();
        var appDlqMsg = await appDlqReader.ReadAsync();

        Assert.Equal("\"failed\"", dlqMsg.PayloadJson);
        Assert.Equal("\"failed\"", appDlqMsg.PayloadJson);
    }

    [Fact]
    public async Task Commit_And_Dispose_RunSuccessfully()
    {
        var (_, _, _, consumer) = CreateSut();
        await consumer.CommitAsync(new MessageEnvelope<string> { Payload = "dummy" });
        await consumer.DisposeAsync();
        Assert.True(true);
    }
}
