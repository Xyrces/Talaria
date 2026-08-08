using System.Threading.Channels;
using Talaria.Core.Abstractions;
using Talaria.Transports.InMemory;

namespace Talaria.Specs;

public class InMemoryConsumerTests
{
    [Fact]
    public async Task ConsumeAsync_CompletesWhenChannelClosed()
    {
        var channel = Channel.CreateUnbounded<InMemoryMessage>();
        var dlqChannel = Channel.CreateUnbounded<InMemoryMessage>();
        var appDlqChannel = Channel.CreateUnbounded<InMemoryMessage>();
        var options = new InMemoryTransportOptions();

        var consumer = new InMemoryConsumer<string>("topic", channel, dlqChannel, appDlqChannel, options);
        
        channel.Writer.Complete();

        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(1));
        var count = 0;
        await foreach (var item in consumer.ConsumeAsync(cts.Token))
        {
            count++;
        }
        
        Assert.Equal(0, count);
    }
    
    [Fact]
    public async Task ConsumeAsync_AppliesSimulatedLatency()
    {
        var channel = Channel.CreateUnbounded<InMemoryMessage>();
        var dlqChannel = Channel.CreateUnbounded<InMemoryMessage>();
        var appDlqChannel = Channel.CreateUnbounded<InMemoryMessage>();
        var options = new InMemoryTransportOptions { SimulatedLatency = TimeSpan.FromMilliseconds(50) };

        var consumer = new InMemoryConsumer<string>("topic", channel, dlqChannel, appDlqChannel, options);
        
        await channel.Writer.WriteAsync(new InMemoryMessage { PayloadJson = "\"test\"", Headers = new MessageHeaders() });
        channel.Writer.Complete();

        var sw = System.Diagnostics.Stopwatch.StartNew();
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(1));
        var count = 0;
        await foreach (var item in consumer.ConsumeAsync(cts.Token))
        {
            count++;
        }
        sw.Stop();
        
        Assert.Equal(1, count);
        Assert.True(sw.ElapsedMilliseconds >= 30, "Should have awaited simulated latency");
    }
}
