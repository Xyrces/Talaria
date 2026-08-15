using System.Text.Json;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Talaria.Core.Abstractions;
using Talaria.Transports.InMemory;
using Xunit;

namespace Talaria.InMemory.Tests;

public class InMemoryConsumerTests
{
    private (Channel<InMemoryMessage>, InMemoryTransport.TopicBus, InMemoryTransport.TopicBus, InMemoryConsumer<string>) CreateSut(TimeSpan latency = default, ILogger? logger = null)
    {
        var ch = Channel.CreateUnbounded<InMemoryMessage>();
        var dlqBus = new InMemoryTransport.TopicBus(100, unbounded: true);
        var appDlqBus = new InMemoryTransport.TopicBus(100, unbounded: true);
        var options = new InMemoryTransportOptions { SimulatedLatency = latency };

        var consumer = new InMemoryConsumer<string>("test-topic", ch, dlqBus, appDlqBus, options, includeDlqExceptionDetails: true, logger);
        return (ch, dlqBus, appDlqBus, consumer);
    }

    private sealed class CollectingLogger : ILogger
    {
        public List<(LogLevel Level, string Message)> Entries { get; } = new();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            Entries.Add((logLevel, formatter(state, exception)));
        }
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
    public async Task ConsumeAsync_SecondEnumeration_ThrowsInvalidOperationException()
    {
        var (ch, _, _, consumer) = CreateSut();
        await ch.Writer.WriteAsync(new InMemoryMessage { PayloadJson = "\"first\"", Headers = new MessageHeaders() });
        ch.Writer.Complete();

        // First enumeration completes normally.
        var first = new List<MessageEnvelope<string>>();
        await foreach (var env in consumer.ConsumeAsync())
        {
            first.Add(env);
        }
        Assert.Single(first);

        // A second call on the same instance is forbidden.
        var ex = Assert.Throws<InvalidOperationException>(() => consumer.ConsumeAsync().GetAsyncEnumerator());
        Assert.Equal("ConsumeAsync may only be enumerated once per consumer instance. Create a new consumer to restart consumption.", ex.Message);
    }

    [Fact]
    public async Task ConsumeAsync_SecondEnumerationWhileFirstActive_ThrowsInvalidOperationException()
    {
        var (ch, _, _, consumer) = CreateSut();
        await ch.Writer.WriteAsync(new InMemoryMessage { PayloadJson = "\"first\"", Headers = new MessageHeaders() });

        // Start the first enumeration but do not complete it.
        var enumerator = consumer.ConsumeAsync().GetAsyncEnumerator();
        Assert.True(await enumerator.MoveNextAsync());
        Assert.Equal("first", enumerator.Current.Payload);

        // A concurrent/second call on the same instance is forbidden even though
        // the first enumeration is still active.
        var ex = Assert.Throws<InvalidOperationException>(() => consumer.ConsumeAsync().GetAsyncEnumerator());
        Assert.Equal("ConsumeAsync may only be enumerated once per consumer instance. Create a new consumer to restart consumption.", ex.Message);
    }

    [Fact]
    public async Task ConsumeAsync_ReEnumerateReturnedInstance_ThrowsInvalidOperationException()
    {
        var (ch, _, _, consumer) = CreateSut();
        await ch.Writer.WriteAsync(new InMemoryMessage { PayloadJson = "\"first\"", Headers = new MessageHeaders() });
        ch.Writer.Complete();

        // Capture the single IAsyncEnumerable returned by ConsumeAsync.
        var enumerable = consumer.ConsumeAsync();

        // First enumeration completes normally.
        var first = new List<MessageEnvelope<string>>();
        await foreach (var env in enumerable)
        {
            first.Add(env);
        }
        Assert.Single(first);

        // Re-enumerating the SAME returned instance is also forbidden. For async
        // iterators the guard runs when MoveNextAsync first advances the enumerator.
        var second = enumerable.GetAsyncEnumerator();
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => second.MoveNextAsync().AsTask());
        Assert.Equal("ConsumeAsync may only be enumerated once per consumer instance. Create a new consumer to restart consumption.", ex.Message);
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
    public async Task DisposeAsync_WithFullChannel_LogsWarningForEachDroppedRequeue()
    {
        // Arrange: a bounded channel with capacity 1, pre-filled so pending requeues have nowhere to go.
        var ch = Channel.CreateBounded<InMemoryMessage>(new BoundedChannelOptions(1) { FullMode = BoundedChannelFullMode.Wait });
        var dlqBus = new InMemoryTransport.TopicBus(100, unbounded: true);
        var appDlqBus = new InMemoryTransport.TopicBus(100, unbounded: true);
        var options = new InMemoryTransportOptions();
        var logger = new CollectingLogger();
        var consumer = new InMemoryConsumer<string>("test-topic", ch, dlqBus, appDlqBus, options, includeDlqExceptionDetails: true, logger);

        await ch.Writer.WriteAsync(new InMemoryMessage { Offset = 1, PayloadJson = "\"blocker\"", Headers = new MessageHeaders() });

        // Read the blocker into the pending set without committing.
        var enumerator = consumer.ConsumeAsync().GetAsyncEnumerator();
        Assert.True(await enumerator.MoveNextAsync());
        Assert.Equal("blocker", enumerator.Current.Payload);

        // Refill the channel so the pending message cannot be requeued on dispose.
        await ch.Writer.WriteAsync(new InMemoryMessage { Offset = 2, PayloadJson = "\"filler\"", Headers = new MessageHeaders() });

        // Act
        await consumer.DisposeAsync();

        // Assert: exactly one warning was logged for the single dropped requeue.
        Assert.Single(logger.Entries);
        var entry = logger.Entries[0];
        Assert.Equal(LogLevel.Warning, entry.Level);
        Assert.Contains("dropped", entry.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("test-topic", entry.Message, StringComparison.OrdinalIgnoreCase);
    }
}
