using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Threading.Channels;
using Talaria.Core.Abstractions;

namespace Talaria.Transports.InMemory;

/// <summary>
/// In-memory consumer backed by a Channel reader.
/// </summary>
internal sealed class InMemoryConsumer<T> : IConsumer<T>
{
    private readonly Channel<InMemoryMessage> _channel;
    private readonly Channel<InMemoryMessage> _dlqChannel;
    private readonly Channel<InMemoryMessage> _appDlqChannel;
    private readonly InMemoryTransportOptions _options;
    private readonly string _topic;

    public InMemoryConsumer(
        string topic,
        Channel<InMemoryMessage> channel,
        Channel<InMemoryMessage> dlqChannel,
        Channel<InMemoryMessage> appDlqChannel,
        InMemoryTransportOptions options)
    {
        _topic = topic;
        _channel = channel;
        _dlqChannel = dlqChannel;
        _appDlqChannel = appDlqChannel;
        _options = options;
    }

    public async IAsyncEnumerable<MessageEnvelope<T>> ConsumeAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        await foreach (var raw in _channel.Reader.ReadAllAsync(ct))
        {
            if (_options.SimulatedLatency > TimeSpan.Zero)
            {
                await Task.Delay(_options.SimulatedLatency, ct);
            }

            var payload = JsonSerializer.Deserialize<T>(raw.PayloadJson)!;

            yield return new MessageEnvelope<T>
            {
                Payload = payload,
                Headers = raw.Headers,
                SourceTopic = _topic,
                CorrelationId = raw.Headers.TryGetValue(MessageHeaders.CorrelationIdKey, out var cid) ? cid : null,
                Offset = raw.Offset,
                Timestamp = raw.Timestamp,
            };
        }
    }

    public Task CommitAsync(MessageEnvelope<T> message, CancellationToken ct = default)
    {
        // In-memory: commit is a no-op (message already consumed from channel)
        return Task.CompletedTask;
    }

    public async Task NackAsync(MessageEnvelope<T> message, CancellationToken ct = default)
    {
        // Route to both topic-specific DLQ and app-wide DLQ
        var dlqMessage = new InMemoryMessage
        {
            PayloadJson = JsonSerializer.Serialize(message.Payload),
            Headers = new MessageHeaders(message.Headers),
            Offset = message.Offset,
            Timestamp = message.Timestamp,
        };

        await _dlqChannel.Writer.WriteAsync(dlqMessage, ct);
        await _appDlqChannel.Writer.WriteAsync(dlqMessage, ct);
    }

    public ValueTask DisposeAsync()
    {
        return ValueTask.CompletedTask;
    }
}
