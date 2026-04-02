using System.Text.Json;
using System.Threading.Channels;
using Talaria.Core.Abstractions;

namespace Talaria.Transports.InMemory;

/// <summary>
/// In-memory producer that writes serialized messages to a Channel.
/// </summary>
internal sealed class InMemoryProducer<T> : IProducer<T>
{
    private readonly Channel<InMemoryMessage> _channel;
    private readonly string _topic;
    private readonly InMemoryTransportOptions _options;
    private long _offset;

    public InMemoryProducer(
        Channel<InMemoryMessage> channel,
        string topic,
        InMemoryTransportOptions options)
    {
        _channel = channel;
        _topic = topic;
        _options = options;
    }

    public async Task ProduceAsync(
        T message,
        MessageHeaders? headers = null,
        string? partitionKey = null,
        CancellationToken ct = default)
    {
        var finalHeaders = headers ?? new MessageHeaders();

        if (System.Diagnostics.Activity.Current != null && string.IsNullOrEmpty(finalHeaders.TraceParent))
        {
            finalHeaders.TraceParent = System.Diagnostics.Activity.Current.Id;
            finalHeaders.TraceState = System.Diagnostics.Activity.Current.TraceStateString;
        }

        if (string.IsNullOrEmpty(finalHeaders.MessageId))
        {
            finalHeaders.MessageId = Guid.NewGuid().ToString("N");
        }

        var msg = new InMemoryMessage
        {
            PayloadJson = JsonSerializer.Serialize(message),
            Headers = finalHeaders,
            Offset = Interlocked.Increment(ref _offset),
            Timestamp = DateTimeOffset.UtcNow,
        };

        if (System.Diagnostics.Activity.Current != null)
        {
            System.Diagnostics.Activity.Current.SetTag("messaging.destination.name", _topic);
            System.Diagnostics.Activity.Current.SetTag("messaging.system", "talaria");
        }

        await _channel.Writer.WriteAsync(msg, ct);
    }

    public ValueTask DisposeAsync()
    {
        return ValueTask.CompletedTask;
    }
}
