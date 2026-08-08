using System.Text.Json;
using Talaria.Core.Abstractions;

namespace Talaria.Transports.InMemory;

/// <summary>
/// In-memory producer that publishes serialized messages to a topic bus.
/// </summary>
internal sealed class InMemoryProducer<T> : IProducer<T>
{
    private readonly InMemoryTransport.TopicBus _bus;
    private readonly string _topic;

    public InMemoryProducer(
        InMemoryTransport.TopicBus bus,
        string topic,
        InMemoryTransportOptions options)
    {
        _bus = bus;
        _topic = topic;
    }

    public async Task ProduceAsync(
        T message,
        MessageHeaders? headers = null,
        string? partitionKey = null,
        CancellationToken ct = default)
    {
        var msg = CreateMessage(message, headers);

        if (System.Diagnostics.Activity.Current != null)
        {
            System.Diagnostics.Activity.Current.SetTag("messaging.destination.name", _topic);
            System.Diagnostics.Activity.Current.SetTag("messaging.system", "talaria");
        }

        await _bus.PublishAsync(msg, ct);
    }

    /// <summary>
    /// Builds a wire message with all engine-owned headers stamped (message id, message type,
    /// hop counter, trace context). Shared with the transactional session's buffering producer.
    /// The offset is assigned by the bus at publish time.
    /// </summary>
    internal static InMemoryMessage CreateMessage(T message, MessageHeaders? headers)
    {
        // Clone incoming headers: never mutate or store the caller's instance.
        var finalHeaders = headers is null ? new MessageHeaders() : new MessageHeaders(headers);

        if (System.Diagnostics.Activity.Current != null && string.IsNullOrEmpty(finalHeaders.TraceParent))
        {
            finalHeaders.TraceParent = System.Diagnostics.Activity.Current.Id;
            finalHeaders.TraceState = System.Diagnostics.Activity.Current.TraceStateString;
        }

        if (string.IsNullOrEmpty(finalHeaders.MessageId))
        {
            finalHeaders.MessageId = Guid.NewGuid().ToString("N");
        }

        // Engine-owned routing metadata: the CLR type of the payload, used by consumers
        // that fan a topic out to multiple typed handlers.
        finalHeaders[MessageHeaders.MessageTypeKey] = typeof(T).FullName ?? typeof(T).Name;

        // Engine-owned hop counter: fresh messages start at 0; forwarded messages (already
        // carrying a count) are incremented so cyclic flows trip the max-hop guard.
        if (finalHeaders.ContainsKey(MessageHeaders.HopCountKey))
        {
            finalHeaders.HopCount = finalHeaders.HopCount + 1;
        }

        return new InMemoryMessage
        {
            PayloadJson = JsonSerializer.Serialize(message),
            Headers = finalHeaders,
            Timestamp = DateTimeOffset.UtcNow,
        };
    }

    public ValueTask DisposeAsync()
    {
        return ValueTask.CompletedTask;
    }
}
