using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Talaria.Core.Abstractions;

namespace Talaria.Transports.Kafka;

/// <summary>
/// Apache Kafka producer that writes serialized messages to topics.
/// </summary>
internal sealed class KafkaProducer<T> : IProducer<T>
{
    private readonly IProducer<string, byte[]> _producer;
    private readonly string _topic;

    public KafkaProducer(IProducer<string, byte[]> producer, string topic)
    {
        _producer = producer;
        _topic = topic;
    }

    public async Task ProduceAsync(
        T message,
        MessageHeaders? headers = null,
        string? partitionKey = null,
        CancellationToken ct = default)
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

        var payloadBytes = JsonSerializer.SerializeToUtf8Bytes(message);
        var kafkaHeaders = new Headers();
        
        foreach (var header in finalHeaders)
        {
            if (header.Value != null)
            {
                kafkaHeaders.Add(header.Key, Encoding.UTF8.GetBytes(header.Value));
            }
        }

        var correlationId = finalHeaders.TryGetValue(MessageHeaders.CorrelationIdKey, out var cid) ? cid : null;
        var msg = new Message<string, byte[]>
        {
            Key = partitionKey ?? (correlationId ?? Guid.NewGuid().ToString("N")),
            Value = payloadBytes,
            Headers = kafkaHeaders,
            Timestamp = new Timestamp(DateTimeOffset.UtcNow)
        };

        if (System.Diagnostics.Activity.Current != null)
        {
            System.Diagnostics.Activity.Current.SetTag("messaging.destination.name", _topic);
            System.Diagnostics.Activity.Current.SetTag("messaging.system", "kafka");
        }

        await _producer.ProduceAsync(_topic, msg, ct);
    }

    public ValueTask DisposeAsync()
    {
        // Producer is usually disposed via underlying transport flush on app shutdown
        return ValueTask.CompletedTask;
    }
}
