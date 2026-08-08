using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Threading.Channels;
using Talaria.Core.Abstractions;

namespace Talaria.Transports.InMemory;

/// <summary>
/// In-memory consumer backed by a Channel reader for a specific consumer group.
/// Malformed or null payloads are routed to the DLQ (Kafka parity) instead of
/// killing the consumer loop.
/// <para>
/// Acknowledgement semantics: yielded envelopes stay in a pending set until
/// <see cref="CommitAsync"/> or <see cref="NackAsync"/> settles them. On dispose
/// (host shutdown or a faulting consumer loop being restarted) every unsettled
/// message is requeued to the group channel — the in-memory equivalent of Kafka
/// redelivering uncommitted messages.
/// </para>
/// </summary>
internal sealed class InMemoryConsumer<T> : IConsumer<T>
{
    private readonly Channel<InMemoryMessage> _groupChannel;
    private readonly InMemoryTransport.TopicBus _dlqBus;
    private readonly InMemoryTransport.TopicBus _appDlqBus;
    private readonly InMemoryTransportOptions _options;
    private readonly bool _includeDlqExceptionDetails;
    private readonly string _topic;

    // Unsettled envelopes by offset, mirroring Kafka's uncommitted offsets.
    private readonly System.Collections.Concurrent.ConcurrentDictionary<long, InMemoryMessage> _pending = new();

    public InMemoryConsumer(
        string topic,
        Channel<InMemoryMessage> groupChannel,
        InMemoryTransport.TopicBus dlqBus,
        InMemoryTransport.TopicBus appDlqBus,
        InMemoryTransportOptions options,
        bool includeDlqExceptionDetails)
    {
        _topic = topic;
        _groupChannel = groupChannel;
        _dlqBus = dlqBus;
        _appDlqBus = appDlqBus;
        _options = options;
        _includeDlqExceptionDetails = includeDlqExceptionDetails;
    }

    public async IAsyncEnumerable<MessageEnvelope<T>> ConsumeAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        await foreach (var raw in _groupChannel.Reader.ReadAllAsync(ct))
        {
            if (_options.SimulatedLatency > TimeSpan.Zero)
            {
                await Task.Delay(_options.SimulatedLatency, ct);
            }

            T? payload;
            try
            {
                payload = JsonSerializer.Deserialize<T>(raw.PayloadJson);
            }
            catch (Exception ex)
            {
                // Poison message — route the raw payload to the DLQ and keep consuming (Kafka parity).
                await RoutePoisonToDlqAsync(raw, "DeserializationFailed", ex, ct);
                continue;
            }

            if (payload is null)
            {
                await RoutePoisonToDlqAsync(raw, "null_payload", ex: null, ct);
                continue;
            }

            _pending[raw.Offset] = raw;

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

    private async Task RoutePoisonToDlqAsync(InMemoryMessage raw, string reason, Exception? ex, CancellationToken ct)
    {
        var headers = new MessageHeaders(raw.Headers)
        {
            DlqReason = reason
        };

        if (ex is not null)
        {
            headers.DlqException = _includeDlqExceptionDetails
                ? ex.Message
                : "Failed to deserialize the message payload. Enable IncludeExceptionDetailsInDlq for details.";
        }

        var dlqMessage = new InMemoryMessage
        {
            PayloadJson = raw.PayloadJson, // keep the raw payload — it failed deserialization
            Headers = headers,
            Timestamp = raw.Timestamp,
        };

        await _dlqBus.PublishAsync(dlqMessage, ct);
        await _appDlqBus.PublishAsync(dlqMessage, ct);
    }

    public Task CommitAsync(MessageEnvelope<T> message, CancellationToken ct = default)
    {
        _pending.TryRemove(message.Offset, out _);
        return Task.CompletedTask;
    }

    public async Task NackAsync(MessageEnvelope<T> message, CancellationToken ct = default)
    {
        // Route to both topic-specific DLQ and app-wide DLQ
        var dlqMessage = new InMemoryMessage
        {
            PayloadJson = JsonSerializer.Serialize(message.Payload),
            Headers = new MessageHeaders(message.Headers),
            Timestamp = message.Timestamp,
        };

        await _dlqBus.PublishAsync(dlqMessage, ct);
        await _appDlqBus.PublishAsync(dlqMessage, ct);

        _pending.TryRemove(message.Offset, out _);
    }

    public async ValueTask DisposeAsync()
    {
        // Kafka parity: unsettled (uncommitted) messages are redelivered. Requeue them
        // to the group channel so the next consumer instance picks them up. A full
        // bounded channel drops the overflow — accepted, documented transport divergence.
        foreach (var raw in _pending.Values)
        {
            _groupChannel.Writer.TryWrite(raw);
        }

        _pending.Clear();
        await ValueTask.CompletedTask;
    }
}
