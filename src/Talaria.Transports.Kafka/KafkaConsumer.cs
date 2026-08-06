using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Talaria.Core.Abstractions;

namespace Talaria.Transports.Kafka;

/// <summary>
/// Apache Kafka consumer implementation.
/// </summary>
internal sealed class KafkaConsumer<T> : IConsumer<T>
{
    private readonly IConsumer<string, byte[]> _consumer;
    private readonly IProducer<string, byte[]> _producer;
    private readonly string _topic;
    private readonly KafkaTransportOptions _options;
    private readonly string _dlqTopic;

    public KafkaConsumer(
        IConsumer<string, byte[]> consumer,
        IProducer<string, byte[]> producer,
        string topic,
        KafkaTransportOptions options,
        string dlqSuffix)
    {
        _consumer = consumer;
        _producer = producer;
        _topic = topic;
        _options = options;
        _dlqTopic = _topic + dlqSuffix;
    }

    public async IAsyncEnumerable<MessageEnvelope<T>> ConsumeAsync([EnumeratorCancellation] CancellationToken ct = default)
    {
        // Must subscribe outside loop or right here
        _consumer.Subscribe(_topic);

        while (!ct.IsCancellationRequested)
        {
            ConsumeResult<string, byte[]>? consumeResult = null;
            try
            {
                // Task.Run to avoid blocking async thread pool on physical socket poll
                consumeResult = await Task.Run(() => _consumer.Consume(ct), ct);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (ConsumeException)
            {
                // Typical to log consumer polling exceptions, but skip yielding to retry next loop iteration
                continue;
            }

            if (consumeResult == null || consumeResult.IsPartitionEOF)
                continue;

            var talariaHeaders = new MessageHeaders();
            foreach (var header in consumeResult.Message.Headers)
            {
                talariaHeaders[header.Key] = Encoding.UTF8.GetString(header.GetValueBytes());
            }

            T? payload = default;
            try
            {
                payload = JsonSerializer.Deserialize<T>(consumeResult.Message.Value);
            }
            catch (Exception ex)
            {
                // If we can't deserialize the target payload, we MUST DLQ it immediately.
                // It can never be processed.
                talariaHeaders.DlqReason = "DeserializationFailed";
                talariaHeaders.DlqException = ex.Message;
                
                await RouteToDlqAsync(consumeResult, talariaHeaders, ct);
                
                _consumer.Commit(consumeResult);
                continue;
            }

            if (payload == null)
            {
                _consumer.Commit(consumeResult);
                continue; // Cannot yield null payloads.
            }

            var env = new MessageEnvelope<T>
            {
                Payload = payload,
                Headers = talariaHeaders,
                SourceTopic = _topic,
                PartitionKey = consumeResult.Message.Key,
                Timestamp = consumeResult.Message.Timestamp.UtcDateTime,
                Offset = consumeResult.Offset.Value
            };
            env.Headers["x-kafka-partition"] = consumeResult.Partition.Value.ToString();

            yield return env;
        }

        _consumer.Unsubscribe();
    }

    public Task CommitAsync(MessageEnvelope<T> message, CancellationToken ct = default)
    {
        if (!string.IsNullOrEmpty(message.SourceTopic) &&
            message.Headers.TryGetValue("x-kafka-partition", out var pStr) &&
            int.TryParse(pStr, out var partitionVal))
        {
            var tpo = new TopicPartitionOffset(message.SourceTopic, new Partition(partitionVal), new Offset(message.Offset));
            _consumer.Commit(new[] { tpo });
        }
        else
        {
            _consumer.Commit();
        }
        return Task.CompletedTask;
    }

    public async Task NackAsync(MessageEnvelope<T> message, CancellationToken ct = default)
    {
        // Nack moves it to the DLQ and commits the original message.
        var headers = new Headers();
        foreach (var header in message.Headers)
        {
            if (header.Value != null)
                headers.Add(header.Key, Encoding.UTF8.GetBytes(header.Value));
        }

        var dlqMsg = new Message<string, byte[]>
        {
            Key = message.PartitionKey ?? message.CorrelationId ?? Guid.NewGuid().ToString("N"),
            Value = JsonSerializer.SerializeToUtf8Bytes(message.Payload),
            Headers = headers
        };

        await _producer.ProduceAsync(_dlqTopic, dlqMsg, ct);

        if (!string.IsNullOrEmpty(message.SourceTopic) &&
            message.Headers.TryGetValue("x-kafka-partition", out var pStr) &&
            int.TryParse(pStr, out var partitionVal))
        {
            var tpo = new TopicPartitionOffset(message.SourceTopic, new Partition(partitionVal), new Offset(message.Offset));
            _consumer.Commit(new[] { tpo });
        }
        else
        {
            _consumer.Commit();
        }
    }

    private async Task RouteToDlqAsync(ConsumeResult<string, byte[]> consumeResult, MessageHeaders headers, CancellationToken ct)
    {
        var kafkaHeaders = new Headers();
        foreach (var header in headers)
        {
            if (header.Value != null)
                kafkaHeaders.Add(header.Key, Encoding.UTF8.GetBytes(header.Value));
        }

        var correlationId = headers.TryGetValue(MessageHeaders.CorrelationIdKey, out var cid) ? cid : null;
        var dlqMsg = new Message<string, byte[]>
        {
            Key = consumeResult.Message.Key ?? correlationId ?? Guid.NewGuid().ToString("N"),
            Value = consumeResult.Message.Value, // Write raw payload back because it failed deserialization
            Headers = kafkaHeaders
        };

        await _producer.ProduceAsync(_dlqTopic, dlqMsg, ct);
    }

    public async IAsyncEnumerable<MessageEnvelope<T>> ConsumeDlqAsync([EnumeratorCancellation] CancellationToken ct = default)
    {
        // Secondary subscription for replaying DLQ messages
        _consumer.Subscribe(_dlqTopic);

        while (!ct.IsCancellationRequested)
        {
            ConsumeResult<string, byte[]>? consumeResult = null;
            try
            {
                consumeResult = await Task.Run(() => _consumer.Consume(ct), ct);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (ConsumeException)
            {
                continue;
            }

            if (consumeResult == null || consumeResult.IsPartitionEOF)
                continue;

            var talariaHeaders = new MessageHeaders();
            foreach (var header in consumeResult.Message.Headers)
            {
                talariaHeaders[header.Key] = Encoding.UTF8.GetString(header.GetValueBytes());
            }

            T? payload = JsonSerializer.Deserialize<T>(consumeResult.Message.Value);
            
            if (payload == null)
            {
                _consumer.Commit(consumeResult);
                continue;
            }

            var env = new MessageEnvelope<T>
            {
                Payload = payload,
                Headers = talariaHeaders,
                SourceTopic = _dlqTopic,
                PartitionKey = consumeResult.Message.Key,
                Timestamp = consumeResult.Message.Timestamp.UtcDateTime,
                Offset = consumeResult.Offset.Value
            };
            env.Headers["x-kafka-partition"] = consumeResult.Partition.Value.ToString();

            yield return env;
        }

        _consumer.Unsubscribe();
    }

    public ValueTask DisposeAsync()
    {
        _consumer.Close();
        return ValueTask.CompletedTask;
    }
}
