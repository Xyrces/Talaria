using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
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
    private readonly int _bufferCapacity;
    private readonly ILogger<KafkaConsumer<T>>? _logger;

    public KafkaConsumer(
        IConsumer<string, byte[]> consumer,
        IProducer<string, byte[]> producer,
        string topic,
        KafkaTransportOptions options,
        string dlqSuffix,
        ILogger<KafkaConsumer<T>>? logger = null,
        int bufferCapacity = 100)
    {
        _consumer = consumer;
        _producer = producer;
        _topic = topic;
        _options = options;
        _dlqTopic = _topic + dlqSuffix;
        _logger = logger;
        _bufferCapacity = bufferCapacity > 0 ? bufferCapacity : 100;
    }

    public async IAsyncEnumerable<MessageEnvelope<T>> ConsumeAsync([EnumeratorCancellation] CancellationToken ct = default)
    {
        var channel = System.Threading.Channels.Channel.CreateBounded<MessageEnvelope<T>>(new System.Threading.Channels.BoundedChannelOptions(_bufferCapacity)
        {
            SingleWriter = true,
            SingleReader = true,
            FullMode = System.Threading.Channels.BoundedChannelFullMode.Wait
        });

        _ = Task.Factory.StartNew(async () =>
        {
            try
            {
                _consumer.Subscribe(_topic);

                while (!ct.IsCancellationRequested)
                {
                    ConsumeResult<string, byte[]>? consumeResult = null;
                    try
                    {
                        consumeResult = _consumer.Consume(TimeSpan.FromMilliseconds(100));
                    }
                    catch (ConsumeException)
                    {
                        try { await Task.Delay(100, ct).ConfigureAwait(false); } catch (OperationCanceledException) { break; }
                        continue;
                    }

                    if (consumeResult == null || consumeResult.IsPartitionEOF)
                    {
                        continue;
                    }

                    var talariaHeaders = new MessageHeaders();
                    if (consumeResult.Message.Headers != null)
                    {
                        foreach (var header in consumeResult.Message.Headers)
                        {
                            talariaHeaders[header.Key] = Encoding.UTF8.GetString(header.GetValueBytes());
                        }
                    }

                    T? payload = default;
                    try
                    {
                        payload = JsonSerializer.Deserialize<T>(consumeResult.Message.Value);
                    }
                    catch (Exception ex)
                    {
                        talariaHeaders.DlqReason = "DeserializationFailed";
                        talariaHeaders.DlqException = ex.Message;
                        
                        await RouteToDlqAsync(consumeResult, talariaHeaders, ct).ConfigureAwait(false);
                        _consumer.Commit(consumeResult);
                        continue;
                    }

                    if (payload == null)
                    {
                        talariaHeaders.DlqReason = "null_payload";
                        await RouteToDlqAsync(consumeResult, talariaHeaders, ct).ConfigureAwait(false);
                        _consumer.Commit(consumeResult);
                        continue;
                    }

                    var env = new MessageEnvelope<T>
                    {
                        Payload = payload,
                        Headers = talariaHeaders,
                        SourceTopic = _topic,
                        PartitionKey = consumeResult.Message.Key,
                        Partition = consumeResult.Partition.Value,
                        CorrelationId = talariaHeaders.TryGetValue(MessageHeaders.CorrelationIdKey, out var cid) ? cid : null,
                        Timestamp = consumeResult.Message.Timestamp.UtcDateTime,
                        Offset = consumeResult.Offset.Value
                    };
                    env.Headers[MessageHeaders.KafkaPartitionKey] = consumeResult.Partition.Value.ToString();

                    await channel.Writer.WriteAsync(env, ct).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) { }
            catch (Exception ex)
            {
                channel.Writer.TryComplete(ex);
                return;
            }
            finally
            {
                try { _consumer.Unsubscribe(); } catch { }
                channel.Writer.TryComplete();
            }
        }, ct, TaskCreationOptions.LongRunning, TaskScheduler.Default);

        await foreach (var env in channel.Reader.ReadAllAsync(ct))
        {
            yield return env;
        }
    }

    public Task CommitAsync(MessageEnvelope<T> message, CancellationToken ct = default)
    {
        if (!string.IsNullOrEmpty(message.SourceTopic) &&
            message.Headers.TryGetValue(MessageHeaders.KafkaPartitionKey, out var pStr) &&
            int.TryParse(pStr, out var partitionVal))
        {
            // Kafka committed offsets mean "the next offset to fetch", hence + 1.
            var tpo = new TopicPartitionOffset(message.SourceTopic, new Partition(partitionVal), new Offset(message.Offset + 1));
            _consumer.Commit(new[] { tpo });
        }
        else
        {
            // Never fall back to Commit() with no arguments — that commits the current position on
            // ALL assigned partitions and can skip unprocessed messages. Leave uncommitted for redelivery.
            _logger?.LogError(
                "Cannot commit offset for message on topic {Topic}: missing partition metadata. Leaving the message uncommitted for redelivery.",
                message.SourceTopic);
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
            message.Headers.TryGetValue(MessageHeaders.KafkaPartitionKey, out var pStr) &&
            int.TryParse(pStr, out var partitionVal))
        {
            // Kafka committed offsets mean "the next offset to fetch", hence + 1.
            var tpo = new TopicPartitionOffset(message.SourceTopic, new Partition(partitionVal), new Offset(message.Offset + 1));
            _consumer.Commit(new[] { tpo });
        }
        else
        {
            _logger?.LogError(
                "Cannot commit offset for nacked message on topic {Topic}: missing partition metadata. Leaving the message uncommitted for redelivery.",
                message.SourceTopic);
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

    public ValueTask DisposeAsync()
    {
        _consumer.Close();
        return ValueTask.CompletedTask;
    }
}
