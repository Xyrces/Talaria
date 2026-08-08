using System.Collections.Concurrent;
using Confluent.Kafka;
using Talaria.Core;
using Talaria.Core.Abstractions;

namespace Talaria.Transports.Kafka;

/// <summary>
/// Apache Kafka transport entry point. Configures and registers Kafka channels.
/// </summary>
public sealed class KafkaTransport : ITransport, IAsyncDisposable
{
    private readonly KafkaTransportOptions _kafkaOptions;
    private readonly ConcurrentDictionary<string, IProducer<string, byte[]>> _producers = new();

    /// <summary>
    /// Creates a KafkaTransport with the specified options.
    /// </summary>
    public KafkaTransport(KafkaTransportOptions kafkaOptions)
    {
        _kafkaOptions = kafkaOptions ?? throw new ArgumentNullException(nameof(kafkaOptions));

        if (string.IsNullOrWhiteSpace(_kafkaOptions.BootstrapServers))
        {
            throw new ArgumentException(
                $"{nameof(KafkaTransportOptions.BootstrapServers)} is required (e.g. \"localhost:9092\").",
                nameof(kafkaOptions));
        }
    }

    public string Name => "Kafka";

    private IProducer<string, byte[]> GetOrCreateRawProducer(string topic, bool enableIdempotence = true)
    {
        return _producers.GetOrAdd($"{topic}|idempotent:{enableIdempotence}", _ =>
        {
            var producerConfig = new ProducerConfig(_kafkaOptions.BaseProducerConfig)
            {
                BootstrapServers = _kafkaOptions.BootstrapServers,
                Acks = Acks.All,
                EnableIdempotence = enableIdempotence
            };
            return new ProducerBuilder<string, byte[]>(producerConfig).Build();
        });
    }

    /// <summary>
    /// Creates a new consumer for a specific topic.
    /// </summary>
    public Task<IConsumer<T>> CreateConsumerAsync<T>(
        string topic,
        ConsumerOptions options,
        CancellationToken ct = default)
    {
        var config = new ConsumerConfig(_kafkaOptions.BaseConsumerConfig)
        {
            BootstrapServers = _kafkaOptions.BootstrapServers,
            GroupId = options.ConsumerGroup ?? _kafkaOptions.BaseConsumerConfig.GroupId ?? "talaria-consumer",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false // We commit manually in the consumer or via Nack Async
        };

        var confluentConsumer = new ConsumerBuilder<string, byte[]>(config).Build();
        var confluentDlqProducer = GetOrCreateRawProducer(topic + _kafkaOptions.DlqSuffix);

        IConsumer<T> wrapper = new KafkaConsumer<T>(
            confluentConsumer, confluentDlqProducer, topic, _kafkaOptions, _kafkaOptions.DlqSuffix,
            bufferCapacity: options.BufferCapacity > 0 ? options.BufferCapacity : 100);
        return Task.FromResult(wrapper);
    }

    /// <summary>
    /// Creates a new producer for a specific topic.
    /// </summary>
    public Task<IProducer<T>> CreateProducerAsync<T>(
        string topic,
        ProducerOptions options,
        CancellationToken ct = default)
    {
        var rawProducer = GetOrCreateRawProducer(topic, options.EnableIdempotence);
        IProducer<T> wrapper = new KafkaProducer<T>(rawProducer, topic);
        return Task.FromResult(wrapper);
    }

    /// <summary>
    /// Creates a transactional session (not purely supported by standard Kafka Producers without exactly-once config)
    /// </summary>
    public Task<ITransactionalSession> BeginTransactionAsync(CancellationToken ct = default)
    {
        // For standard Kafka, we might just use a no-op session unless we configure Transactions in Kafka exactly-once semantics.
        // For MVP, we'll return a NoOpSession.
        ITransactionalSession session = new NoOpKafkaTransactionalSession();
        return Task.FromResult(session);
    }

    public ValueTask DisposeAsync()
    {
        foreach (var producer in _producers.Values)
        {
            try
            {
                producer.Flush(_kafkaOptions.FlushTimeout);
                producer.Dispose();
            }
            catch
            {
                // Ignore cleanup errors during shutdown
            }
        }
        _producers.Clear();
        return ValueTask.CompletedTask;
    }
}

internal class NoOpKafkaTransactionalSession : ITransactionalSession
{
    public Task CommitAsync(CancellationToken ct = default) => Task.CompletedTask;
    public Task AbortAsync(CancellationToken ct = default) => Task.CompletedTask;
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
