using Confluent.Kafka;
using Talaria.Core;
using Talaria.Core.Abstractions;

namespace Talaria.Transports.Kafka;

/// <summary>
/// Apache Kafka transport entry point. Configures and registers Kafka channels.
/// </summary>
public sealed class KafkaTransport : ITransport
{
    private readonly KafkaTransportOptions _kafkaOptions;

    /// <summary>
    /// Creates a KafkaTransport with the specified options.
    /// </summary>
    public KafkaTransport(KafkaTransportOptions kafkaOptions)
    {
        _kafkaOptions = kafkaOptions ?? throw new ArgumentNullException(nameof(kafkaOptions));
    }

    public string Name => "Kafka";

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
        
        var producerConfig = new ProducerConfig(_kafkaOptions.BaseProducerConfig)
        {
            BootstrapServers = _kafkaOptions.BootstrapServers
        };
        var confluentDlqProducer = new ProducerBuilder<string, byte[]>(producerConfig).Build();

        IConsumer<T> wrapper = new KafkaConsumer<T>(confluentConsumer, confluentDlqProducer, topic, _kafkaOptions, ".dlq");
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
        var producerConfig = new ProducerConfig(_kafkaOptions.BaseProducerConfig)
        {
            BootstrapServers = _kafkaOptions.BootstrapServers
        };
        
        var confluentProducer = new ProducerBuilder<string, byte[]>(producerConfig).Build();

        IProducer<T> wrapper = new KafkaProducer<T>(confluentProducer, topic);
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
}

internal class NoOpKafkaTransactionalSession : ITransactionalSession
{
    public Task CommitAsync(CancellationToken ct = default) => Task.CompletedTask;
    public Task AbortAsync(CancellationToken ct = default) => Task.CompletedTask;
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
