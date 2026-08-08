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

    // Pool of transactional producers (each holds a stable TransactionalId for zombie fencing).
    private readonly ConcurrentBag<IProducer<string, byte[]>> _transactionalProducerPool = new();
    private readonly string _transactionalIdPrefix = $"talaria-{Guid.NewGuid():N}";
    private int _transactionalProducerCounter;

    // Consumer group metadata by group id, registered when a consumer is created.
    // Required to commit consumer offsets inside a transaction.
    private readonly ConcurrentDictionary<string, IConsumerGroupMetadata> _groupMetadata = new();

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

        // Track the group's metadata so transactions can commit this group's offsets.
        _groupMetadata[config.GroupId] = confluentConsumer.ConsumerGroupMetadata;

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
    /// Begins a real Kafka transaction (exactly-once semantics). All produces obtained
    /// from the session, plus the consumed message's offset when provided, commit atomically.
    /// </summary>
    public Task<ITransactionalSession> BeginTransactionAsync(
        string? consumerGroup = null,
        TransactionOffsetSource? offsetSource = null,
        CancellationToken ct = default)
    {
        var producer = CheckoutTransactionalProducer();
        ITransactionalSession session = new KafkaTransactionalSession(this, producer, consumerGroup, offsetSource);
        return Task.FromResult(session);
    }

    internal IProducer<string, byte[]> CheckoutTransactionalProducer()
    {
        if (_transactionalProducerPool.TryTake(out var pooled))
        {
            return pooled;
        }

        var id = Interlocked.Increment(ref _transactionalProducerCounter);
        var config = new ProducerConfig(_kafkaOptions.BaseProducerConfig)
        {
            BootstrapServers = _kafkaOptions.BootstrapServers,
            Acks = Acks.All,
            EnableIdempotence = true, // required for transactions
            TransactionalId = $"{_transactionalIdPrefix}-{id}"
        };

        var producer = new ProducerBuilder<string, byte[]>(config).Build();
        producer.InitTransactions(TimeSpan.FromSeconds(30));
        return producer;
    }

    internal void ReturnTransactionalProducer(IProducer<string, byte[]> producer)
        => _transactionalProducerPool.Add(producer);

    internal IConsumerGroupMetadata? GetConsumerGroupMetadata(string consumerGroup)
        => _groupMetadata.TryGetValue(consumerGroup, out var metadata) ? metadata : null;

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

        while (_transactionalProducerPool.TryTake(out var transactional))
        {
            try
            {
                transactional.Flush(_kafkaOptions.FlushTimeout);
                transactional.Dispose();
            }
            catch
            {
                // Ignore cleanup errors during shutdown
            }
        }

        return ValueTask.CompletedTask;
    }
}
