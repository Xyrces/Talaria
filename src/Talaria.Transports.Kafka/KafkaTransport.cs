// SPDX-License-Identifier: Apache-2.0

using System.Collections.Concurrent;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Talaria.Core;
using Talaria.Core.Abstractions;

namespace Talaria.Transports.Kafka;

/// <summary>
/// Apache Kafka transport entry point. Configures and registers Kafka channels.
/// Raw producers are shared across all topics (they are thread-safe and topic-agnostic);
/// consumers created by this transport are tracked and disposed with it.
/// </summary>
public sealed class KafkaTransport : ITransport, IAsyncDisposable
{
    private readonly KafkaTransportOptions _kafkaOptions;
    private readonly ILoggerFactory? _loggerFactory;
    private readonly ILogger? _logger;
    private readonly bool _includeDlqExceptionDetails;

    // At most two shared raw producers: idempotent (default) and non-idempotent.
    private readonly ConcurrentDictionary<bool, IProducer<string, byte[]>> _sharedProducers = new();

    // Consumers created by this transport — disposed with it (double-dispose is safe).
    private readonly ConcurrentBag<IAsyncDisposable> _trackedConsumers = new();

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
    /// <param name="kafkaOptions">Transport configuration.</param>
    /// <param name="loggerFactory">Optional logger factory for transport and consumer logging.</param>
    /// <param name="includeDlqExceptionDetails">
    /// When true, raw exception messages are written to DLQ headers. Mirrors
    /// <c>TalariaOptions.IncludeExceptionDetailsInDlq</c> — keep disabled in production.
    /// </param>
    public KafkaTransport(
        KafkaTransportOptions kafkaOptions,
        ILoggerFactory? loggerFactory = null,
        bool includeDlqExceptionDetails = false)
    {
        _kafkaOptions = kafkaOptions ?? throw new ArgumentNullException(nameof(kafkaOptions));
        _loggerFactory = loggerFactory;
        _logger = loggerFactory?.CreateLogger<KafkaTransport>();
        _includeDlqExceptionDetails = includeDlqExceptionDetails;

        if (string.IsNullOrWhiteSpace(_kafkaOptions.BootstrapServers))
        {
            throw new ArgumentException(
                $"{nameof(KafkaTransportOptions.BootstrapServers)} is required (e.g. \"localhost:9092\").",
                nameof(kafkaOptions));
        }

        WarnIfInsecure();
    }

    public string Name => "Kafka";

    private void WarnIfInsecure()
    {
        var protocol = _kafkaOptions.BaseProducerConfig.SecurityProtocol ?? SecurityProtocol.Plaintext;
        if (protocol == SecurityProtocol.Plaintext && !IsLocalhostOnly(_kafkaOptions.BootstrapServers))
        {
            _logger?.LogWarning(
                "Kafka transport connects to non-localhost brokers ({BootstrapServers}) over PLAINTEXT. " +
                "Configure SASL/SSL via KafkaTransportOptions.BaseProducerConfig/BaseConsumerConfig for production use.",
                _kafkaOptions.BootstrapServers);
        }
    }

    private static bool IsLocalhostOnly(string bootstrapServers)
        => bootstrapServers.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .All(host =>
            {
                var name = host.Split(':')[0];
                return name is "localhost" or "127.0.0.1" or "::1";
            });

    private IProducer<string, byte[]> GetOrCreateSharedProducer(bool enableIdempotence)
    {
        return _sharedProducers.GetOrAdd(enableIdempotence, idempotent =>
        {
            var producerConfig = new ProducerConfig(_kafkaOptions.BaseProducerConfig)
            {
                BootstrapServers = _kafkaOptions.BootstrapServers,
                Acks = Acks.All,
                EnableIdempotence = idempotent
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
        var dlqProducer = GetOrCreateSharedProducer(enableIdempotence: true);

        // Track the group's metadata so transactions can commit this group's offsets.
        _groupMetadata[config.GroupId] = confluentConsumer.ConsumerGroupMetadata;

        var wrapper = new KafkaConsumer<T>(
            confluentConsumer, dlqProducer, topic, _kafkaOptions.DlqSuffix,
            _loggerFactory?.CreateLogger<KafkaConsumer<T>>(),
            bufferCapacity: options.BufferCapacity > 0 ? options.BufferCapacity : 100,
            includeDlqExceptionDetails: _includeDlqExceptionDetails);

        _trackedConsumers.Add(wrapper);
        return Task.FromResult<IConsumer<T>>(wrapper);
    }

    /// <summary>
    /// Creates a new producer for a specific topic.
    /// </summary>
    public Task<IProducer<T>> CreateProducerAsync<T>(
        string topic,
        ProducerOptions options,
        CancellationToken ct = default)
    {
        var rawProducer = GetOrCreateSharedProducer(options.EnableIdempotence);
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

    public async ValueTask DisposeAsync()
    {
        while (_trackedConsumers.TryTake(out var consumer))
        {
            try
            {
                await consumer.DisposeAsync();
            }
            catch
            {
                // Ignore cleanup errors during shutdown
            }
        }

        foreach (var producer in _sharedProducers.Values)
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
        _sharedProducers.Clear();

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
    }
}
