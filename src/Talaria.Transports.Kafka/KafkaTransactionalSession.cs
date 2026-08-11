// SPDX-License-Identifier: AGPL-3.0-or-later

using Confluent.Kafka;
using Talaria.Core.Abstractions;

namespace Talaria.Transports.Kafka;

/// <summary>
/// A real Kafka transaction (KIP-98): produces and the consumed message's offset
/// commit atomically. Uses a pooled transactional producer (stable TransactionalId
/// for zombie fencing) that is returned to the pool on dispose.
/// Disposing an open session aborts the transaction.
/// </summary>
internal sealed class KafkaTransactionalSession : ITransactionalSession
{
    private static readonly TimeSpan TransactionTimeout = TimeSpan.FromSeconds(30);

    private readonly KafkaTransport _transport;
    private readonly IProducer<string, byte[]> _producer;
    private readonly string? _consumerGroup;
    private readonly TransactionOffsetSource? _offsetSource;
    private bool _completed;

    public KafkaTransactionalSession(
        KafkaTransport transport,
        IProducer<string, byte[]> producer,
        string? consumerGroup,
        TransactionOffsetSource? offsetSource)
    {
        _transport = transport;
        _producer = producer;
        _consumerGroup = consumerGroup;
        _offsetSource = offsetSource;

        _producer.BeginTransaction();
    }

    public Task<IProducer<T>> GetProducerAsync<T>(string topic, CancellationToken ct = default)
    {
        ThrowIfCompleted();
        return Task.FromResult<IProducer<T>>(new KafkaProducer<T>(_producer, topic));
    }

    public Task CommitAsync(CancellationToken ct = default)
    {
        ThrowIfCompleted();

        if (_offsetSource is not null && _consumerGroup is not null)
        {
            var metadata = _transport.GetConsumerGroupMetadata(_consumerGroup)
                ?? throw new InvalidOperationException(
                    $"Cannot commit offsets in transaction: no active consumer is registered for group '{_consumerGroup}'.");

            // Offset + 1: the position to resume from, i.e. the consumed message is done.
            _producer.SendOffsetsToTransaction(
                new[]
                {
                    new TopicPartitionOffset(_offsetSource.Topic, _offsetSource.Partition, _offsetSource.Offset + 1)
                },
                metadata,
                TransactionTimeout);
        }

        // CommitTransaction blocks until all outstanding messages are delivered,
        // then ends the transaction with commit markers.
        _producer.CommitTransaction(TransactionTimeout);
        _completed = true;
        return Task.CompletedTask;
    }

    public Task AbortAsync(CancellationToken ct = default)
    {
        ThrowIfCompleted();
        _producer.AbortTransaction(TransactionTimeout);
        _completed = true;
        return Task.CompletedTask;
    }

    public ValueTask DisposeAsync()
    {
        if (!_completed)
        {
            try
            {
                _producer.AbortTransaction(TransactionTimeout);
            }
            catch
            {
                // Best effort — the transaction times out broker-side regardless.
            }
        }

        _transport.ReturnTransactionalProducer(_producer);
        return ValueTask.CompletedTask;
    }

    private void ThrowIfCompleted()
    {
        if (_completed)
        {
            throw new InvalidOperationException("The transaction has already been committed or aborted.");
        }
    }
}
