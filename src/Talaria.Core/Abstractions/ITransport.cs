// SPDX-License-Identifier: AGPL-3.0-or-later

namespace Talaria.Core.Abstractions;

/// <summary>
/// Core transport abstraction. Implementations provide the mechanism for
/// consuming from and producing to message channels (topics, queues, etc.).
/// </summary>
public interface ITransport
{
    /// <summary>
    /// Human-readable name of this transport (e.g., "InMemory", "Kafka").
    /// </summary>
    string Name { get; }

    /// <summary>
    /// Creates a consumer that reads messages of type <typeparamref name="T"/> from the specified topic.
    /// </summary>
    Task<IConsumer<T>> CreateConsumerAsync<T>(
        string topic,
        ConsumerOptions options,
        CancellationToken ct = default);

    /// <summary>
    /// Creates a producer that writes messages of type <typeparamref name="T"/>.
    /// </summary>
    Task<IProducer<T>> CreateProducerAsync<T>(
        string topic,
        ProducerOptions options,
        CancellationToken ct = default);

    /// <summary>
    /// Begins a transactional session. When <paramref name="consumerGroup"/> and
    /// <paramref name="offsetSource"/> are provided and the transport supports it,
    /// the consumed message's offset is committed inside the same transaction as the
    /// session's produces (Kafka exactly-once semantics). Disposing an open session aborts it.
    /// </summary>
    Task<ITransactionalSession> BeginTransactionAsync(
        string? consumerGroup = null,
        TransactionOffsetSource? offsetSource = null,
        CancellationToken ct = default);
}
