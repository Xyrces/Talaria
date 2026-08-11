// SPDX-License-Identifier: AGPL-3.0-or-later

namespace Talaria.Core.Abstractions;

/// <summary>
/// Core transport abstraction. Implementations provide the mechanism for
/// consuming from and producing to message channels (topics, queues, etc.).
/// </summary>
/// <remarks>
/// Implementations are registered as singletons in the DI container; the container
/// owns their lifecycle and disposes them on host shutdown. Talaria ships
/// in-memory and Kafka transports; the abstraction is also the contract test
/// surface for any custom provider.
/// </remarks>
/// <since>1.0.0</since>
public interface ITransport
{
    /// <summary>
    /// Human-readable name of this transport (e.g., "InMemory", "Kafka").
    /// </summary>
    /// <remarks>Used in logs, metrics tags, and DLQ routing decisions.</remarks>
    string Name { get; }

    /// <summary>
    /// Creates a consumer that reads messages of type <typeparamref name="T"/> from the specified topic.
    /// </summary>
    /// <typeparam name="T">The CLR message type the consumer will deserialize each envelope into.</typeparam>
    /// <param name="topic">The topic name to subscribe to. Naming is transport-specific.</param>
    /// <param name="options">Consumer tuning (consumer group, buffer capacity, etc.).</param>
    /// <param name="ct">Cancellation token; cancels the consumer subscription setup.</param>
    /// <returns>A consumer that must be disposed by the caller when finished.</returns>
    Task<IConsumer<T>> CreateConsumerAsync<T>(
        string topic,
        ConsumerOptions options,
        CancellationToken ct = default);

    /// <summary>
    /// Creates a producer that writes messages of type <typeparamref name="T"/>.
    /// </summary>
    /// <typeparam name="T">The CLR message type the producer will serialize.</typeparam>
    /// <param name="topic">The topic name to produce to. Naming is transport-specific.</param>
    /// <param name="options">Producer tuning (idempotence, etc.).</param>
    /// <param name="ct">Cancellation token; cancels producer creation.</param>
    /// <returns>A producer that must be disposed by the caller when finished.</returns>
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
    /// <param name="consumerGroup">
    /// Optional consumer group whose offset should join the transaction. When null, the session
    /// is produce-only (InMemory buffers produces until commit; Kafka produces join the txn).
    /// </param>
    /// <param name="offsetSource">
    /// The exact (topic, partition, offset) of the message whose offset should commit in the
    /// transaction. Must be supplied together with <paramref name="consumerGroup"/> to enable
    /// exactly-once semantics; ignored otherwise.
    /// </param>
    /// <param name="ct">Cancellation token; cancels session creation.</param>
    /// <returns>An open session that must be committed or aborted by the caller.</returns>
    /// <remarks>
    /// Saga state stores (Redis/InMemory) do NOT participate in this transaction — a crash
    /// between the state save and the transaction commit replays the message against
    /// transitioned state, so saga step handlers must be idempotent.
    /// </remarks>
    Task<ITransactionalSession> BeginTransactionAsync(
        string? consumerGroup = null,
        TransactionOffsetSource? offsetSource = null,
        CancellationToken ct = default);
}
