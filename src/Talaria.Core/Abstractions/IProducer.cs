namespace Talaria.Core.Abstractions;

/// <summary>
/// Produces messages to a topic.
/// </summary>
/// <typeparam name="T">The CLR message type the producer serializes.</typeparam>
/// <remarks>
/// Producers are created by <see cref="ITransport.CreateProducerAsync{T}"/> and are
/// typically cached per (topic, message type) — Talaria's hosted services reuse a single
/// producer instance for the lifetime of the host rather than creating one per message.
/// </remarks>
/// <since>1.0.0</since>
public interface IProducer<T> : IAsyncDisposable
{
    /// <summary>
    /// Publishes a message to the configured topic.
    /// </summary>
    /// <param name="message">The payload to serialize and publish.</param>
    /// <param name="headers">
    /// Optional headers (W3C Trace Context, OTel Baggage, Talaria metadata). When null, an
    /// empty <see cref="MessageHeaders"/> is used; Talaria will not synthesize a MessageId
    /// for the caller.
    /// </param>
    /// <param name="partitionKey">
    /// Optional partition key. Transports with keyed partitioning route to a deterministic
    /// partition; ignored by transports without partition semantics.
    /// </param>
    /// <param name="ct">Cancellation token; cancels the in-flight publish.</param>
    /// <remarks>
    /// At-least-once: a successful return means the broker accepted the message, but
    /// redelivery or duplicates are still possible. Downstream consumers dedupe via the
    /// idempotency store keyed by <c>MessageHeaders.MessageId</c>.
    /// </remarks>
    Task ProduceAsync(
        T message,
        MessageHeaders? headers = null,
        string? partitionKey = null,
        CancellationToken ct = default);
}
