// SPDX-License-Identifier: Apache-2.0

namespace Talaria.Core.Abstractions;

/// <summary>
/// Consumes messages of type <typeparamref name="T"/> from a topic.
/// </summary>
/// <typeparam name="T">The CLR message type the consumer deserializes each envelope into.</typeparam>
/// <remarks>
/// Created by <see cref="ITransport.CreateConsumerAsync{T}"/>. Each consumer belongs to a single
/// topic + consumer-group pair; sharing consumers across groups is not supported. Consumers are
/// <see cref="IAsyncDisposable"/> — disposing them stops iteration and releases the underlying
/// transport resources.
/// </remarks>
/// <since>1.0.0</since>
public interface IConsumer<T> : IAsyncDisposable
{
    /// <summary>
    /// Yields messages as they become available. Respects cancellation.
    /// </summary>
    /// <param name="ct">Cancellation token; stops iteration on cancel.</param>
    /// <returns>
    /// An async sequence of <see cref="MessageEnvelope{T}"/>. Offsets are NOT committed by
    /// iteration alone — the caller must invoke <see cref="CommitAsync"/> after successful
    /// handling or <see cref="NackAsync"/> to route to the DLQ.
    /// </returns>
    /// <remarks>
    /// The returned sequence is single-consumer; concurrent iteration is not supported.
    /// Each <see cref="IConsumer{T}"/> instance may be enumerated exactly once: a second
    /// call to <c>ConsumeAsync</c> on the same instance throws <see cref="InvalidOperationException"/>.
    /// Callers that need to restart consumption must create a new consumer via
    /// <see cref="ITransport.CreateConsumerAsync{T}"/>.
    /// Transports may buffer ahead per <see cref="ConsumerOptions.BufferCapacity"/>.
    /// </remarks>
    /// <exception cref="InvalidOperationException">
    /// Thrown when this method is called more than once on the same consumer instance.
    /// </exception>
    IAsyncEnumerable<MessageEnvelope<T>> ConsumeAsync(CancellationToken ct = default);

    /// <summary>
    /// Commits the offset/acknowledgement for the given message.
    /// Called after successful handler execution.
    /// </summary>
    /// <param name="message">The envelope whose offset should be marked processed.</param>
    /// <param name="ct">Cancellation token; cancels the commit.</param>
    /// <remarks>
    /// On Kafka this advances the committed offset of the consumer group; on InMemory it
    /// drops the message from the uncommitted-redelivery set.
    /// </remarks>
    /// <exception cref="Exception">
    /// Implementations surface commit failures as exceptions. A failure leaves the
    /// message eligible for redelivery on the next consumer session; callers must
    /// handle the exception. The engines mark the idempotency record complete once
    /// the handler has succeeded, so a commit failure after successful handling is
    /// safe: the redelivered copy is suppressed by the idempotency gate.
    /// </exception>
    Task CommitAsync(MessageEnvelope<T> message, CancellationToken ct = default);

    /// <summary>
    /// Negatively acknowledges a message, making it available for redelivery
    /// or routing it to the dead-letter queue based on transport policy.
    /// </summary>
    /// <param name="message">The envelope to negatively acknowledge.</param>
    /// <param name="ct">Cancellation token; cancels the nack.</param>
    /// <remarks>
    /// TalariaListener uses NackAsync to route handler exceptions, missing
    /// correlation IDs, and deserialization failures to the dead-letter queue.
    /// The DLQ entity name is derived from the source topic suffixed with the
    /// transport-specific DLQ suffix.
    /// </remarks>
    Task NackAsync(MessageEnvelope<T> message, CancellationToken ct = default);
}
