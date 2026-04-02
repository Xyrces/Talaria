namespace Talaria.Core.Abstractions;

/// <summary>
/// Consumes messages of type <typeparamref name="T"/> from a topic.
/// </summary>
public interface IConsumer<T> : IAsyncDisposable
{
    /// <summary>
    /// Yields messages as they become available. Respects cancellation.
    /// </summary>
    IAsyncEnumerable<MessageEnvelope<T>> ConsumeAsync(CancellationToken ct = default);

    /// <summary>
    /// Commits the offset/acknowledgement for the given message.
    /// Called after successful handler execution.
    /// </summary>
    Task CommitAsync(MessageEnvelope<T> message, CancellationToken ct = default);

    /// <summary>
    /// Negatively acknowledges a message, making it available for redelivery
    /// or routing it to the dead-letter queue based on transport policy.
    /// </summary>
    Task NackAsync(MessageEnvelope<T> message, CancellationToken ct = default);
}
