namespace Talaria.Core.Abstractions;

/// <summary>
/// A centralized abstraction for enforcing exactly-once delivery across a distributed cluster.
/// Tracks physical message IDs globally to prevent redundant concurrent processing and duplicate replays.
/// </summary>
public interface IIdempotencyStore
{
    /// <summary>
    /// Attempts to mark the message ID as processed exclusively. 
    /// If it returns true, no other replica has claimed or processed this message.
    /// In an event-driven setup natively backing Exactly-Once Delivery, this replaces complex local locking.
    /// </summary>
    Task<bool> TryAcquireLockAsync(string messageId, string consumerQueue, TimeSpan expiration, CancellationToken ct = default);

    /// <summary>
    /// Formally seals the message's successful execution, converting the transient lock into a long-lasting flag.
    /// Subsequent checks against this message ID will permanently bounce.
    /// </summary>
    Task MarkCompleteAsync(string messageId, string consumerQueue, CancellationToken ct = default);

    /// <summary>
    /// Removes the lock ensuring it can be legitimately re-tried (e.g. if the consumer failed internally).
    /// </summary>
    Task ReleaseLockAsync(string messageId, string consumerQueue, CancellationToken ct = default);
}
