namespace Talaria.Core.Abstractions;

/// <summary>
/// A saga message that is waiting for its scheduled (re)delivery time.
/// </summary>
/// <param name="Id">Unique identifier of this deferred entry.</param>
/// <param name="Topic">The topic the message must be republished to.</param>
/// <param name="MessageType">Assembly-qualified CLR type name of the payload, used to resolve the deserializer and producer.</param>
/// <param name="PayloadJson">The message payload serialized as JSON.</param>
/// <param name="Headers">Headers to republish with the message (deferral attempt, minted message id, trace context).</param>
/// <param name="CorrelationId">The saga correlation id, if one was resolved.</param>
/// <param name="Attempt">The deferral attempt number (1-based).</param>
/// <param name="DueAt">When the message becomes eligible for republishing.</param>
public sealed record DeferredMessage(
    Guid Id,
    string Topic,
    string MessageType,
    string PayloadJson,
    MessageHeaders Headers,
    string? CorrelationId,
    int Attempt,
    DateTimeOffset DueAt);

/// <summary>
/// Durable store for deferred saga messages (out-of-order arrivals and handler-initiated
/// deferrals). Unlike the previous in-process delay, entries survive restarts and can be
/// swept by any node of the application.
/// <para>
/// Claiming semantics: <see cref="PopDueAsync"/> atomically claims the returned messages —
/// a second caller (another node or a concurrent sweep) will not see them again.
/// <see cref="CompleteAsync"/> confirms successful republishing and is a no-op for stores
/// that remove entries on pop (the built-in stores do); <see cref="RequeueAsync"/>
/// reschedules a message whose republication failed.
/// </para>
/// </summary>
public interface IDeferralStore
{
    /// <summary>Schedules a message for delivery at <see cref="DeferredMessage.DueAt"/>.</summary>
    Task EnqueueAsync(DeferredMessage message, CancellationToken ct = default);

    /// <summary>
    /// Atomically claims up to <paramref name="maxBatch"/> messages due at or before
    /// <paramref name="now"/>. Claimed messages are invisible to subsequent pops.
    /// </summary>
    Task<IReadOnlyList<DeferredMessage>> PopDueAsync(DateTimeOffset now, int maxBatch, CancellationToken ct = default);

    /// <summary>Confirms a claimed message was republished successfully.</summary>
    Task CompleteAsync(Guid id, CancellationToken ct = default);

    /// <summary>Reschedules a claimed message after a failed republication attempt.</summary>
    Task RequeueAsync(DeferredMessage message, DateTimeOffset newDueAt, CancellationToken ct = default);
}
