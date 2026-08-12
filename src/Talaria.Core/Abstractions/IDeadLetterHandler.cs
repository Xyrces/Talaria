// SPDX-License-Identifier: AGPL-3.0-or-later

namespace Talaria.Core.Abstractions;

/// <summary>
/// A message that the transport routed to its native dead-letter queue
/// (Azure Service Bus's built-in DLQ, SQS DLQ, etc.), surfaced to the host
/// so a custom handler can inspect, replay, or alert on it.
/// </summary>
/// <param name="SourceEntity">
/// Name of the source queue or subscription the message was dead-lettered
/// from. Used to route the dead-letter back to its originating topic when
/// the handler chooses to replay.
/// </param>
/// <param name="DeadLetterReason">
/// Transport-native reason string (e.g. "MaxDeliveryCountExceeded",
/// "Expired"). The host's <see cref="IDeadLetterHandler"/> implementation
/// can switch on this to choose between alert, replay, and quarantine.
/// </param>
/// <param name="DeliveryCount">
/// Number of delivery attempts the source entity made before dead-lettering.
/// Useful for distinguishing poison messages (delivery=1 with deserialization
/// failure) from genuinely transient failures (delivery&gt;MaxDeliveryCount).
/// </param>
/// <param name="Payload">
/// The deserialized message payload, or null when the transport could not
/// deserialize it (raw JSON is preserved in <see cref="RawPayloadJson"/>).
/// </param>
/// <param name="RawPayloadJson">
/// The raw payload JSON exactly as it arrived on the source entity.
/// Available even when <see cref="Payload"/> is null because deserialization
/// failed.
/// </param>
/// <param name="Headers">
/// Headers carried by the dead-lettered message, including the standard
/// Talaria metadata (<c>DlqReason</c>, <c>DlqException</c>, etc.) when the
/// engine had a chance to populate them before dead-lettering.
/// </param>
/// <param name="DeadLetteredAt">
/// Timestamp the transport recorded the dead-letter event.
/// </param>
/// <since>1.0.0</since>
public sealed record NativeDeadLetterEnvelope(
    string SourceEntity,
    string DeadLetterReason,
    int DeliveryCount,
    object? Payload,
    string RawPayloadJson,
    MessageHeaders Headers,
    DateTimeOffset DeadLetteredAt);

/// <summary>
/// Extension point the host can register to receive messages the transport
/// itself routed to its native dead-letter queue. Distinct from Talaria's
/// own <c>*.dlq</c> topics — the host's <c>IConsumer.NackAsync</c> writes to
/// those, while <see cref="IDeadLetterHandler"/> consumes from whatever the
/// transport defines as its DLQ (Azure Service Bus's per-entity DLQ, SQS
/// DLQ, etc.).
/// </summary>
/// <remarks>
/// Transports without a native DLQ simply never invoke the registered
/// handler — the engine's existing <c>NackAsync</c>-based routing covers
/// those paths. The interface exists to bridge transports that have a
/// first-class DLQ concept that the host should observe and react to
/// (alerting, replay tools, metrics counters). Implementations are expected
/// to be quick — they run on the consumer's hot path between dead-letter
/// detection and the transport moving on to the next message. Long-running
/// work (writing to a quarantine store, paging on-call) should be
/// dispatched onto a background queue inside the handler.
/// </remarks>
/// <since>1.0.0</since>
public interface IDeadLetterHandler
{
    /// <summary>
    /// Invoked once per native dead-letter the transport observes. Hosts
    /// may register exactly one handler; transports that surface a single
    /// DLQ for the whole namespace deliver a single <see cref="NativeDeadLetterEnvelope"/>
    /// per message, with <see cref="NativeDeadLetterEnvelope.SourceEntity"/>
    /// identifying the origin.
    /// </summary>
    /// <param name="envelope">The dead-lettered message and its metadata.</param>
    /// <param name="ct">Cancellation token.</param>
    Task HandleAsync(NativeDeadLetterEnvelope envelope, CancellationToken ct = default);
}

/// <summary>
/// No-op <see cref="IDeadLetterHandler"/> used when no native-DLQ observer is
/// registered. The transport detects and acknowledges dead-letters, but no
/// host logic runs in response.
/// </summary>
/// <since>1.0.0</since>
public sealed class NullDeadLetterHandler : IDeadLetterHandler
{
    /// <summary>The shared singleton instance.</summary>
    public static readonly NullDeadLetterHandler Instance = new();

    private NullDeadLetterHandler() { }

    /// <inheritdoc />
    public Task HandleAsync(NativeDeadLetterEnvelope envelope, CancellationToken ct = default)
        => Task.CompletedTask;
}
