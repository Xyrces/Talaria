namespace Talaria.Core.Abstractions;

/// <summary>
/// The wire-format wrapper for all messages flowing through Talaria.
/// Carries the payload alongside headers (trace context, baggage) and routing metadata.
/// </summary>
public sealed record MessageEnvelope<T>
{
    /// <summary>
    /// The deserialized message payload.
    /// </summary>
    public required T Payload { get; init; }

    /// <summary>
    /// Headers carrying W3C Trace Context, OTel Baggage, and Talaria metadata.
    /// </summary>
    public MessageHeaders Headers { get; init; } = new();

    /// <summary>
    /// The correlation ID used for saga state lookups.
    /// </summary>
    public string? CorrelationId { get; init; }

    /// <summary>
    /// The source topic this message was consumed from.
    /// </summary>
    public string? SourceTopic { get; init; }

    /// <summary>
    /// The partition key used for routing (if applicable).
    /// </summary>
    public string? PartitionKey { get; init; }

    /// <summary>
    /// The transport partition the message was consumed from (if the transport is partitioned).
    /// </summary>
    public int? Partition { get; init; }

    /// <summary>
    /// Timestamp of when the message was produced.
    /// </summary>
    public DateTimeOffset Timestamp { get; init; } = DateTimeOffset.UtcNow;

    /// <summary>
    /// Transport-specific offset or delivery tag for commit tracking.
    /// </summary>
    public long Offset { get; init; }
}
