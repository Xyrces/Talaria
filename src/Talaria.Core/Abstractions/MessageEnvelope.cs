// SPDX-License-Identifier: AGPL-3.0-or-later

namespace Talaria.Core.Abstractions;

/// <summary>
/// The wire-format wrapper for all messages flowing through Talaria.
/// Carries the payload alongside headers (trace context, baggage) and routing metadata.
/// </summary>
/// <typeparam name="T">The deserialized message payload type.</typeparam>
/// <since>1.0.0</since>
public sealed record MessageEnvelope<T>
{
    /// <summary>
    /// The deserialized message payload.
    /// </summary>
    public required T Payload { get; init; }

    /// <summary>
    /// Headers carrying W3C Trace Context, OTel Baggage, and Talaria metadata.
    /// </summary>
    /// <remarks>
    /// Headers are mutable on the envelope (they are populated by the consumer engine
    /// with DLQ reason, hop count, etc.); producers that re-emit a message must clone
    /// the headers to avoid sharing mutable state with the source delivery.
    /// </remarks>
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
