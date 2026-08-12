// SPDX-License-Identifier: AGPL-3.0-or-later

namespace Talaria.Core.Abstractions;

/// <summary>
/// Options for creating a consumer.
/// </summary>
/// <since>1.0.0</since>
public sealed class ConsumerOptions
{
    /// <summary>
    /// Consumer group identifier. If null, auto-generated as {ApplicationName}.{topic}.
    /// </summary>
    /// <remarks>
    /// Override per consumer when you want a different offset progression for the same
    /// topic (e.g., a side-channel audit consumer with independent offsets).
    /// </remarks>
    public string? ConsumerGroup { get; set; }

    /// <summary>
    /// Maximum number of messages to buffer in the consumer pipeline.
    /// </summary>
    /// <remarks>
    /// Larger values trade memory for fewer round-trips on the underlying channel.
    /// Transport-specific bounds may apply.
    /// </remarks>
    public int BufferCapacity { get; set; } = 100;
}

/// <summary>
/// Options for creating a producer.
/// </summary>
/// <since>1.0.0</since>
public sealed class ProducerOptions
{
    /// <summary>
    /// Whether to enable idempotent production (exactly-once guarantee where supported).
    /// </summary>
    /// <remarks>
    /// Defaults to true. Disable only when the transport does not support idempotent
    /// production and you accept at-least-once semantics — downstream consumers must
    /// then dedupe via the idempotency store.
    /// </remarks>
    public bool EnableIdempotence { get; set; } = true;
}
