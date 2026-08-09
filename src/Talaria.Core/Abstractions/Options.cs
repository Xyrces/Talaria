// SPDX-License-Identifier: AGPL-3.0-or-later

namespace Talaria.Core.Abstractions;

/// <summary>
/// Options for creating a consumer.
/// </summary>
public sealed class ConsumerOptions
{
    /// <summary>
    /// Consumer group identifier. If null, auto-generated as {ApplicationName}.{topic}.
    /// </summary>
    public string? ConsumerGroup { get; set; }

    /// <summary>
    /// Maximum number of messages to buffer in the consumer pipeline.
    /// </summary>
    public int BufferCapacity { get; set; } = 100;
}

/// <summary>
/// Options for creating a producer.
/// </summary>
public sealed class ProducerOptions
{
    /// <summary>
    /// Whether to enable idempotent production (exactly-once guarantee where supported).
    /// </summary>
    public bool EnableIdempotence { get; set; } = true;
}
