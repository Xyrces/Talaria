// SPDX-License-Identifier: Apache-2.0

namespace Talaria.Transports.InMemory;

/// <summary>
/// Configuration options for the in-memory transport.
/// </summary>
/// <since>1.0.0</since>
public sealed class InMemoryTransportOptions
{
    /// <summary>
    /// Maximum number of messages buffered per topic channel. Must be greater than zero.
    /// </summary>
    /// <remarks>
    /// When the channel is full, the oldest message is dropped (newest is preserved)
    /// for regular topics. DLQ topics are unbounded so dead letters are never lost.
    /// </remarks>
    public int ChannelCapacity { get; set; } = 1000;

    /// <summary>
    /// Artificial latency injected per message for realistic testing.
    /// Set to TimeSpan.Zero for maximum speed. Must not be negative.
    /// </summary>
    public TimeSpan SimulatedLatency { get; set; } = TimeSpan.Zero;

    /// <summary>
    /// Suffix appended to a topic name to form its dead-letter queue topic.
    /// </summary>
    public string DlqSuffix { get; set; } = ".dlq";
}
