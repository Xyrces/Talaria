namespace Talaria.Transports.InMemory;

/// <summary>
/// Configuration options for the in-memory transport.
/// </summary>
public sealed class InMemoryTransportOptions
{
    /// <summary>
    /// Maximum number of messages buffered per topic channel. Must be greater than zero.
    /// </summary>
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
