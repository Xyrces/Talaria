namespace Talaria.Transports.InMemory;

/// <summary>
/// Configuration options for the in-memory transport.
/// </summary>
public sealed class InMemoryTransportOptions
{
    /// <summary>
    /// Maximum number of messages buffered per topic channel.
    /// </summary>
    public int ChannelCapacity { get; set; } = 1000;

    /// <summary>
    /// Artificial latency injected per message for realistic testing.
    /// Set to TimeSpan.Zero for maximum speed.
    /// </summary>
    public TimeSpan SimulatedLatency { get; set; } = TimeSpan.Zero;
}
