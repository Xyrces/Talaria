using Confluent.Kafka;

namespace Talaria.Transports.Kafka;

/// <summary>
/// Configuration for the Talaria Kafka transport.
/// </summary>
public sealed class KafkaTransportOptions
{
    /// <summary>
    /// Broker connection string (e.g., "localhost:9092").
    /// </summary>
    public string BootstrapServers { get; set; } = string.Empty;

    /// <summary>
    /// Base configuration applied to all internal consumers.
    /// Use this to configure SASL, security protocols, etc.
    /// </summary>
    public ConsumerConfig BaseConsumerConfig { get; set; } = new();

    /// <summary>
    /// Base configuration applied to all internal producers.
    /// Use this to configure SASL, security protocols, etc.
    /// </summary>
    public ProducerConfig BaseProducerConfig { get; set; } = new();

    /// <summary>
    /// Time to wait for producer flush on shutdown.
    /// </summary>
    public TimeSpan FlushTimeout { get; set; } = TimeSpan.FromSeconds(5);
}
