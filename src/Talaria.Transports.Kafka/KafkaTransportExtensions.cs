using Microsoft.Extensions.DependencyInjection;
using Talaria.Core.Registration;

namespace Talaria.Transports.Kafka;

/// <summary>
/// Extensions for registering Kafka transport with Talaria.
/// </summary>
public static class KafkaTransportExtensions
{
    /// <summary>
    /// Configures Talaria to use the Kafka transport.
    /// </summary>
    public static TalariaBuilder UseKafkaTransport(
        this TalariaBuilder builder,
        Action<KafkaTransportOptions> configure)
    {
        var options = new KafkaTransportOptions();
        configure(options);
        return builder.UseTransport(new KafkaTransport(options));
    }
}
