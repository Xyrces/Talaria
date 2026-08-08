using Microsoft.Extensions.DependencyInjection;
using Talaria.Core.Registration;

namespace Talaria.Transports.Kafka;

/// <summary>
/// Extensions for registering Kafka transport with Talaria.
/// </summary>
public static class KafkaTransportExtensions
{
    /// <summary>
    /// Configures Talaria to use the Kafka transport. The transport is created by the DI
    /// container (with logging wired in), which therefore owns its disposal on shutdown.
    /// </summary>
    public static TalariaBuilder UseKafkaTransport(
        this TalariaBuilder builder,
        Action<KafkaTransportOptions> configure)
    {
        var options = new KafkaTransportOptions();
        configure(options);

        builder.Services.AddSingleton<Talaria.Core.Abstractions.ITransport>(sp =>
            new KafkaTransport(options, sp.GetService<Microsoft.Extensions.Logging.ILoggerFactory>()));

        return builder;
    }
}
