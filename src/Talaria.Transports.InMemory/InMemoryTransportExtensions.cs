using Microsoft.Extensions.DependencyInjection;
using Talaria.Core.Registration;

namespace Talaria.Transports.InMemory;

/// <summary>
/// Extension methods for configuring the in-memory transport.
/// </summary>
public static class InMemoryTransportExtensions
{
    /// <summary>
    /// Configures Talaria to use the in-memory transport.
    /// </summary>
    public static TalariaBuilder UseInMemoryTransport(this TalariaBuilder builder)
    {
        return builder.UseTransport(new InMemoryTransport());
    }

    /// <summary>
    /// Configures Talaria to use the in-memory transport with options.
    /// </summary>
    public static TalariaBuilder UseInMemoryTransport(
        this TalariaBuilder builder,
        Action<InMemoryTransportOptions> configure)
    {
        var options = new InMemoryTransportOptions();
        configure(options);
        return builder.UseTransport(new InMemoryTransport(options));
    }

    /// <summary>
    /// Configures Talaria to use the in-memory transport with a specific instance
    /// (allows sharing the transport for test assertions).
    /// </summary>
    public static TalariaBuilder UseInMemoryTransport(
        this TalariaBuilder builder,
        InMemoryTransport transport)
    {
        Microsoft.Extensions.DependencyInjection.Extensions.ServiceCollectionDescriptorExtensions.TryAddSingleton(
            builder.Services, 
            typeof(Talaria.Core.Abstractions.IStateStore<>), 
            typeof(InMemoryStateStore<>));
            
        return builder.UseTransport(transport);
    }
}
