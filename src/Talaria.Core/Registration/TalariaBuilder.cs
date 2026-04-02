using Microsoft.Extensions.DependencyInjection;
using Talaria.Core.Abstractions;

namespace Talaria.Core.Registration;

/// <summary>
/// Fluent builder for configuring Talaria messaging services.
/// Returned by <see cref="TalariaServiceExtensions.AddTalaria"/>.
/// </summary>
public sealed class TalariaBuilder
{
    public IServiceCollection Services { get; }

    internal TalariaBuilder(IServiceCollection services)
    {
        Services = services;
    }

    /// <summary>
    /// Registers a transport implementation.
    /// </summary>
    public TalariaBuilder UseTransport<TTransport>() where TTransport : class, ITransport
    {
        Services.AddSingleton<ITransport, TTransport>();
        return this;
    }

    /// <summary>
    /// Registers a transport instance directly.
    /// </summary>
    public TalariaBuilder UseTransport(ITransport transport)
    {
        Services.AddSingleton(transport);
        return this;
    }

    /// <summary>
    /// Configures global Talaria options.
    /// </summary>
    public TalariaBuilder Configure(Action<TalariaOptions> configure)
    {
        Services.Configure(configure);
        return this;
    }

    /// <summary>
    /// Registers a global idempotency store checking deduplication across all message consumption.
    /// </summary>
    public TalariaBuilder UseIdempotencyStore<TStore>() where TStore : class, IIdempotencyStore
    {
        Services.AddSingleton<IIdempotencyStore, TStore>();
        return this;
    }

    /// <summary>
    /// Registers a global idempotency store instance directly.
    /// </summary>
    public TalariaBuilder UseIdempotencyStore(IIdempotencyStore store)
    {
        Services.AddSingleton(store);
        return this;
    }
}
