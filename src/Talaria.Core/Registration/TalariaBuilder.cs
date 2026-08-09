using Microsoft.Extensions.DependencyInjection;
using Talaria.Core.Abstractions;

namespace Talaria.Core.Registration;

/// <summary>
/// Fluent builder for configuring Talaria messaging services.
/// Returned by <see cref="TalariaServiceExtensions.AddTalaria(IServiceCollection)"/>.
/// </summary>
/// <since>1.0.0</since>
public sealed class TalariaBuilder
{
    public IServiceCollection Services { get; }

    internal TalariaBuilder(IServiceCollection services)
    {
        Services = services;
    }

    /// <summary>
    /// Registers a transport implementation. The container creates and disposes it.
    /// </summary>
    /// <typeparam name="TTransport">The <see cref="ITransport"/> implementation type to register as a singleton.</typeparam>
    /// <returns>The same builder, for chaining.</returns>
    public TalariaBuilder UseTransport<TTransport>() where TTransport : class, ITransport
    {
        Services.AddSingleton<ITransport, TTransport>();
        return this;
    }

    /// <summary>
    /// Registers a transport via a factory. The container disposes the created instance
    /// on shutdown. Prefer this over the instance overload.
    /// </summary>
    /// <param name="factory">Factory invoked by the container to construct the transport.</param>
    /// <returns>The same builder, for chaining.</returns>
    public TalariaBuilder UseTransport(Func<IServiceProvider, ITransport> factory)
    {
        Services.AddSingleton(factory);
        return this;
    }

    /// <summary>
    /// Registers a transport instance directly. Intended for tests that need to share
    /// the instance for assertions — the DI container does NOT dispose externally
    /// created instances; the caller owns their lifecycle.
    /// </summary>
    /// <param name="transport">The pre-built transport instance to register.</param>
    /// <returns>The same builder, for chaining.</returns>
    public TalariaBuilder UseTransport(ITransport transport)
    {
        Services.AddSingleton(transport);
        return this;
    }

    /// <summary>
    /// Configures global Talaria options.
    /// </summary>
    /// <param name="configure">A callback that mutates <see cref="TalariaOptions"/>.</param>
    /// <returns>The same builder, for chaining.</returns>
    public TalariaBuilder Configure(Action<TalariaOptions> configure)
    {
        Services.Configure(configure);
        return this;
    }

    /// <summary>
    /// Registers a global idempotency store checking deduplication across all message consumption.
    /// </summary>
    /// <typeparam name="TStore">The <see cref="IIdempotencyStore"/> implementation type to register as a singleton.</typeparam>
    /// <returns>The same builder, for chaining.</returns>
    public TalariaBuilder UseIdempotencyStore<TStore>() where TStore : class, IIdempotencyStore
    {
        Services.AddSingleton<IIdempotencyStore, TStore>();
        return this;
    }

    /// <summary>
    /// Registers a global idempotency store instance directly.
    /// </summary>
    /// <param name="store">The pre-built <see cref="IIdempotencyStore"/> instance to register.</param>
    /// <returns>The same builder, for chaining.</returns>
    /// <remarks>Caller owns lifecycle; the container does not dispose externally created instances.</remarks>
    public TalariaBuilder UseIdempotencyStore(IIdempotencyStore store)
    {
        Services.AddSingleton(store);
        return this;
    }
}
