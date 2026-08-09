// SPDX-License-Identifier: AGPL-3.0-or-later

using Microsoft.Extensions.DependencyInjection;
using Talaria.Core.Abstractions;

namespace Talaria.Core.Registration;

/// <summary>
/// Fluent builder for configuring Talaria messaging services.
/// Returned by <see cref="TalariaServiceExtensions.AddTalaria(IServiceCollection)"/>.
/// </summary>
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
    public TalariaBuilder UseTransport<TTransport>() where TTransport : class, ITransport
    {
        Services.AddSingleton<ITransport, TTransport>();
        return this;
    }

    /// <summary>
    /// Registers a transport via a factory. The container disposes the created instance
    /// on shutdown. Prefer this over the instance overload.
    /// </summary>
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
