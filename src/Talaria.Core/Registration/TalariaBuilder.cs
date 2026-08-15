// SPDX-License-Identifier: Apache-2.0

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Talaria.Core.Abstractions;
using Talaria.Core.Requesting;

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

    /// <summary>
    /// Registers a typed request client and the shared <see cref="RequestClientFactory"/> singleton.
    /// </summary>
    /// <typeparam name="TRequest">The CLR request type.</typeparam>
    /// <param name="topic">The topic to which requests are published.</param>
    /// <returns>The same builder, for chaining.</returns>
    /// <remarks>
    /// The factory is registered as a singleton and disposed by the container on shutdown.
    /// Multiple calls for different request types share the same factory instance and inbox pump.
    /// </remarks>
    public TalariaBuilder AddRequestClient<TRequest>(string topic)
        where TRequest : class
    {
        Services.AddSingleton<RequestClientFactory>(sp =>
        {
            var transport = sp.GetRequiredService<ITransport>();
            var options = sp.GetRequiredService<IOptions<TalariaOptions>>().Value;
            var loggerFactory = sp.GetRequiredService<ILoggerFactory>();
            var provisioner = sp.GetService<ITopologyProvisioner>();
            return new RequestClientFactory(transport, options, loggerFactory, provisioner);
        });

        Services.AddSingleton<IRequestClient<TRequest>>(sp =>
        {
            var factory = sp.GetRequiredService<RequestClientFactory>();
            return factory.CreateClient<TRequest>(topic);
        });

        return this;
    }
}
