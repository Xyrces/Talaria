// SPDX-License-Identifier: AGPL-3.0-or-later

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
    /// Also registers the in-memory saga state store.
    /// </summary>
    public static TalariaBuilder UseInMemoryTransport(this TalariaBuilder builder)
    {
        return builder.UseInMemoryTransport(_ => { });
    }

    /// <summary>
    /// Configures Talaria to use the in-memory transport with options.
    /// The transport is created by the DI container (which wires
    /// TalariaOptions.IncludeExceptionDetailsInDlq through). Also registers the
    /// in-memory saga state store.
    /// </summary>
    public static TalariaBuilder UseInMemoryTransport(
        this TalariaBuilder builder,
        Action<InMemoryTransportOptions> configure)
    {
        var options = new InMemoryTransportOptions();
        configure(options);
        builder.UseInMemoryStateStore();
        builder.Services.AddSingleton<Talaria.Core.Abstractions.ITransport>(sp =>
            new InMemoryTransport(
                options,
                sp.GetService<Microsoft.Extensions.Options.IOptions<Talaria.Core.TalariaOptions>>()?.Value.IncludeExceptionDetailsInDlq ?? false));
        return builder;
    }

    /// <summary>
    /// Configures Talaria to use a specific in-memory transport instance
    /// (allows sharing the transport for test assertions). Intended for tests:
    /// the DI container does not own or dispose externally created instances.
    /// Also registers the in-memory saga state store.
    /// </summary>
    public static TalariaBuilder UseInMemoryTransport(
        this TalariaBuilder builder,
        InMemoryTransport transport)
    {
        builder.UseInMemoryStateStore();
        return builder.UseTransport(transport);
    }

    /// <summary>
    /// Configures Talaria to use the in-memory saga state store.
    /// Also registers the in-memory transactional outbox: saga state transitions stage
    /// their outbound messages atomically (under one lock) with the state write.
    /// </summary>
    public static TalariaBuilder UseInMemoryStateStore(this TalariaBuilder builder)
    {
        Microsoft.Extensions.DependencyInjection.Extensions.ServiceCollectionDescriptorExtensions.TryAddSingleton(
            builder.Services,
            typeof(InMemoryOutboxStore));
        // Factory, not a second singleton: the state stores and the relay must share
        // the same outbox instance.
        Microsoft.Extensions.DependencyInjection.Extensions.ServiceCollectionDescriptorExtensions.TryAddSingleton<Talaria.Core.Abstractions.IOutboxStore>(
            builder.Services,
            sp => sp.GetRequiredService<InMemoryOutboxStore>());
        Microsoft.Extensions.DependencyInjection.Extensions.ServiceCollectionDescriptorExtensions.TryAddSingleton(
            builder.Services,
            typeof(Talaria.Core.Abstractions.IStateStore<>),
            typeof(InMemoryStateStore<>));
        return builder;
    }

    /// <summary>
    /// Configures Talaria to use the in-memory idempotency store.
    /// </summary>
    public static TalariaBuilder UseInMemoryIdempotencyStore(this TalariaBuilder builder)
    {
        builder.Services.AddSingleton<Talaria.Core.Abstractions.IIdempotencyStore, InMemoryIdempotencyStore>();
        return builder;
    }

    /// <summary>
    /// Configures Talaria to use the in-memory deferral store for saga deferrals.
    /// Entries do not survive a process restart — use a durable store in production.
    /// </summary>
    public static TalariaBuilder UseInMemoryDeferralStore(this TalariaBuilder builder)
    {
        Microsoft.Extensions.DependencyInjection.Extensions.ServiceCollectionDescriptorExtensions.TryAddSingleton<
            Talaria.Core.Abstractions.IDeferralStore, InMemoryDeferralStore>(builder.Services);
        return builder;
    }
}
