// SPDX-License-Identifier: AGPL-3.0-or-later

using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Talaria.Core.Abstractions;
using Talaria.Core.Registration;

namespace Talaria.Transports.AzureServiceBus.Deferral;

/// <summary>
/// Extension methods for registering the Azure Service Bus <see cref="DeferralAdapter"/>
/// as the engine's <see cref="IDeferralStore"/>. Requires a <see cref="ServiceBusClient"/>
/// in the DI container (typically registered by the ASB transport itself on
/// <c>UseAzureServiceBusTransport()</c>) plus an existing durable deferral store
/// (typically <c>UseInMemoryDeferralStore()</c> or <c>UseRedisDeferralStore()</c>) that
/// backs long-term deferrals.
/// </summary>
public static class AzureServiceBusDeferralExtensions
{
    /// <summary>
    /// Registers the deferral adapter with default thresholds
    /// (<see cref="DeferralAdapterOptions.ShortTermCutoff"/> = 10 min,
    /// <see cref="DeferralAdapterOptions.MaxPayloadBytes"/> = 256 KB).
    /// </summary>
    public static TalariaBuilder UseAzureServiceBusDeferral(this TalariaBuilder builder)
        => builder.UseAzureServiceBusDeferral(_ => { });

    /// <summary>
    /// Registers the deferral adapter with custom thresholds. The adapter is the only
    /// <see cref="IDeferralStore"/> implementation the engine resolves for saga message
    /// deferrals; the durable store remains responsible for the long path and for all
    /// lease/sweeper traffic.
    /// </summary>
    public static TalariaBuilder UseAzureServiceBusDeferral(
        this TalariaBuilder builder,
        Action<DeferralAdapterOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);

        var adapterOptions = new DeferralAdapterOptions();
        configure(adapterOptions);

        // Replace any IDeferralStore already registered (Redis or InMemory) with the
        // adapter; the adapter itself takes the previous store as its long-term
        // backing store.
        Microsoft.Extensions.DependencyInjection.Extensions.ServiceCollectionDescriptorExtensions.RemoveAll(builder.Services, typeof(IDeferralStore));

        builder.Services.AddSingleton(adapterOptions);
        builder.Services.AddSingleton<IServiceBusMessageScheduler>(sp =>
            new ServiceBusMessageScheduler(sp.GetRequiredService<ServiceBusClient>()));
        builder.Services.TryAddSingleton(TimeProvider.System);

        builder.Services.AddSingleton<IDeferralStore>(sp => new DeferralAdapter(
            sp.GetRequiredService<IServiceBusMessageScheduler>(),
            ResolveLongTermStore(sp),
            sp.GetRequiredService<DeferralAdapterOptions>(),
            sp.GetRequiredService<Microsoft.Extensions.Options.IOptions<Talaria.Core.TalariaOptions>>(),
            sp.GetService<TimeProvider>()));

        return builder;
    }

    /// <summary>
    /// Reads the durable store the host registered before the adapter was added
    /// (typically UseInMemoryDeferralStore or UseRedisDeferralStore). Required:
    /// adapter-only deployments are not supported because the durable path owns
    /// the lease/sweeper traffic.
    /// </summary>
    /// <exception cref="InvalidOperationException">No durable IDeferralStore is registered.</exception>
    private static IDeferralStore ResolveLongTermStore(IServiceProvider sp)
    {
        var existing = sp.GetService<IDeferralStore>();
        if (existing is not null)
        {
            return existing;
        }

        throw new InvalidOperationException(
            "UseAzureServiceBusDeferral() requires a durable IDeferralStore. Register one via UseInMemoryDeferralStore() or UseRedisDeferralStore() before calling the adapter extension.");
    }
}
