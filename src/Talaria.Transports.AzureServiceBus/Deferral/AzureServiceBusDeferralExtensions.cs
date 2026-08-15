// SPDX-License-Identifier: Apache-2.0

using System.Collections.Generic;
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

        // Capture the durable IDeferralStore the host registered before the adapter was
        // added (typically UseInMemoryDeferralStore() or UseRedisDeferralStore()).
        // The adapter needs that exact instance as its long-term backing store, but
        // resolving it via sp.GetService<IDeferralStore>() after RemoveAll would either
        // return null (the durable was unregistered) or resolve the adapter itself
        // (circular dependency). Capturing the descriptor list before RemoveAll and
        // materialising the durable instance directly from those descriptors breaks
        // the cycle cleanly.
        var priorDescriptors = builder.Services
            .Where(d => d.ServiceType == typeof(IDeferralStore))
            .ToList();

        if (priorDescriptors.Count == 0)
        {
            throw new InvalidOperationException(
                "UseAzureServiceBusDeferral() requires a durable IDeferralStore. Register one via UseInMemoryDeferralStore() or UseRedisDeferralStore() before calling the adapter extension.");
        }

        // Replace any IDeferralStore already registered (Redis or InMemory) with the
        // adapter; the adapter itself takes the previously-captured store as its
        // long-term backing store.
        Microsoft.Extensions.DependencyInjection.Extensions.ServiceCollectionDescriptorExtensions.RemoveAll(builder.Services, typeof(IDeferralStore));

        builder.Services.AddSingleton(adapterOptions);
        // TryAddSingleton (not AddSingleton) so a host or test can pre-register an
        // IServiceBusMessageScheduler fake without triggering duplicate-descriptor
        // errors; the production wiring still registers the real SDK-backed scheduler
        // by default.
        builder.Services.TryAddSingleton<IServiceBusMessageScheduler>(sp =>
            new ServiceBusMessageScheduler(sp.GetRequiredService<ServiceBusClient>()));
        builder.Services.TryAddSingleton(TimeProvider.System);

        builder.Services.AddSingleton<IDeferralStore>(sp => new DeferralAdapter(
            sp.GetRequiredService<IServiceBusMessageScheduler>(),
            ResolvePriorDurableStore(sp, priorDescriptors),
            sp.GetRequiredService<DeferralAdapterOptions>(),
            sp.GetService<TimeProvider>()));

        return builder;
    }

    /// <summary>
    /// Materialises the durable <see cref="IDeferralStore"/> from the descriptor list the
    /// host had registered before <see cref="AzureServiceBusDeferralExtensions.UseAzureServiceBusDeferral(Talaria.Core.Registration.TalariaBuilder, System.Action{DeferralAdapterOptions})"/> was called.
    /// Walks each captured descriptor in order and builds the instance using whichever
    /// registration shape the host chose (instance, type, or factory); never resolves
    /// via <c>sp.GetService&lt;IDeferralStore&gt;()</c>, which would either be circular
    /// (only the adapter is registered now) or null.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// None of the captured descriptors could produce an <see cref="IDeferralStore"/>.
    /// </exception>
    private static IDeferralStore ResolvePriorDurableStore(
        IServiceProvider sp,
        IReadOnlyList<ServiceDescriptor> priorDescriptors)
    {
        ArgumentNullException.ThrowIfNull(sp);
        ArgumentNullException.ThrowIfNull(priorDescriptors);

        var errors = new List<Exception>();
        foreach (var descriptor in priorDescriptors)
        {
            try
            {
                if (descriptor.ImplementationInstance is IDeferralStore existing)
                {
                    return existing;
                }

                if (descriptor.ImplementationType is not null)
                {
                    var instance = ActivatorUtilities.CreateInstance(sp, descriptor.ImplementationType);
                    if (instance is IDeferralStore typed)
                    {
                        return typed;
                    }

                    errors.Add(new InvalidOperationException(
                        $"Descriptor for {descriptor.ImplementationType.FullName} did not produce an IDeferralStore."));
                    continue;
                }

                if (descriptor.ImplementationFactory is not null)
                {
                    var instance = descriptor.ImplementationFactory(sp);
                    if (instance is IDeferralStore factoryed)
                    {
                        return factoryed;
                    }

                    errors.Add(new InvalidOperationException(
                        $"Descriptor factory did not produce an IDeferralStore."));
                    continue;
                }
            }
            catch (Exception ex)
            {
                errors.Add(ex);
            }
        }

        throw new InvalidOperationException(
            "UseAzureServiceBusDeferral() could not resolve the previously-registered durable IDeferralStore. " +
            "Ensure UseInMemoryDeferralStore() or UseRedisDeferralStore() is called before UseAzureServiceBusDeferral().",
            errors.Count == 1 ? errors[0] : new AggregateException(errors));
    }
}
