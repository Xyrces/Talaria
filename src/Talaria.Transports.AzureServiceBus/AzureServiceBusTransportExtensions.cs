// SPDX-License-Identifier: Apache-2.0

using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Talaria.Core.Abstractions;
using Talaria.Core.Registration;

namespace Talaria.Transports.AzureServiceBus;

/// <summary>
/// Extension methods for registering the Azure Service Bus transport with
/// Talaria.
/// </summary>
/// <since>1.0.0</since>
public static class AzureServiceBusTransportExtensions
{
    /// <summary>
    /// Configures Talaria to use the Azure Service Bus transport. The
    /// transport is created by the DI container (with logging wired in),
    /// which therefore owns its disposal on shutdown. A
    /// <see cref="ServiceBusClient"/> is also registered as a singleton so
    /// sibling extensions (e.g. <c>UseAzureServiceBusDeferral</c>) can
    /// share the same AMQP connection.
    /// </summary>
    /// <param name="builder">The Talaria builder.</param>
    /// <param name="configure">Callback that mutates the transport options.</param>
    /// <returns>The same <paramref name="builder"/>, for chaining.</returns>
    public static TalariaBuilder UseAzureServiceBusTransport(
        this TalariaBuilder builder,
        Action<AzureServiceBusTransportOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);

        var options = new AzureServiceBusTransportOptions();
        configure(options);

        // Singleton client so deferral adapters and any other consumer of the
        // ServiceBusClient type share one AMQP connection.
        builder.Services.AddSingleton(_ => new ServiceBusClient(
            string.IsNullOrWhiteSpace(options.ConnectionString)
                ? options.FullyQualifiedNamespace ?? throw new InvalidOperationException(
                    $"{nameof(AzureServiceBusTransportOptions.ConnectionString)} or {nameof(AzureServiceBusTransportOptions.FullyQualifiedNamespace)} is required.")
                : options.ConnectionString));

        builder.Services.AddSingleton(options);

        builder.Services.AddSingleton<ITransport>(sp =>
        {
            var loggerFactory = sp.GetService<ILoggerFactory>();
            var includeDetails = sp.GetService<Microsoft.Extensions.Options.IOptions<Talaria.Core.TalariaOptions>>()?.Value.IncludeExceptionDetailsInDlq ?? false;
            return new AzureServiceBusTransport(options, loggerFactory, includeDetails);
        });

        return builder;
    }

    /// <summary>
    /// Configures Talaria to use the Azure Service Bus transport with the
    /// supplied connection string. Convenience overload for the common case
    /// where the host has a connection string from configuration and does
    /// not need to tune individual options.
    /// </summary>
    /// <param name="builder">The Talaria builder.</param>
    /// <param name="connectionString">
    /// Connection string copied from the Service Bus namespace's "Shared
    /// access policies" blade. Pass <c>UseDevelopmentEnvironment=true</c> for
    /// the local Service Bus emulator (the saga sample default).
    /// </param>
    /// <returns>The same <paramref name="builder"/>, for chaining.</returns>
    public static TalariaBuilder UseAzureServiceBusTransport(
        this TalariaBuilder builder,
        string connectionString)
        => builder.UseAzureServiceBusTransport(opts =>
        {
            opts.ConnectionString = connectionString;
        });

    /// <summary>
    /// Registers a pre-built <see cref="AzureServiceBusTransport"/> instance
    /// directly. Intended for tests that need to share the instance for
    /// assertions — the DI container does NOT dispose externally created
    /// instances; the caller owns their lifecycle.
    /// </summary>
    public static TalariaBuilder UseAzureServiceBusTransport(
        this TalariaBuilder builder,
        AzureServiceBusTransport transport)
    {
        ArgumentNullException.ThrowIfNull(transport);

        // Mirror the singleton client/options pattern so sibling extensions
        // resolve the same AMQP connection as the supplied transport.
        builder.Services.AddSingleton(_ => transport.Client);
        builder.Services.AddSingleton(transport);
        return builder.UseTransport(transport);
    }
}
