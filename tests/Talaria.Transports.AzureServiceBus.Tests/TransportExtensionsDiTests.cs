// SPDX-License-Identifier: AGPL-3.0-or-later

using System;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.DependencyInjection;
using Talaria.Core.Abstractions;
using Talaria.Core.Registration;
using Talaria.Transports.AzureServiceBus;
using Xunit;

namespace Talaria.Transports.AzureServiceBus.Tests;

/// <summary>
/// Pins the DI registration contract for the Azure Service Bus transport.
/// Three overloads are exposed; each must wire the right ServiceBusClient,
/// options, and ITransport slot so sibling extensions (deferral adapters,
/// outbox stores) share the same AMQP connection and the host can pick
/// the transport uniformly via <see cref="ITransport"/>.
/// </summary>
public class TransportExtensionsDiTests
{
    [Fact]
    public void UseAzureServiceBusTransport_ConfigureOverload_RegistersOptionsAndClient()
    {
        var services = new ServiceCollection();
        services.AddTalaria().UseAzureServiceBusTransport(opts =>
        {
            opts.ConnectionString = "Endpoint=sb://example/;SharedAccessKeyName=RootManage;SharedAccessKey=KEY";
            opts.DlqSuffix = ".deadletter";
        });

        var provider = services.BuildServiceProvider();

        // ITransport is the singleton the saga host resolves; the
        // configure-callback overload must produce the same type.
        var transport = provider.GetRequiredService<ITransport>();
        Assert.IsType<AzureServiceBusTransport>(transport);
        Assert.Equal("AzureServiceBus", transport.Name);

        // Options are also registered so sibling extensions can resolve
        // them without having to call back into the transport.
        var options = provider.GetRequiredService<AzureServiceBusTransportOptions>();
        Assert.Equal(".deadletter", options.DlqSuffix);

        // The ServiceBusClient is registered as a singleton so the deferral
        // adapter (registered via UseAzureServiceBusDeferral) shares the
        // same AMQP connection rather than opening a parallel one.
        var client = provider.GetRequiredService<ServiceBusClient>();
        Assert.Same(client, provider.GetRequiredService<ServiceBusClient>());
    }

    [Fact]
    public void UseAzureServiceBusTransport_ConnectionStringOverload_PassesThrough()
    {
        var services = new ServiceCollection();
        services.AddTalaria().UseAzureServiceBusTransport(
            "Endpoint=sb://example/;SharedAccessKeyName=RootManage;SharedAccessKey=KEY");

        var provider = services.BuildServiceProvider();

        var options = provider.GetRequiredService<AzureServiceBusTransportOptions>();
        Assert.Equal("Endpoint=sb://example/;SharedAccessKeyName=RootManage;SharedAccessKey=KEY", options.ConnectionString);
    }

    [Fact]
    public void UseAzureServiceBusTransport_ConfigureOverload_NullConfigure_Throws()
    {
        var services = new ServiceCollection();
        Assert.Throws<ArgumentNullException>(() =>
            services.AddTalaria().UseAzureServiceBusTransport((Action<AzureServiceBusTransportOptions>)null!));
    }

    [Fact]
    public void UseAzureServiceBusTransport_ConnectionStringOverload_NullConnectionString_SurfacesAtBuildServiceProvider()
    {
        // The connection-string overload does not eagerly validate null:
        // it forwards the value to opts.ConnectionString and the transport
        // constructor surfaces the misconfiguration when the container
        // resolves ITransport.
        var services = new ServiceCollection();
        services.AddTalaria().UseAzureServiceBusTransport((string)null!);
        var provider = services.BuildServiceProvider();

        // Resolving ITransport fires the transport factory which calls
        // the AzureServiceBusTransport constructor; that constructor
        // throws when neither ConnectionString nor FullyQualifiedNamespace
        // is supplied.
        Assert.Throws<ArgumentException>(() => provider.GetRequiredService<ITransport>());
    }

    [Fact]
    public void UseAzureServiceBusTransport_InstanceOverload_NullInstance_Throws()
    {
        var services = new ServiceCollection();
        Assert.Throws<ArgumentNullException>(() =>
            services.AddTalaria().UseAzureServiceBusTransport((AzureServiceBusTransport)null!));
    }

    [Fact]
    public void UseAzureServiceBusTransport_InstanceOverload_PreservesIdentity()
    {
        var services = new ServiceCollection();
        var transport = new AzureServiceBusTransport(new AzureServiceBusTransportOptions
        {
            ConnectionString = "Endpoint=sb://example/;SharedAccessKeyName=RootManage;SharedAccessKey=KEY",
        });

        services.AddTalaria().UseAzureServiceBusTransport(transport);

        var provider = services.BuildServiceProvider();
        var resolved = provider.GetRequiredService<ITransport>();
        Assert.Same(transport, resolved);
        // The instance overload also wires the underlying ServiceBusClient
        // so any sibling extension still resolves the same AMQP connection
        // (the transport's own client, not a fresh one).
        Assert.Same(transport.Client, provider.GetRequiredService<ServiceBusClient>());
    }
}
