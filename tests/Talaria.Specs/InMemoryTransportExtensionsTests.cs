using Microsoft.Extensions.DependencyInjection;
using Talaria.Core.Abstractions;
using Talaria.Core.Registration;
using Talaria.Transports.InMemory;

namespace Talaria.Specs;

public class InMemoryTransportExtensionsTests
{
    [Fact]
    public void UseInMemoryTransport_Default_RegistersTransport()
    {
        var services = new ServiceCollection();
        var builder = services.AddTalaria();
        builder.UseInMemoryTransport();

        var provider = services.BuildServiceProvider();
        Assert.IsType<InMemoryTransport>(provider.GetRequiredService<ITransport>());
    }

    [Fact]
    public void UseInMemoryTransport_WithOptions_RegistersTransport()
    {
        var services = new ServiceCollection();
        var builder = services.AddTalaria();
        builder.UseInMemoryTransport(opts => { opts.ChannelCapacity = 500; });

        var provider = services.BuildServiceProvider();
        var transport = Assert.IsType<InMemoryTransport>(provider.GetRequiredService<ITransport>());
        Assert.Equal(500, transport.Options.ChannelCapacity);
    }

    [Fact]
    public void UseInMemoryTransport_Instance_RegistersSharedInstance()
    {
        var services = new ServiceCollection();
        var builder = services.AddTalaria();
        var transport = new InMemoryTransport();
        builder.UseInMemoryTransport(transport);

        var provider = services.BuildServiceProvider();
        Assert.Same(transport, provider.GetRequiredService<ITransport>());
    }
}
