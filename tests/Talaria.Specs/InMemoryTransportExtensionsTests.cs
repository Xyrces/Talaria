using Microsoft.Extensions.DependencyInjection;
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
        
        Assert.Contains(services, s => s.ImplementationInstance is InMemoryTransport);
    }
    
    [Fact]
    public void UseInMemoryTransport_WithOptions_RegistersTransport()
    {
        var services = new ServiceCollection();
        var builder = services.AddTalaria();
        builder.UseInMemoryTransport(opts => { opts.ChannelCapacity = 500; });
        
        var registration = services.FirstOrDefault(s => s.ImplementationInstance is InMemoryTransport);
        Assert.NotNull(registration);
        var transport = (InMemoryTransport)registration.ImplementationInstance!;
        Assert.Equal(500, transport.Options.ChannelCapacity);
    }
}
