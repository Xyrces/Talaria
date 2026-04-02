using Microsoft.Extensions.DependencyInjection;
using Talaria.Core.Abstractions;
using Talaria.Core.Registration;
using Talaria.Transports.InMemory;
using Xunit;

namespace Talaria.InMemory.Tests;

public class InMemoryTransportExtensionsTests
{
    [Fact]
    public void UseInMemoryTransport_ShouldRegisterConfiguredTransport()
    {
        var services = new ServiceCollection();
        
        services.AddTalaria(cfg => cfg.MaxHopCount = 10)
                .UseInMemoryTransport(opts => 
                {
                    opts.SimulatedLatency = TimeSpan.FromMilliseconds(50);
                });

        var provider = services.BuildServiceProvider();

        // Verify ITransport is registered
        var transport = provider.GetRequiredService<ITransport>();
        
        Assert.IsType<InMemoryTransport>(transport);
        Assert.Equal(TimeSpan.FromMilliseconds(50), ((InMemoryTransport)transport).Options.SimulatedLatency);
    }

    [Fact]
    public void UseInMemoryTransport_Default_ShouldRegisterTransport()
    {
        var services = new ServiceCollection();
        services.AddTalaria().UseInMemoryTransport();

        var transport = services.BuildServiceProvider().GetRequiredService<ITransport>();
        Assert.IsType<InMemoryTransport>(transport);
    }

    [Fact]
    public void UseInMemoryTransport_Instance_ShouldRegisterProvidedInstance()
    {
        var services = new ServiceCollection();
        var myTransport = new InMemoryTransport();
        services.AddTalaria().UseInMemoryTransport(myTransport);

        var transport = services.BuildServiceProvider().GetRequiredService<ITransport>();
        Assert.Same(myTransport, transport);
    }
}
