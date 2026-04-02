using Microsoft.Extensions.DependencyInjection;
using Talaria.Core.Abstractions;
using Talaria.Core.Registration;

namespace Talaria.Core.Tests;

public class TalariaBuilderTests
{
    private class DummyTransport : ITransport
    {
        public string Name => "Dummy";
        public Task<IConsumer<T>> CreateConsumerAsync<T>(string topic, ConsumerOptions options, CancellationToken ct = default) => throw new NotImplementedException();
        public Task<IProducer<T>> CreateProducerAsync<T>(string topic, ProducerOptions options, CancellationToken ct = default) => throw new NotImplementedException();
        public Task<ITransactionalSession> BeginTransactionAsync(CancellationToken ct = default) => throw new NotImplementedException();
    }

    [Fact]
    public void UseTransport_Generic_RegistersTransport()
    {
        var services = new ServiceCollection();
        var builder = services.AddTalaria();
        
        builder.UseTransport<DummyTransport>();
        
        var provider = services.BuildServiceProvider();
        var transport = provider.GetRequiredService<ITransport>();
        Assert.IsType<DummyTransport>(transport);
    }

    [Fact]
    public void UseTransport_Instance_RegistersTransport()
    {
        var services = new ServiceCollection();
        var builder = services.AddTalaria();
        var dummy = new DummyTransport();
        
        builder.UseTransport(dummy);
        
        var provider = services.BuildServiceProvider();
        var transport = provider.GetRequiredService<ITransport>();
        Assert.Same(dummy, transport);
    }
}
