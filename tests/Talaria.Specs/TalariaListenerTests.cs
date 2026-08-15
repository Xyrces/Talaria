using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Talaria.Core;
using Talaria.Core.Abstractions;
using Talaria.Core.Hosting;
using Talaria.Core.Registration;
using Talaria.Core.Sagas;
using Talaria.Transports.InMemory;
using Xunit;

namespace Talaria.Specs.Tests;

public class TalariaListenerTests
{
    private class DummyMessage { }

    [Fact]
    public async Task StopAsync_Disposes_Consumers()
    {
        var transport = new InMemoryTransport();
        var topicReg = new TopicRegistry();
        topicReg.Add(new TopicRegistration {
            TopicName = "test-topic",
            MessageType = typeof(DummyMessage),
            Handler = (msg, headers, _, ct) => Task.CompletedTask
        });

        var opts = Options.Create(new TalariaOptions());
        var services = new ServiceCollection().BuildServiceProvider();

        var listener = new TalariaListener(
            transport,
            topicReg,
            new SagaRegistry(),
            opts.Value,
            NullLogger<TalariaListener>.Instance,
            services);

        using var cts = new CancellationTokenSource();
        await listener.StartAsync(cts.Token);

        // Ensure starting and stopping covers the dispose paths; stopping twice must not throw.
        await listener.StopAsync(cts.Token);
        await listener.StopAsync(cts.Token);
    }
}
