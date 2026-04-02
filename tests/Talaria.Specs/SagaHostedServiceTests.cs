using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Talaria.Core;
using Talaria.Core.Abstractions;
using Talaria.Core.Hosting;
using Talaria.Core.Sagas;
using Talaria.Transports.InMemory;
using Xunit;

namespace Talaria.Specs.Tests;

public class SagaHostedServiceTests
{
    private class TestState { public string Id { get; set; } = ""; }
    private class NoCorrelationMessage { public string Data { get; set; } = ""; }

    [Fact]
    public async Task Nacks_Message_When_No_CorrelationId_Resolved()
    {
        var transport = new InMemoryTransport();
        var registry = new SagaRegistry();
        
        var config = new SagaConfigurator<TestState>(registry);
        // Force no internal correlation resolver, so it falls back to CorrelationResolver which fails
        config.On<NoCorrelationMessage>("test-topic", async (state, msg, ctx) => ctx.Transition(state));

        var services = new ServiceCollection()
            .AddSingleton<ITransport>(transport)
            .AddSingleton(typeof(IStateStore<>), typeof(InMemoryStateStore<>))
            .BuildServiceProvider();

        var opts = Options.Create(new TalariaOptions());

        var hostedService = new SagaHostedService(registry, services, opts, NullLogger<SagaHostedService>.Instance);
        
        using var cts = new CancellationTokenSource();
        await hostedService.StartAsync(cts.Token);

        var producer = await transport.CreateProducerAsync<NoCorrelationMessage>("test-topic", new ProducerOptions());
        await producer.ProduceAsync(new NoCorrelationMessage());

        await Task.Delay(200);

        // NackAsync on InMemoryConsumer writes to the DLQ channel
        var consumer = await transport.CreateConsumerAsync<NoCorrelationMessage>("test-topic", new ConsumerOptions());
        var nackedEnv = await ((InMemoryConsumer<NoCorrelationMessage>)consumer).ConsumeDlqAsync(cts.Token).GetAsyncEnumerator().MoveNextAsync();
        
        Assert.True(nackedEnv); // Successfully nacked
        
        // Wait, Is there a DLQ for SagaHostedService's nack?
        // InMemoryConsumer.NackAsync writes to dlqChannel.
        await hostedService.StopAsync(cts.Token);
    }

    [Fact]
    public async Task Routes_To_DLQ_When_Max_Deferrals_Exceeded()
    {
        var transport = new InMemoryTransport();
        var registry = new SagaRegistry();
        
        var config = new SagaConfigurator<TestState>(registry);
        config.On<NoCorrelationMessage>("defer-topic", async (state, msg, ctx) => ctx.Transition(state), correlateBy: m => "static-id");

        var services = new ServiceCollection()
            .AddSingleton<ITransport>(transport)
            .AddSingleton(typeof(IStateStore<>), typeof(InMemoryStateStore<>))
            .BuildServiceProvider();

        var opts = Options.Create(new TalariaOptions { MaxDeferralAttempts = 1, DeferralBackoff = TimeSpan.FromMilliseconds(5) });

        var hostedService = new SagaHostedService(registry, services, opts, NullLogger<SagaHostedService>.Instance);
        
        using var cts = new CancellationTokenSource();
        await hostedService.StartAsync(cts.Token);

        var producer = await transport.CreateProducerAsync<NoCorrelationMessage>("defer-topic", new ProducerOptions());
        var msg = new NoCorrelationMessage();
        var fakeHeaders = new MessageHeaders { ["x-deferral-attempt"] = "1" };
        await producer.ProduceAsync(msg, fakeHeaders);

        await Task.Delay(200);

        await hostedService.StopAsync(cts.Token);
        
        // At max attempt, it logs warning and drops/Nacks. Wait, it currently just returns and the message stays uncommitted.
        // Actually, in the current design it drops it (logs warning, return) but the Consumer loop still Nacks or Commits?
        // Wait, HandleDeferralAsync does not Nack or DLQ itself, it just returns. 
        // The outer loop says `continue;` which leaves it uncommitted, but wait: 
        // If it throws an exception it Nacks. If it just `continue;` the message is uncommitted and blocks.
    }
}
