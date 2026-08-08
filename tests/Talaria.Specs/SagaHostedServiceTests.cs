using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
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
        config.Complete();

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

        // The message has no correlation ID — it must be nacked into the topic DLQ.
        var dlq = await ReadUntilAsync<NoCorrelationMessage>(transport, "test-topic.dlq", 1);

        Assert.Single(dlq);
        Assert.Equal("missing_correlation_id", dlq[0].Headers.DlqReason);

        await hostedService.StopAsync(cts.Token);
    }

    [Fact]
    public async Task Routes_To_DLQ_When_Max_Deferrals_Exceeded()
    {
        var transport = new InMemoryTransport();
        var registry = new SagaRegistry();
        
        var config = new SagaConfigurator<TestState>(registry);
        config.On<NoCorrelationMessage>("defer-topic", async (state, msg, ctx) => ctx.Transition(state), correlateBy: m => "static-id");
        config.Complete();

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
        var fakeHeaders = new MessageHeaders { [MessageHeaders.DeferralAttemptKey] = "1" };
        await producer.ProduceAsync(msg, fakeHeaders);

        // Attempt 1 already used (header) → next deferral exceeds MaxDeferralAttempts=1 → DLQ.
        var dlq = await ReadUntilAsync<NoCorrelationMessage>(transport, "defer-topic.dlq", 1);

        Assert.Single(dlq);
        Assert.Equal("max_deferrals_exceeded", dlq[0].Headers.DlqReason);

        await hostedService.StopAsync(cts.Token);
    }

    private static async Task<List<MessageEnvelope<T>>> ReadUntilAsync<T>(
        InMemoryTransport transport, string topic, int expectedCount)
    {
        var deadline = DateTime.UtcNow.AddSeconds(10);
        List<MessageEnvelope<T>> messages;
        do
        {
            messages = await transport.ReadAllFromTopicAsync<T>(topic);
            if (messages.Count >= expectedCount)
            {
                break;
            }

            await Task.Delay(50);
        }
        while (DateTime.UtcNow < deadline);

        return messages;
    }
}
