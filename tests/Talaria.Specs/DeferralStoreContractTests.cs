using System;
using System.Collections.Generic;
using System.Linq;
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

public class DeferralStoreContractTests
{
    private class TestState { public string Id { get; set; } = ""; }
    private class StarterMessage { public string Id { get; set; } = ""; }
    private class StepMessage { public string Id { get; set; } = ""; }

    [Fact]
    public async Task Out_Of_Order_Step_Is_Deferred_In_Store_And_Republished_With_Minted_MessageId()
    {
        var transport = new InMemoryTransport();
        var deferralStore = new InMemoryDeferralStore();
        var stepHandlerCalls = 0;

        var registry = new SagaRegistry();
        var config = new SagaConfigurator<TestState>(registry);
        config.StartedBy<StarterMessage>("starter-topic",
            async (msg, ctx) => ctx.Transition(new TestState { Id = msg.Id }),
            correlateBy: m => m.Id);
        config.On<StepMessage>("step-topic",
            async (state, msg, ctx) =>
            {
                Interlocked.Increment(ref stepHandlerCalls);
                return ctx.Transition(state);
            },
            correlateBy: m => m.Id);
        config.Complete();

        var services = new ServiceCollection()
            .AddSingleton<ITransport>(transport)
            .AddSingleton(typeof(IStateStore<>), typeof(InMemoryStateStore<>))
            .AddSingleton<IDeferralStore>(deferralStore)
            .BuildServiceProvider();

        // 200ms backoff: wide enough to reliably observe the pending entry before the
        // sweeper republishes it, and ensures the starter creates state before the first sweep.
        var opts = Options.Create(new TalariaOptions { DeferralBackoff = TimeSpan.FromMilliseconds(200) });

        var hostedService = new SagaHostedService(registry, services, opts, NullLogger<SagaHostedService>.Instance);

        using var cts = new CancellationTokenSource();
        await hostedService.StartAsync(cts.Token);

        // 1. Produce the non-starter step message BEFORE any state exists → deferred.
        var stepProducer = await transport.CreateProducerAsync<StepMessage>("step-topic", new ProducerOptions());
        await stepProducer.ProduceAsync(
            new StepMessage { Id = "corr-1" },
            new MessageHeaders { MessageId = "step-msg-1" });

        // The message must be durably scheduled in the deferral store.
        await WaitUntilAsync(() => deferralStore.Count == 1);
        Assert.Equal(1, deferralStore.Count);
        Assert.Equal(0, Volatile.Read(ref stepHandlerCalls));

        // 2. Produce the starter for the same correlation → state now exists.
        var starterProducer = await transport.CreateProducerAsync<StarterMessage>("starter-topic", new ProducerOptions());
        await starterProducer.ProduceAsync(new StarterMessage { Id = "corr-1" });

        // 3. The sweeper republishes the deferred copy once due; the step handler fires.
        await WaitUntilAsync(() => Volatile.Read(ref stepHandlerCalls) == 1);
        await WaitUntilAsync(() => deferralStore.Count == 0);

        // 4. The republished copy carries a freshly minted MessageId per deferral attempt
        //    ("{original}:defer:{attempt}") so it is not suppressed as a duplicate.
        var stepTopicMessages = await ReadUntilAsync<StepMessage>(transport, "step-topic", 2);
        Assert.Contains(stepTopicMessages, m =>
            m.Headers.MessageId is not null && m.Headers.MessageId.Contains(":defer:1"));

        // 5. Nothing was dead-lettered along the way.
        Assert.Empty(await transport.ReadAllFromTopicAsync<StarterMessage>("starter-topic.dlq"));
        Assert.Empty(await transport.ReadAllFromTopicAsync<StepMessage>("step-topic.dlq"));

        await hostedService.StopAsync(cts.Token);
    }

    [Fact]
    public async Task Acquire_Leases_Without_Removing_And_Complete_Is_Fenced()
    {
        var store = new InMemoryDeferralStore();
        var lease = TimeSpan.FromSeconds(30);

        var due = new DeferredMessage(
            Guid.NewGuid(), "topic-a", typeof(StepMessage).AssemblyQualifiedName!,
            "{}", new MessageHeaders(), "corr-1", 1, DateTimeOffset.UtcNow.AddMilliseconds(-1));
        var notDue = new DeferredMessage(
            Guid.NewGuid(), "topic-a", typeof(StepMessage).AssemblyQualifiedName!,
            "{}", new MessageHeaders(), "corr-2", 1, DateTimeOffset.UtcNow.AddHours(1));

        await store.EnqueueAsync(due);
        await store.EnqueueAsync(notDue);
        Assert.Equal(2, store.Count);

        var now = DateTimeOffset.UtcNow;

        // Only the due entry is leased, and the lease hides it from other acquirers
        // without removing it from the store.
        var first = Assert.Single(await store.AcquireDueAsync(now, lease, 64));
        Assert.Equal(due.Id, first.Message.Id);
        Assert.Empty(await store.AcquireDueAsync(now.AddSeconds(5), lease, 64));
        Assert.Equal(2, store.Count);

        // After lease expiry the entry is acquirable again, with a bumped fencing token,
        // and the stale holder can no longer complete it.
        var reacquired = Assert.Single(await store.AcquireDueAsync(now.Add(lease).AddSeconds(1), lease, 64));
        Assert.True(reacquired.Lease.Token > first.Lease.Token);
        Assert.False(await store.CompleteAsync(first.Lease));

        // The current owner completes; only the not-due entry remains.
        Assert.True(await store.CompleteAsync(reacquired.Lease));
        Assert.Equal(1, store.Count);
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

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow.AddSeconds(10);
        while (DateTime.UtcNow < deadline)
        {
            if (condition())
            {
                return;
            }

            await Task.Delay(25);
        }

        Assert.True(condition(), "Condition was not met within the timeout.");
    }
}
