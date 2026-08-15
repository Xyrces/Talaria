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
using Talaria.Core.Registration;
using Talaria.Core.Sagas;
using Talaria.Transports.InMemory;
using Xunit;

namespace Talaria.Specs.Tests;

/// <summary>
/// Engine-level coverage of the transactional outbox path: when an IOutboxStore is
/// registered, saga dispatches are staged atomically with the state transition and
/// published asynchronously by the relay — instead of being produced directly inside
/// a transport transaction.
/// </summary>
public class SagaOutboxTests
{
    private class OrderState { public string Id { get; set; } = ""; public bool Billed { get; set; } }
    private class PlaceOrder { public string Id { get; set; } = ""; }
    private class BillOrder { public string Id { get; set; } = ""; }
    private class OrderBilled { public string Id { get; set; } = ""; }

    private static (SagaRegistry Registry, ServiceProvider Services, InMemoryTransport Transport, InMemoryOutboxStore Outbox)
        BuildEngine(Action<SagaConfigurator<OrderState>> configure)
    {
        var transport = new InMemoryTransport();
        var outbox = new InMemoryOutboxStore();
        var registry = new SagaRegistry();

        var config = new SagaConfigurator<OrderState>(registry);
        configure(config);
        config.Complete();

        var services = new ServiceCollection()
            .AddSingleton<ITransport>(transport)
            .AddSingleton(outbox)
            .AddSingleton<IOutboxStore>(outbox)
            .AddSingleton(typeof(IStateStore<>), typeof(InMemoryStateStore<>))
            .BuildServiceProvider();

        return (registry, services, transport, outbox);
    }

    private static TalariaListener StartHost(SagaRegistry registry, IServiceProvider services, ITransport transport)
    {
        return new TalariaListener(
            transport,
            new TopicRegistry(),
            registry,
            new TalariaOptions
            {
                ApplicationName = "test-app",
                OutboxRelayInterval = TimeSpan.FromMilliseconds(50)
            },
            NullLogger<TalariaListener>.Instance,
            services);
    }

    [Fact]
    public async Task Dispatch_Is_Staged_In_Outbox_And_Published_By_Relay()
    {
        var (registry, services, transport, outbox) = BuildEngine(config =>
        {
            config.StartedBy<PlaceOrder>("ob-start",
                (msg, ctx) => Task.FromResult(ctx.Transition(new OrderState { Id = msg.Id })),
                m => m.Id);
            config.On<BillOrder>("ob-bill",
                (state, msg, ctx) =>
                {
                    state.Billed = true;
                    ctx.Dispatch(new OrderBilled { Id = msg.Id });
                    return Task.FromResult(ctx.Transition(state));
                },
                m => m.Id);
            config.DispatchTo<OrderBilled>("ob-billed");
        });

        var listener = StartHost(registry, services, transport);
        using var cts = new CancellationTokenSource();
        await listener.StartAsync(cts.Token);

        await (await transport.CreateProducerAsync<PlaceOrder>("ob-start", new ProducerOptions()))
            .ProduceAsync(new PlaceOrder { Id = "c1" });

        // Starter and step live on different topics with independent consumers: wait
        // until the starter has created state before producing the step, or the step
        // arrives out of order (and, with no deferral store registered, dead-letters).
        var stateStore = services.GetRequiredService<IStateStore<OrderState>>();
        var started = await PollUntilAsync(async () => await stateStore.GetAsync("c1") is not null, TimeSpan.FromSeconds(5));
        Assert.True(started, "The starter never created saga state.");

        await (await transport.CreateProducerAsync<BillOrder>("ob-bill", new ProducerOptions()))
            .ProduceAsync(new BillOrder { Id = "c1" });

        // The relay publishes the staged dispatch...
        var dispatched = await ReadUntilAsync<OrderBilled>(transport, "ob-billed", 1);
        var envelope = Assert.Single(dispatched);
        Assert.Equal("c1", envelope.Payload!.Id);

        // ...with a minted MessageId (proof it came through the outbox, not direct dispatch).
        Assert.False(string.IsNullOrEmpty(envelope.Headers.MessageId));

        // ...and the outbox drains once publication is confirmed.
        var drained = await PollUntilAsync(() => Task.FromResult(outbox.Count == 0), TimeSpan.FromSeconds(5));
        Assert.True(drained, "The outbox was not drained after the dispatch was published.");

        // State reflects the transition that staged the dispatch.
        var store = services.GetRequiredService<IStateStore<OrderState>>();
        Assert.True((await store.GetAsync("c1"))!.Billed);

        await listener.StopAsync(cts.Token);
    }

    [Fact]
    public async Task Dispatch_From_Keyed_Inbound_Message_Preserves_PartitionKey()
    {
        var (registry, services, transport, outbox) = BuildEngine(config =>
        {
            config.StartedBy<PlaceOrder>("pk-start",
                (msg, ctx) => Task.FromResult(ctx.Transition(new OrderState { Id = msg.Id })),
                m => m.Id);
            config.On<BillOrder>("pk-bill",
                (state, msg, ctx) =>
                {
                    state.Billed = true;
                    ctx.Dispatch(new OrderBilled { Id = msg.Id });
                    return Task.FromResult(ctx.Transition(state));
                },
                m => m.Id);
            config.DispatchTo<OrderBilled>("pk-billed");
        });

        var listener = StartHost(registry, services, transport);
        using var cts = new CancellationTokenSource();
        await listener.StartAsync(cts.Token);

        await (await transport.CreateProducerAsync<PlaceOrder>("pk-start", new ProducerOptions()))
            .ProduceAsync(new PlaceOrder { Id = "c1" });

        var stateStore = services.GetRequiredService<IStateStore<OrderState>>();
        var started = await PollUntilAsync(async () => await stateStore.GetAsync("c1") is not null, TimeSpan.FromSeconds(5));
        Assert.True(started, "The starter never created saga state.");

        await (await transport.CreateProducerAsync<BillOrder>("pk-bill", new ProducerOptions()))
            .ProduceAsync(new BillOrder { Id = "c1" }, partitionKey: "order-partition-7");

        var dispatched = await ReadUntilAsync<OrderBilled>(transport, "pk-billed", 1);
        var envelope = Assert.Single(dispatched);
        Assert.Equal("c1", envelope.Payload!.Id);
        Assert.Equal("order-partition-7", envelope.PartitionKey);

        var drained = await PollUntilAsync(() => Task.FromResult(outbox.Count == 0), TimeSpan.FromSeconds(5));
        Assert.True(drained, "The outbox was not drained after the dispatch was published.");

        await listener.StopAsync(cts.Token);
    }

    [Fact]
    public async Task Completion_Purges_State_And_Stages_Final_Dispatch_Atomically()
    {
        var (registry, services, transport, outbox) = BuildEngine(config =>
        {
            config.StartedBy<PlaceOrder>("oc-start",
                (msg, ctx) => Task.FromResult(ctx.Transition(new OrderState { Id = msg.Id })),
                m => m.Id);
            config.On<BillOrder>("oc-bill",
                (state, msg, ctx) =>
                {
                    ctx.Dispatch(new OrderBilled { Id = msg.Id });
                    return Task.FromResult(ctx.Complete());
                },
                m => m.Id);
            config.DispatchTo<OrderBilled>("oc-billed");
        });

        var listener = StartHost(registry, services, transport);
        using var cts = new CancellationTokenSource();
        await listener.StartAsync(cts.Token);

        await (await transport.CreateProducerAsync<PlaceOrder>("oc-start", new ProducerOptions()))
            .ProduceAsync(new PlaceOrder { Id = "c1" });

        // Same cross-topic ordering hazard as the dispatch test: wait for state first.
        var stateStore = services.GetRequiredService<IStateStore<OrderState>>();
        var started = await PollUntilAsync(async () => await stateStore.GetAsync("c1") is not null, TimeSpan.FromSeconds(5));
        Assert.True(started, "The starter never created saga state.");

        await (await transport.CreateProducerAsync<BillOrder>("oc-bill", new ProducerOptions()))
            .ProduceAsync(new BillOrder { Id = "c1" });

        var dispatched = await ReadUntilAsync<OrderBilled>(transport, "oc-billed", 1);
        Assert.Single(dispatched);

        // The purge half of the atomic transition: no state remains.
        var store = services.GetRequiredService<IStateStore<OrderState>>();
        Assert.Null(await store.GetAsync("c1"));

        var drained = await PollUntilAsync(() => Task.FromResult(outbox.Count == 0), TimeSpan.FromSeconds(5));
        Assert.True(drained);

        await listener.StopAsync(cts.Token);
    }

    [Fact]
    public async Task Starter_Replay_Does_Not_Stage_A_Second_Outbox_Entry()
    {
        var (registry, services, transport, outbox) = BuildEngine(config =>
        {
            config.StartedBy<PlaceOrder>("or-start",
                (msg, ctx) =>
                {
                    ctx.Dispatch(new OrderBilled { Id = msg.Id });
                    return Task.FromResult(ctx.Transition(new OrderState { Id = msg.Id }));
                },
                m => m.Id);
            config.DispatchTo<OrderBilled>("or-billed");
        });

        var listener = StartHost(registry, services, transport);
        using var cts = new CancellationTokenSource();
        await listener.StartAsync(cts.Token);

        var producer = await transport.CreateProducerAsync<PlaceOrder>("or-start", new ProducerOptions());
        await producer.ProduceAsync(new PlaceOrder { Id = "c1" }, new MessageHeaders { MessageId = "starter-1" });

        var dispatched = await ReadUntilAsync<OrderBilled>(transport, "or-billed", 1);
        Assert.Single(dispatched);

        // Replay the same starter (same MessageId): the idempotency gate is not wired
        // here, but the starter replay guard (state exists) must skip re-execution.
        await producer.ProduceAsync(new PlaceOrder { Id = "c1" }, new MessageHeaders { MessageId = "starter-1" });

        // The outbox stays empty and no second dispatch is published.
        var stable = await PollStableAsync(() => Task.FromResult(outbox.Count == 0), TimeSpan.FromMilliseconds(500));
        Assert.True(stable, "A replayed starter staged a second outbox entry.");
        Assert.Empty(await transport.ReadAllFromTopicAsync<OrderBilled>("or-billed"));

        await listener.StopAsync(cts.Token);
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

    private static async Task<bool> PollUntilAsync(Func<Task<bool>> condition, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (await condition())
            {
                return true;
            }

            await Task.Delay(50);
        }

        return await condition();
    }

    /// <summary>Negative-assertion helper: the condition must hold continuously for the whole window.</summary>
    private static async Task<bool> PollStableAsync(Func<Task<bool>> condition, TimeSpan window)
    {
        var deadline = DateTime.UtcNow + window;
        while (DateTime.UtcNow < deadline)
        {
            if (!await condition())
            {
                return false;
            }

            await Task.Delay(50);
        }

        return await condition();
    }
}
