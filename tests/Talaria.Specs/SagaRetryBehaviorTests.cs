using System;
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

public class SagaRetryBehaviorTests
{
    private class SagaRetryState { public string Id { get; set; } = ""; public int Transitions { get; set; } }
    private class SagaRetryStart { public string Id { get; set; } = ""; }
    private class SagaRetryStep { public string Id { get; set; } = ""; }

    [Fact]
    public async Task SagaStep_FailsOnceThenSucceeds_OnRetry_StateTransitionsExactlyOnce()
    {
        var transport = new InMemoryTransport();
        var deferralStore = new InMemoryDeferralStore();
        var registry = new SagaRegistry();

        var stepRuns = 0;
        var config = new SagaConfigurator<SagaRetryState>(registry);
        config.StartedBy<SagaRetryStart>("sr-start",
            (msg, ctx) => Task.FromResult(ctx.Transition(new SagaRetryState { Id = msg.Id })),
            m => m.Id);
        config.On<SagaRetryStep>("sr-step",
            (state, msg, ctx) =>
            {
                Interlocked.Increment(ref stepRuns);
                if (Volatile.Read(ref stepRuns) < 2)
                {
                    throw new InvalidOperationException("boom");
                }

                state.Transitions++;
                return Task.FromResult(ctx.Transition(state));
            },
            m => m.Id);
        config.Complete();

        var services = new ServiceCollection()
            .AddSingleton<ITransport>(transport)
            .AddSingleton(typeof(IStateStore<>), typeof(InMemoryStateStore<>))
            .AddSingleton<IDeferralStore>(deferralStore)
            .BuildServiceProvider();

        var hostedService = new SagaHostedService(registry, services, Options.Create(new TalariaOptions
        {
            ApplicationName = "test-app",
            DeferralBackoff = TimeSpan.FromMilliseconds(50),
            MaxDeferralAttempts = 5,
            MinRetryDelay = TimeSpan.FromMilliseconds(50),
            DefaultRetryPolicy = new RetryPolicy
            {
                MaxRetryAttempts = 2,
                RetryInterval = TimeSpan.FromMilliseconds(50),
                BackoffType = RetryBackoffType.Fixed,
            },
        }), NullLogger<SagaHostedService>.Instance);

        using var cts = new CancellationTokenSource();
        await hostedService.StartAsync(cts.Token);

        var startProducer = await transport.CreateProducerAsync<SagaRetryStart>("sr-start", new ProducerOptions());
        await startProducer.ProduceAsync(new SagaRetryStart { Id = "c1" });

        var store = services.GetRequiredService<IStateStore<SagaRetryState>>();
        var started = await PollUntilAsync(async () => await store.GetAsync("c1") != null, TimeSpan.FromSeconds(5));
        Assert.True(started, "Saga state was never created by the starter.");

        var stepProducer = await transport.CreateProducerAsync<SagaRetryStep>("sr-step", new ProducerOptions());
        await stepProducer.ProduceAsync(new SagaRetryStep { Id = "c1" }, new MessageHeaders { MessageId = "sr-step-1" });

        var succeeded = await PollUntilAsync(async () =>
        {
            var state = await store.GetAsync("c1");
            return Volatile.Read(ref stepRuns) >= 2 && state is { Transitions: 1 };
        }, TimeSpan.FromSeconds(10));

        Assert.True(succeeded, $"Step did not succeed on retry (runs={Volatile.Read(ref stepRuns)}).");

        var stable = await PollStableAsync(async () =>
        {
            var state = await store.GetAsync("c1");
            return Volatile.Read(ref stepRuns) == 2 && state is { Transitions: 1 };
        }, TimeSpan.FromSeconds(2));

        Assert.True(stable, "Step handler re-ran after the successful retry.");
        Assert.Equal(2, Volatile.Read(ref stepRuns));
        Assert.Equal(1, (await store.GetAsync("c1"))!.Transitions);
        Assert.Empty(await transport.ReadAllFromTopicAsync<SagaRetryStep>("sr-step.dlq"));

        await hostedService.StopAsync(cts.Token);
    }

    [Fact]
    public async Task SagaStep_Exhausted_RoutesToDLQ_AsRetriesExhausted()
    {
        var transport = new InMemoryTransport();
        var deferralStore = new InMemoryDeferralStore();
        var registry = new SagaRegistry();

        var config = new SagaConfigurator<SagaRetryState>(registry);
        config.StartedBy<SagaRetryStart>("sr-start",
            (msg, ctx) => Task.FromResult(ctx.Transition(new SagaRetryState { Id = msg.Id })),
            m => m.Id);
        config.On<SagaRetryStep>("sr-step",
            (state, msg, ctx) => throw new InvalidOperationException("boom"),
            m => m.Id);
        config.Complete();

        var services = new ServiceCollection()
            .AddSingleton<ITransport>(transport)
            .AddSingleton(typeof(IStateStore<>), typeof(InMemoryStateStore<>))
            .AddSingleton<IDeferralStore>(deferralStore)
            .BuildServiceProvider();

        var hostedService = new SagaHostedService(registry, services, Options.Create(new TalariaOptions
        {
            ApplicationName = "test-app",
            DeferralBackoff = TimeSpan.FromMilliseconds(50),
            MaxDeferralAttempts = 5,
            MinRetryDelay = TimeSpan.FromMilliseconds(50),
            DefaultRetryPolicy = new RetryPolicy
            {
                MaxRetryAttempts = 1,
                RetryInterval = TimeSpan.FromMilliseconds(50),
                BackoffType = RetryBackoffType.Fixed,
            },
        }), NullLogger<SagaHostedService>.Instance);

        using var cts = new CancellationTokenSource();
        await hostedService.StartAsync(cts.Token);

        var startProducer = await transport.CreateProducerAsync<SagaRetryStart>("sr-start", new ProducerOptions());
        await startProducer.ProduceAsync(new SagaRetryStart { Id = "c2" });

        var store = services.GetRequiredService<IStateStore<SagaRetryState>>();
        var started = await PollUntilAsync(async () => await store.GetAsync("c2") != null, TimeSpan.FromSeconds(5));
        Assert.True(started, "Saga state was never created by the starter.");

        var stepProducer = await transport.CreateProducerAsync<SagaRetryStep>("sr-step", new ProducerOptions());
        await stepProducer.ProduceAsync(new SagaRetryStep { Id = "c2" }, new MessageHeaders { MessageId = "sr-step-2" });

        var dlq = await ReadUntilAsync<SagaRetryStep>(transport, "sr-step.dlq", 1);

        Assert.Single(dlq);
        Assert.Equal("retries_exhausted", dlq[0].Headers.DlqReason);

        await hostedService.StopAsync(cts.Token);
    }

    [Fact]
    public async Task SagaStep_RetryEnabledWithoutDeferralStore_RoutesToDLQ_AsRetryUnavailable()
    {
        var transport = new InMemoryTransport();
        var registry = new SagaRegistry();

        var config = new SagaConfigurator<SagaRetryState>(registry);
        config.StartedBy<SagaRetryStart>("sr-start",
            (msg, ctx) => Task.FromResult(ctx.Transition(new SagaRetryState { Id = msg.Id })),
            m => m.Id);
        config.On<SagaRetryStep>("sr-step",
            (state, msg, ctx) => throw new InvalidOperationException("boom"),
            m => m.Id);
        config.Complete();

        var services = new ServiceCollection()
            .AddSingleton<ITransport>(transport)
            .AddSingleton(typeof(IStateStore<>), typeof(InMemoryStateStore<>))
            .BuildServiceProvider();

        var hostedService = new SagaHostedService(registry, services, Options.Create(new TalariaOptions
        {
            ApplicationName = "test-app",
            DeferralBackoff = TimeSpan.FromMilliseconds(50),
            MaxDeferralAttempts = 5,
            MinRetryDelay = TimeSpan.FromMilliseconds(50),
            DefaultRetryPolicy = new RetryPolicy
            {
                MaxRetryAttempts = 2,
                RetryInterval = TimeSpan.FromMilliseconds(50),
                BackoffType = RetryBackoffType.Fixed,
            },
        }), NullLogger<SagaHostedService>.Instance);

        using var cts = new CancellationTokenSource();
        await hostedService.StartAsync(cts.Token);

        var startProducer = await transport.CreateProducerAsync<SagaRetryStart>("sr-start", new ProducerOptions());
        await startProducer.ProduceAsync(new SagaRetryStart { Id = "c3" });

        var store = services.GetRequiredService<IStateStore<SagaRetryState>>();
        var started = await PollUntilAsync(async () => await store.GetAsync("c3") != null, TimeSpan.FromSeconds(5));
        Assert.True(started, "Saga state was never created by the starter.");

        var stepProducer = await transport.CreateProducerAsync<SagaRetryStep>("sr-step", new ProducerOptions());
        await stepProducer.ProduceAsync(new SagaRetryStep { Id = "c3" }, new MessageHeaders { MessageId = "sr-step-3" });

        var dlq = await ReadUntilAsync<SagaRetryStep>(transport, "sr-step.dlq", 1);

        Assert.Single(dlq);
        Assert.Equal("retry_unavailable", dlq[0].Headers.DlqReason);

        await hostedService.StopAsync(cts.Token);
    }

    [Fact]
    public async Task StarterRetry_WithExistingState_RunsAndTransitionsState()
    {
        var transport = new InMemoryTransport();
        var deferralStore = new InMemoryDeferralStore();
        var registry = new SagaRegistry();

        var starterRuns = 0;
        var config = new SagaConfigurator<SagaRetryState>(registry);
        config.StartedBy<SagaRetryStart>("sr-start",
            (msg, ctx) =>
            {
                Interlocked.Increment(ref starterRuns);
                if (Volatile.Read(ref starterRuns) < 2)
                {
                    throw new InvalidOperationException("boom");
                }

                return Task.FromResult(ctx.Transition(new SagaRetryState { Id = msg.Id }));
            },
            m => m.Id);
        config.Complete();

        var services = new ServiceCollection()
            .AddSingleton<ITransport>(transport)
            .AddSingleton(typeof(IStateStore<>), typeof(InMemoryStateStore<>))
            .AddSingleton<IDeferralStore>(deferralStore)
            .BuildServiceProvider();

        var hostedService = new SagaHostedService(registry, services, Options.Create(new TalariaOptions
        {
            ApplicationName = "test-app",
            DeferralBackoff = TimeSpan.FromMilliseconds(50),
            MaxDeferralAttempts = 5,
            MinRetryDelay = TimeSpan.FromMilliseconds(50),
            DefaultRetryPolicy = new RetryPolicy
            {
                MaxRetryAttempts = 2,
                RetryInterval = TimeSpan.FromMilliseconds(50),
                BackoffType = RetryBackoffType.Fixed,
            },
        }), NullLogger<SagaHostedService>.Instance);

        using var cts = new CancellationTokenSource();
        await hostedService.StartAsync(cts.Token);

        var producer = await transport.CreateProducerAsync<SagaRetryStart>("sr-start", new ProducerOptions());
        await producer.ProduceAsync(new SagaRetryStart { Id = "c4" }, new MessageHeaders { MessageId = "starter-1" });

        var store = services.GetRequiredService<IStateStore<SagaRetryState>>();

        // The starter fails once, a retry copy is scheduled, then it succeeds and creates state.
        var succeeded = await PollUntilAsync(async () =>
        {
            var state = await store.GetAsync("c4");
            return Volatile.Read(ref starterRuns) >= 2 && state is { Id: "c4" };
        }, TimeSpan.FromSeconds(10));

        Assert.True(succeeded, $"Starter retry did not run and transition state (runs={Volatile.Read(ref starterRuns)}).");

        var stable = await PollStableAsync(async () =>
        {
            var state = await store.GetAsync("c4");
            return Volatile.Read(ref starterRuns) == 2 && state is { Id: "c4" };
        }, TimeSpan.FromSeconds(2));

        Assert.True(stable, "Starter handler re-ran after the successful retry.");
        Assert.Equal(2, Volatile.Read(ref starterRuns));
        Assert.Empty(await transport.ReadAllFromTopicAsync<SagaRetryStart>("sr-start.dlq"));

        await hostedService.StopAsync(cts.Token);
    }

    // ---- Helpers (same pattern as SagaEngineBehaviorTests) ----

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
