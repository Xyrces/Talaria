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

        // Create the saga state first so the step is invoked (not deferred as out-of-order).
        var startProducer = await transport.CreateProducerAsync<SagaRetryStart>("sr-start", new ProducerOptions());
        await startProducer.ProduceAsync(new SagaRetryStart { Id = "c1" });

        var store = services.GetRequiredService<IStateStore<SagaRetryState>>();
        var started = await PollUntilAsync(async () => await store.GetAsync("c1") != null, TimeSpan.FromSeconds(5));
        Assert.True(started, "Saga state was never created by the starter.");

        var stepProducer = await transport.CreateProducerAsync<SagaRetryStep>("sr-step", new ProducerOptions());
        await stepProducer.ProduceAsync(new SagaRetryStep { Id = "c1" }, new MessageHeaders { MessageId = "sr-step-1" });

        // Wait until the retry has been durably scheduled and the sweeper republishes it.
        var succeeded = await PollUntilAsync(async () =>
        {
            var state = await store.GetAsync("c1");
            return Volatile.Read(ref stepRuns) >= 2 && state is { Transitions: 1 };
        }, TimeSpan.FromSeconds(10));

        Assert.True(succeeded, $"Step did not succeed on retry (runs={Volatile.Read(ref stepRuns)}, deferrals={deferralStore.Count}).");

        // Stability window: the handler must not run again.
        var stable = await PollStableAsync(async () =>
        {
            var state = await store.GetAsync("c1");
            return Volatile.Read(ref stepRuns) == 2 && state is { Transitions: 1 };
        }, TimeSpan.FromSeconds(2));

        Assert.True(stable, "Handler re-ran after the successful retry.");
        Assert.Equal(2, Volatile.Read(ref stepRuns));
        Assert.Equal(1, (await store.GetAsync("c1"))!.Transitions);
        Assert.Empty(await transport.ReadAllFromTopicAsync<SagaRetryStep>("sr-step.dlq"));

        await hostedService.StopAsync(cts.Token);
    }

    // ---- Helpers (same pattern as SagaEngineBehaviorTests) ----

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
