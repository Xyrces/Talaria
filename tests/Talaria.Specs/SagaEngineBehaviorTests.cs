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

public class SagaEngineBehaviorTests
{
    // ---- Test 1: out-of-order deferral ----

    private class OooState { public string Id { get; set; } = ""; public string Log { get; set; } = ""; }
    private class OooStart { public string Id { get; set; } = ""; }
    private class OooStep { public string Id { get; set; } = ""; }

    [Fact]
    public async Task OutOfOrder_StepMessage_IsDeferred_ThenProcessed_AfterStarterArrives()
    {
        var transport = new InMemoryTransport();
        var deferralStore = new InMemoryDeferralStore();
        var registry = new SagaRegistry();

        var stepRuns = 0;
        var config = new SagaConfigurator<OooState>(registry);
        config.StartedBy<OooStart>("s1-start",
            (msg, ctx) => Task.FromResult(ctx.Transition(new OooState { Id = msg.Id, Log = "start" })),
            m => m.Id);
        config.On<OooStep>("s1-step",
            (state, msg, ctx) =>
            {
                state.Log += "+step";
                Interlocked.Increment(ref stepRuns);
                return Task.FromResult(ctx.Transition(state));
            },
            m => m.Id);
        config.Complete();

        var services = new ServiceCollection()
            .AddSingleton<ITransport>(transport)
            .AddSingleton(typeof(IStateStore<>), typeof(InMemoryStateStore<>))
            .AddSingleton<IDeferralStore>(deferralStore)
            .BuildServiceProvider();

        var listener = new TalariaListener(
            transport,
            new TopicRegistry(),
            registry,
            new TalariaOptions
            {
                ApplicationName = "test-app",
                DeferralBackoff = TimeSpan.FromMilliseconds(50),
                MaxDeferralAttempts = 5
            },
            NullLogger<TalariaListener>.Instance,
            services);

        using var cts = new CancellationTokenSource();
        await listener.StartAsync(cts.Token);

        // Produce the STEP first — no state exists yet, so it must be deferred.
        var stepProducer = await transport.CreateProducerAsync<OooStep>("s1-step", new ProducerOptions());
        await stepProducer.ProduceAsync(new OooStep { Id = "c1" });

        // Wait until the step message has actually been deferred (durable proof the
        // out-of-order path ran) before producing the starter.
        var deferred = await PollUntilAsync(() => Task.FromResult(deferralStore.Count >= 1), TimeSpan.FromSeconds(5));
        Assert.True(deferred, "The out-of-order step message was never deferred.");

        var startProducer = await transport.CreateProducerAsync<OooStart>("s1-start", new ProducerOptions());
        await startProducer.ProduceAsync(new OooStart { Id = "c1" });

        // The sweeper republishes the deferred copy; once state exists the step runs.
        var stepRan = await PollUntilAsync(
            () => Task.FromResult(Volatile.Read(ref stepRuns) >= 1), TimeSpan.FromSeconds(10));
        Assert.True(stepRan, "The deferred step handler never ran after the starter arrived.");

        // Final state reflects both messages, in order.
        var store = services.GetRequiredService<IStateStore<OooState>>();
        OooState? state = null;
        await PollUntilAsync(async () =>
        {
            state = await store.GetAsync("c1");
            return state is { Log: "start+step" };
        }, TimeSpan.FromSeconds(5));
        Assert.Equal("start+step", state?.Log);

        // Nothing dead-lettered along the way.
        Assert.Empty(await transport.ReadAllFromTopicAsync<OooStep>("s1-step.dlq"));
        Assert.Empty(await transport.ReadAllFromTopicAsync<OooStart>("s1-start.dlq"));

        await listener.StopAsync(cts.Token);
    }

    // ---- Test 2: two steps on the same topic (fan-out via message-type header) ----

    private class FanState { public string Id { get; set; } = ""; }
    private class FanStart { public string Id { get; set; } = ""; }
    private class FanMsgA { public string Id { get; set; } = ""; }
    private class FanMsgB { public string Id { get; set; } = ""; }

    [Fact]
    public async Task TwoSteps_OnSameTopic_BothHandlersFire_ViaMessageTypeFanOut()
    {
        var transport = new InMemoryTransport();
        var registry = new SagaRegistry();

        var aRuns = 0;
        var bRuns = 0;
        var config = new SagaConfigurator<FanState>(registry);
        config.StartedBy<FanStart>("s2-start",
            (msg, ctx) => Task.FromResult(ctx.Transition(new FanState { Id = msg.Id })),
            m => m.Id);
        config.On<FanMsgA>("s2-shared",
            (state, msg, ctx) =>
            {
                Interlocked.Increment(ref aRuns);
                return Task.FromResult(ctx.Transition(state));
            },
            m => m.Id);
        config.On<FanMsgB>("s2-shared",
            (state, msg, ctx) =>
            {
                Interlocked.Increment(ref bRuns);
                return Task.FromResult(ctx.Transition(state));
            },
            m => m.Id);
        config.Complete();

        var services = new ServiceCollection()
            .AddSingleton<ITransport>(transport)
            .AddSingleton(typeof(IStateStore<>), typeof(InMemoryStateStore<>))
            .BuildServiceProvider();

        var listener = new TalariaListener(
            transport,
            new TopicRegistry(),
            registry,
            new TalariaOptions
            {
                ApplicationName = "test-app",
                DeferralBackoff = TimeSpan.FromMilliseconds(50),
                MaxDeferralAttempts = 5
            },
            NullLogger<TalariaListener>.Instance,
            services);

        using var cts = new CancellationTokenSource();
        await listener.StartAsync(cts.Token);

        // Create the saga state first — without it the non-starter steps would defer
        // (and, with no IDeferralStore registered, dead-letter).
        var startProducer = await transport.CreateProducerAsync<FanStart>("s2-start", new ProducerOptions());
        await startProducer.ProduceAsync(new FanStart { Id = "c2" });

        var store = services.GetRequiredService<IStateStore<FanState>>();
        var started = await PollUntilAsync(async () => await store.GetAsync("c2") != null, TimeSpan.FromSeconds(5));
        Assert.True(started, "Saga state was never created by the starter.");

        var sharedProducer = await transport.CreateProducerAsync<FanMsgA>("s2-shared", new ProducerOptions());
        var sharedProducerB = await transport.CreateProducerAsync<FanMsgB>("s2-shared", new ProducerOptions());
        await sharedProducer.ProduceAsync(new FanMsgA { Id = "c2" });
        await sharedProducerB.ProduceAsync(new FanMsgB { Id = "c2" });

        var bothRan = await PollUntilAsync(
            () => Task.FromResult(Volatile.Read(ref aRuns) >= 1 && Volatile.Read(ref bRuns) >= 1),
            TimeSpan.FromSeconds(10));
        Assert.True(bothRan, $"Not all shared-topic handlers fired (A={Volatile.Read(ref aRuns)}, B={Volatile.Read(ref bRuns)}).");

        Assert.Empty(await transport.ReadAllFromTopicAsync<FanMsgA>("s2-shared.dlq"));

        await listener.StopAsync(cts.Token);
    }

    // ---- Test 3: starter replay skip ----

    private class ReplayState { public string Id { get; set; } = ""; }
    private class ReplayStart { public string Id { get; set; } = ""; }

    [Fact]
    public async Task StarterReplay_WithExistingState_IsSkipped_AndNothingDeadLetters()
    {
        var transport = new InMemoryTransport();
        var registry = new SagaRegistry();

        var starterRuns = 0;
        var config = new SagaConfigurator<ReplayState>(registry);
        config.StartedBy<ReplayStart>("s3-start",
            (msg, ctx) =>
            {
                Interlocked.Increment(ref starterRuns);
                return Task.FromResult(ctx.Transition(new ReplayState { Id = msg.Id }));
            },
            m => m.Id);
        config.Complete();

        var services = new ServiceCollection()
            .AddSingleton<ITransport>(transport)
            .AddSingleton(typeof(IStateStore<>), typeof(InMemoryStateStore<>))
            .BuildServiceProvider();

        var listener = new TalariaListener(
            transport,
            new TopicRegistry(),
            registry,
            new TalariaOptions
            {
                ApplicationName = "test-app",
                DeferralBackoff = TimeSpan.FromMilliseconds(50),
                MaxDeferralAttempts = 5
            },
            NullLogger<TalariaListener>.Instance,
            services);

        using var cts = new CancellationTokenSource();
        await listener.StartAsync(cts.Token);

        // Same correlation, different MessageIds — the second is a replay, not a duplicate.
        var producer = await transport.CreateProducerAsync<ReplayStart>("s3-start", new ProducerOptions());
        await producer.ProduceAsync(new ReplayStart { Id = "c3" }, new MessageHeaders { MessageId = "replay-1" });
        await producer.ProduceAsync(new ReplayStart { Id = "c3" }, new MessageHeaders { MessageId = "replay-2" });

        var firstRan = await PollUntilAsync(
            () => Task.FromResult(Volatile.Read(ref starterRuns) >= 1), TimeSpan.FromSeconds(5));
        Assert.True(firstRan, "The starter handler never ran.");

        // Stability window: the replay must be consumed, skipped and committed —
        // the handler stays at one run and nothing lands in the DLQ.
        var stable = await PollStableAsync(
            async () => Volatile.Read(ref starterRuns) == 1
                && (await transport.ReadAllFromTopicAsync<ReplayStart>("s3-start.dlq")).Count == 0,
            TimeSpan.FromSeconds(2));

        Assert.True(stable, "The replayed starter was not skipped cleanly (handler re-ran or DLQ'd).");
        Assert.Equal(1, Volatile.Read(ref starterRuns));

        await listener.StopAsync(cts.Token);
    }

    // ---- Test 4: dispatch validation (no DispatchTo mapping) ----

    private class DispatchState { public string Id { get; set; } = ""; }
    private class DispatchStart { public string Id { get; set; } = ""; }
    private class UnmappedMsg { public string Data { get; set; } = ""; }

    // The engine validates dispatch routes before opening the transaction
    // (TalariaListener/SagaConsumerEngine): an unmapped dispatch type dead-letters the
    // consumed message with reason "unmapped_dispatch" and never saves saga state.
    // (Previously the exception escaped the consumer loop and the message was silently dropped.)
    [Fact]
    public async Task Dispatch_WithoutDispatchToMapping_RoutesToDlq_WithoutStateSave()
    {
        var transport = new InMemoryTransport();
        var registry = new SagaRegistry();

        var handlerRuns = 0;
        var config = new SagaConfigurator<DispatchState>(registry);
        config.StartedBy<DispatchStart>("s4-start",
            (msg, ctx) =>
            {
                Interlocked.Increment(ref handlerRuns);
                ctx.Dispatch(new UnmappedMsg { Data = "no-route" });
                return Task.FromResult(ctx.Transition(new DispatchState { Id = msg.Id }));
            },
            m => m.Id);
        // Deliberately NO config.DispatchTo<UnmappedMsg>(...) declared.
        config.Complete();

        var services = new ServiceCollection()
            .AddSingleton<ITransport>(transport)
            .AddSingleton(typeof(IStateStore<>), typeof(InMemoryStateStore<>))
            .BuildServiceProvider();

        var listener = new TalariaListener(
            transport,
            new TopicRegistry(),
            registry,
            new TalariaOptions
            {
                ApplicationName = "test-app",
                DeferralBackoff = TimeSpan.FromMilliseconds(50),
                MaxDeferralAttempts = 5
            },
            NullLogger<TalariaListener>.Instance,
            services);

        using var cts = new CancellationTokenSource();
        await listener.StartAsync(cts.Token);

        var producer = await transport.CreateProducerAsync<DispatchStart>("s4-start", new ProducerOptions());
        await producer.ProduceAsync(new DispatchStart { Id = "c4" });

        var ran = await PollUntilAsync(
            () => Task.FromResult(Volatile.Read(ref handlerRuns) >= 1), TimeSpan.FromSeconds(5));
        Assert.True(ran, "The starter handler never ran.");

        // The engine validates dispatch routes before opening the transaction: an unmapped
        // dispatch type is a configuration bug → the message dead-letters (reason
        // "unmapped_dispatch") and the saga state is never saved.
        var dlq = await ReadUntilAsync<DispatchStart>(transport, "s4-start.dlq", 1);
        Assert.Single(dlq);
        Assert.Equal("unmapped_dispatch", dlq[0].Headers.DlqReason);

        var store = services.GetRequiredService<IStateStore<DispatchState>>();
        Assert.Null(await store.GetAsync("c4"));
        Assert.Equal(1, Volatile.Read(ref handlerRuns));

        await listener.StopAsync(cts.Token);
    }

    // ---- Test 5: handler exception → lock released + DLQ ----

    private class FailState { public string Id { get; set; } = ""; }
    private class FailStart { public string Id { get; set; } = ""; }
    private class FailStep { public string Id { get; set; } = ""; }

    [Fact]
    public async Task HandlerException_RoutesToDlq_AndReleasesIdempotencyLock()
    {
        var transport = new InMemoryTransport();
        var idempotencyStore = new InMemoryIdempotencyStore();
        var registry = new SagaRegistry();

        var config = new SagaConfigurator<FailState>(registry);
        config.StartedBy<FailStart>("s5-start",
            (msg, ctx) => Task.FromResult(ctx.Transition(new FailState { Id = msg.Id })),
            m => m.Id);
        config.On<FailStep>("s5-step",
            (state, msg, ctx) => throw new InvalidOperationException("boom"),
            m => m.Id);
        config.Complete();

        var services = new ServiceCollection()
            .AddSingleton<ITransport>(transport)
            .AddSingleton(typeof(IStateStore<>), typeof(InMemoryStateStore<>))
            .AddSingleton<IIdempotencyStore>(idempotencyStore)
            .BuildServiceProvider();

        var listener = new TalariaListener(
            transport,
            new TopicRegistry(),
            registry,
            new TalariaOptions
            {
                ApplicationName = "test-app",
                DeferralBackoff = TimeSpan.FromMilliseconds(50),
                MaxDeferralAttempts = 5
            },
            NullLogger<TalariaListener>.Instance,
            services);

        using var cts = new CancellationTokenSource();
        await listener.StartAsync(cts.Token);

        // Establish saga state so the failing step is actually invoked (not deferred).
        var startProducer = await transport.CreateProducerAsync<FailStart>("s5-start", new ProducerOptions());
        await startProducer.ProduceAsync(new FailStart { Id = "c5" });

        var store = services.GetRequiredService<IStateStore<FailState>>();
        var started = await PollUntilAsync(async () => await store.GetAsync("c5") != null, TimeSpan.FromSeconds(5));
        Assert.True(started, "Saga state was never created by the starter.");

        var stepProducer = await transport.CreateProducerAsync<FailStep>("s5-step", new ProducerOptions());
        await stepProducer.ProduceAsync(new FailStep { Id = "c5" }, new MessageHeaders { MessageId = "fail-msg-1" });

        // The throwing handler routes the message to the step topic's DLQ. With default
        // options the exception detail is gated behind a generic note.
        var dlq = await ReadUntilAsync<FailStep>(transport, "s5-step.dlq", 1);
        Assert.Single(dlq);
        Assert.False(string.IsNullOrEmpty(dlq[0].Headers.DlqException), "DLQ message should carry a DlqException note.");

        // The failure path must release the idempotency lock, so it is re-acquirable.
        var reacquired = await idempotencyStore.TryAcquireLockAsync(
            "fail-msg-1", "test-app.s5-step", TimeSpan.FromMinutes(1));
        Assert.NotNull(reacquired);

        await listener.StopAsync(cts.Token);
    }

    // ---- Test 6: duplicate saga MessageId → exactly once ----

    private class DupState { public string Id { get; set; } = ""; }
    private class DupStart { public string Id { get; set; } = ""; }

    [Fact]
    public async Task DuplicateMessageId_StarterRunsExactlyOnce()
    {
        var transport = new InMemoryTransport();
        var idempotencyStore = new InMemoryIdempotencyStore();
        var registry = new SagaRegistry();

        var starterRuns = 0;
        var config = new SagaConfigurator<DupState>(registry);
        config.StartedBy<DupStart>("s6-start",
            (msg, ctx) =>
            {
                Interlocked.Increment(ref starterRuns);
                return Task.FromResult(ctx.Transition(new DupState { Id = msg.Id }));
            },
            m => m.Id);
        config.Complete();

        var services = new ServiceCollection()
            .AddSingleton<ITransport>(transport)
            .AddSingleton(typeof(IStateStore<>), typeof(InMemoryStateStore<>))
            .AddSingleton<IIdempotencyStore>(idempotencyStore)
            .BuildServiceProvider();

        var listener = new TalariaListener(
            transport,
            new TopicRegistry(),
            registry,
            new TalariaOptions
            {
                ApplicationName = "test-app",
                DeferralBackoff = TimeSpan.FromMilliseconds(50),
                MaxDeferralAttempts = 5
            },
            NullLogger<TalariaListener>.Instance,
            services);

        using var cts = new CancellationTokenSource();
        await listener.StartAsync(cts.Token);

        // Same MessageId produced twice — the idempotency gate must skip the second.
        var producer = await transport.CreateProducerAsync<DupStart>("s6-start", new ProducerOptions());
        await producer.ProduceAsync(new DupStart { Id = "c6" }, new MessageHeaders { MessageId = "dup-1" });
        await producer.ProduceAsync(new DupStart { Id = "c6" }, new MessageHeaders { MessageId = "dup-1" });

        var firstRan = await PollUntilAsync(
            () => Task.FromResult(Volatile.Read(ref starterRuns) >= 1), TimeSpan.FromSeconds(5));
        Assert.True(firstRan, "The starter handler never ran.");

        // Stability window: the duplicate delivery must not re-run the handler.
        var stable = await PollStableAsync(
            () => Task.FromResult(Volatile.Read(ref starterRuns) == 1),
            TimeSpan.FromSeconds(2));

        Assert.True(stable, "The duplicate MessageId re-ran the starter handler.");
        Assert.Equal(1, Volatile.Read(ref starterRuns));
        Assert.Empty(await transport.ReadAllFromTopicAsync<DupStart>("s6-start.dlq"));

        await listener.StopAsync(cts.Token);
    }

    // ---- Test 7: deferral commit failure releases lock and duplicate deferred copies are suppressed ----

    private class DeferCommitState { public string Id { get; set; } = ""; public string Log { get; set; } = ""; }
    private class DeferCommitStart { public string Id { get; set; } = ""; }
    private class DeferCommitStep { public string Id { get; set; } = ""; }

    [Fact]
    public async Task Deferral_CommitFailure_ReleasesLock_AndSuppressesDuplicateDeferredCopy()
    {
        var innerTransport = new InMemoryTransport();
        var transport = new CommitFailingInMemoryTransport(innerTransport);
        var deferralStore = new InMemoryDeferralStore();
        var idempotencyStore = new InMemoryIdempotencyStore();
        var registry = new SagaRegistry();

        var stepRuns = 0;
        var config = new SagaConfigurator<DeferCommitState>(registry);
        config.StartedBy<DeferCommitStart>("s7-start",
            (msg, ctx) => Task.FromResult(ctx.Transition(new DeferCommitState { Id = msg.Id, Log = "start" })),
            m => m.Id);
        config.On<DeferCommitStep>("s7-step",
            (state, msg, ctx) =>
            {
                state.Log += "+step";
                Interlocked.Increment(ref stepRuns);
                return Task.FromResult(ctx.Transition(state));
            },
            m => m.Id);
        config.Complete();

        var services = new ServiceCollection()
            .AddSingleton<ITransport>(transport)
            .AddSingleton(transport)
            .AddSingleton(typeof(IStateStore<>), typeof(InMemoryStateStore<>))
            .AddSingleton<IDeferralStore>(deferralStore)
            .AddSingleton<IIdempotencyStore>(idempotencyStore)
            .BuildServiceProvider();

        var options = new TalariaOptions
        {
            ApplicationName = "test-app",
            DeferralBackoff = TimeSpan.FromSeconds(1),
            MaxDeferralAttempts = 5
        };

        var listener = new TalariaListener(
            transport,
            new TopicRegistry(),
            registry,
            options,
            NullLogger<TalariaListener>.Instance,
            services,
            new TalariaListenerStores(idempotencyStore, deferralStore, null));

        await listener.StartAsync();

        // The first commit of the original step envelope will fail after the deferred copy is enqueued.
        transport.SetCommitFailures("s7-step-1", 1);

        var stepProducer = await transport.CreateProducerAsync<DeferCommitStep>("s7-step", new ProducerOptions());
        await stepProducer.ProduceAsync(new DeferCommitStep { Id = "c7" }, new MessageHeaders { MessageId = "s7-step-1" });

        // Wait until the out-of-order step has been deferred at least once.
        var deferred = await PollUntilAsync(() => Task.FromResult(deferralStore.Count >= 1), TimeSpan.FromSeconds(5));
        Assert.True(deferred, "The step message was never deferred.");

        await listener.StopAsync();

        // The commit-failure path must release the original idempotency lock so the
        // original message can redeliver promptly.
        var reacquired = await idempotencyStore.TryAcquireLockAsync(
            "s7-step-1", "test-app", TimeSpan.FromMinutes(1));
        Assert.NotNull(reacquired);
        await idempotencyStore.ReleaseLockAsync(reacquired);

        // Now start a fresh listener, produce the starter, and prove the step ran exactly once
        // even though the original redelivery enqueued a second deferred copy with the same id.
        var listener2 = new TalariaListener(
            transport,
            new TopicRegistry(),
            registry,
            options,
            NullLogger<TalariaListener>.Instance,
            services,
            new TalariaListenerStores(idempotencyStore, deferralStore, null));

        await listener2.StartAsync();

        var startProducer = await transport.CreateProducerAsync<DeferCommitStart>("s7-start", new ProducerOptions());
        await startProducer.ProduceAsync(new DeferCommitStart { Id = "c7" });

        var stepRan = await PollUntilAsync(
            () => Task.FromResult(Volatile.Read(ref stepRuns) >= 1), TimeSpan.FromSeconds(10));
        Assert.True(stepRan, "The deferred step handler never ran after the starter arrived.");

        var store = services.GetRequiredService<IStateStore<DeferCommitState>>();
        DeferCommitState? state = null;
        await PollUntilAsync(async () =>
        {
            state = await store.GetAsync("c7");
            return state is { Log: "start+step" };
        }, TimeSpan.FromSeconds(5));
        Assert.Equal("start+step", state?.Log);

        // No duplicate step processing and nothing dead-lettered.
        Assert.Equal(1, Volatile.Read(ref stepRuns));
        Assert.Empty(await innerTransport.ReadAllFromTopicAsync<DeferCommitStep>("s7-step.dlq"));
        Assert.Empty(await innerTransport.ReadAllFromTopicAsync<DeferCommitStart>("s7-start.dlq"));

        await listener2.StopAsync();
    }

    // ---- Test 8: OperationCanceledException during saga shutdown is not DLQ'd ----

    private class ShutdownState { public string Id { get; set; } = ""; }
    private class ShutdownStart { public string Id { get; set; } = ""; }

    [Fact]
    public async Task OperationCanceledException_DuringShutdown_IsNotDLQed_AndRedelivers()
    {
        var transport = new InMemoryTransport();
        var registry = new SagaRegistry();
        var attempts = 0;

        var config = new SagaConfigurator<ShutdownState>(registry);
        config.StartedBy<ShutdownStart>("s8-start",
            async (msg, ctx) =>
            {
                Interlocked.Increment(ref attempts);
                await Task.Delay(Timeout.Infinite, ctx.CancellationToken);
                return ctx.Transition(new ShutdownState { Id = msg.Id });
            },
            m => m.Id);
        config.Complete();

        var services = new ServiceCollection()
            .AddSingleton<ITransport>(transport)
            .AddSingleton(typeof(IStateStore<>), typeof(InMemoryStateStore<>))
            .BuildServiceProvider();

        var listener = new TalariaListener(
            transport,
            new TopicRegistry(),
            registry,
            new TalariaOptions
            {
                ApplicationName = "shutdown-app",
                DeferralBackoff = TimeSpan.FromMilliseconds(50),
                MaxDeferralAttempts = 5
            },
            NullLogger<TalariaListener>.Instance,
            services);

        await listener.StartAsync();

        var producer = await transport.CreateProducerAsync<ShutdownStart>("s8-start", new ProducerOptions());
        await producer.ProduceAsync(new ShutdownStart { Id = "c8" }, new MessageHeaders { MessageId = "shutdown-saga-1" });

        // Wait until the handler has entered before stopping.
        await PollUntilAsync(
            () => Task.FromResult(Volatile.Read(ref attempts) >= 1), TimeSpan.FromSeconds(5));

        // Stopping cancels the loop token, so the handler throws OCE.
        await listener.StopAsync();

        // The message must NOT be in the DLQ.
        Assert.Empty(await transport.ReadAllFromTopicAsync<ShutdownStart>("s8-start.dlq"));

        // Start a fresh listener; the uncommitted message redelivers.
        var received = new List<string>();
        var registry2 = new SagaRegistry();
        var config2 = new SagaConfigurator<ShutdownState>(registry2);
        config2.StartedBy<ShutdownStart>("s8-start",
            (msg, ctx) =>
            {
                received.Add(msg.Id);
                return Task.FromResult(ctx.Transition(new ShutdownState { Id = msg.Id }));
            },
            m => m.Id);
        config2.Complete();

        var listener2 = new TalariaListener(
            transport,
            new TopicRegistry(),
            registry2,
            new TalariaOptions { ApplicationName = "shutdown-app" },
            NullLogger<TalariaListener>.Instance,
            services);

        await listener2.StartAsync();

        await PollUntilAsync(
            () => Task.FromResult(received.Count == 1), TimeSpan.FromSeconds(5));
        Assert.Equal("c8", received[0]);

        await listener2.StopAsync();
    }

    // ---- Helpers (same poll-with-timeout pattern as other saga behavior tests) ----

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
