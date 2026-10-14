// SPDX-License-Identifier: Apache-2.0

using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Talaria.Core;
using Talaria.Core.Abstractions;
using Talaria.Core.Diagnostics;
using Talaria.Core.Hosting;
using Talaria.Core.Registration;
using Talaria.Core.Sagas;
using Talaria.Transports.InMemory;

namespace Talaria.Specs.Tests;

/// <summary>
/// Given/When/Then behavior specifications for saga timeouts, timeout cancellation/superseding, and delayed saga dispatches.
/// </summary>
public sealed class SagaTimeoutBehaviorTests
{
    public sealed class TimeoutSagaState
    {
        public string Id { get; set; } = string.Empty;
        public string Phase { get; set; } = string.Empty;
        public int FiredTimeoutGeneration { get; set; }
    }

    public sealed record StartWorkflow(string CorrelationId);
    public sealed record ExtendDeadline(string CorrelationId);
    public sealed record CancelDeadline(string CorrelationId);
    public sealed record ReArmDeadline(string CorrelationId);
    public sealed record WorkflowTimeout(string CorrelationId, int Generation);
    public sealed record FollowUpCommand(string CorrelationId, string Action);

    [Fact]
    public async Task Given_OnTimeoutWithConventionAndExplicitTopic_When_TimeoutRequested_Then_RoutesWithoutManualDispatchTo()
    {
        var transport = new InMemoryTransport();
        var outboxStore = new InMemoryOutboxStore();
        var stateStore = new InMemoryStateStore<TimeoutSagaState>(outboxStore);
        var deferralStore = new InMemoryDeferralStore();
        var registry = new SagaRegistry();

        var customTopicFired = new TaskCompletionSource<WorkflowTimeout>(TaskCreationOptions.RunContinuationsAsynchronously);

        var config = new SagaConfigurator<TimeoutSagaState>(registry);
        config.StartedBy<StartWorkflow>(
            "timeout-explicit.start",
            (msg, ctx) =>
            {
                ctx.RequestTimeout(new WorkflowTimeout(msg.CorrelationId, 1), TimeSpan.FromMilliseconds(60));
                return Task.FromResult(ctx.Transition(new TimeoutSagaState { Id = msg.CorrelationId, Phase = "Started" }));
            },
            m => m.CorrelationId);

        config.OnTimeout<WorkflowTimeout>(
            "timeout-explicit.custom-timeout",
            (_, timeout, ctx) =>
            {
                customTopicFired.TrySetResult(timeout);
                return Task.FromResult(ctx.Complete());
            },
            m => m.CorrelationId);
        config.Complete();

        var services = new ServiceCollection()
            .AddSingleton<ITransport>(transport)
            .AddSingleton<IOutboxStore>(outboxStore)
            .AddSingleton<IStateStore<TimeoutSagaState>>(stateStore)
            .AddSingleton<IDeferralStore>(deferralStore)
            .BuildServiceProvider();

        var listener = new TalariaListener(
            transport,
            new TopicRegistry(),
            registry,
            new TalariaOptions
            {
                ApplicationName = "timeout-explicit-app",
                OutboxRelayInterval = TimeSpan.FromMilliseconds(15),
                DeferralBackoff = TimeSpan.FromMilliseconds(20)
            },
            NullLogger<TalariaListener>.Instance,
            services);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await listener.StartAsync(cts.Token);

        try
        {
            await using var producer = await transport.CreateProducerAsync<StartWorkflow>("timeout-explicit.start", new ProducerOptions());
            await producer.ProduceAsync(new StartWorkflow("wf-explicit-1"));

            var fired = await customTopicFired.Task.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.Equal("wf-explicit-1", fired.CorrelationId);
            Assert.Equal(1, fired.Generation);

            var completed = await TestAsyncHelpers.PollUntilAsync(
                async () => await stateStore.GetAsync("wf-explicit-1") is null,
                TimeSpan.FromSeconds(3));
            Assert.True(completed);
        }
        finally
        {
            await listener.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task Given_PendingTimeout_When_SubsequentStepRequestsNewTimeoutOfSameType_Then_EarlierTimeoutIsSupersededAndLaterTimeoutFires()
    {
        var transport = new InMemoryTransport();
        var outboxStore = new InMemoryOutboxStore();
        var stateStore = new InMemoryStateStore<TimeoutSagaState>(outboxStore);
        var deferralStore = new InMemoryDeferralStore();
        var registry = new SagaRegistry();

        var firedTimeouts = new ConcurrentQueue<WorkflowTimeout>();

        var config = new SagaConfigurator<TimeoutSagaState>(registry);
        config.StartedBy<StartWorkflow>(
            "supersede.start",
            (msg, ctx) =>
            {
                ctx.RequestTimeout(new WorkflowTimeout(msg.CorrelationId, Generation: 1), TimeSpan.FromMilliseconds(80));
                return Task.FromResult(ctx.Transition(new TimeoutSagaState { Id = msg.CorrelationId, Phase = "V1" }));
            },
            m => m.CorrelationId);

        config.On<ExtendDeadline>(
            "supersede.extend",
            (state, msg, ctx) =>
            {
                state.Phase = "V2";
                ctx.RequestTimeout(new WorkflowTimeout(msg.CorrelationId, Generation: 2), TimeSpan.FromMilliseconds(220));
                return Task.FromResult(ctx.Transition(state));
            },
            m => m.CorrelationId);

        config.OnTimeout<WorkflowTimeout>(
            (state, timeout, ctx) =>
            {
                firedTimeouts.Enqueue(timeout);
                state.Phase = $"TimedOutGen{timeout.Generation}";
                state.FiredTimeoutGeneration = timeout.Generation;
                return Task.FromResult(ctx.Transition(state));
            },
            m => m.CorrelationId);
        config.Complete();

        var services = new ServiceCollection()
            .AddSingleton<ITransport>(transport)
            .AddSingleton<IOutboxStore>(outboxStore)
            .AddSingleton<IStateStore<TimeoutSagaState>>(stateStore)
            .AddSingleton<IDeferralStore>(deferralStore)
            .BuildServiceProvider();

        var listener = new TalariaListener(
            transport,
            new TopicRegistry(),
            registry,
            new TalariaOptions
            {
                ApplicationName = "supersede-app",
                OutboxRelayInterval = TimeSpan.FromMilliseconds(15),
                DeferralBackoff = TimeSpan.FromMilliseconds(20)
            },
            NullLogger<TalariaListener>.Instance,
            services);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await listener.StartAsync(cts.Token);

        try
        {
            await using var startProducer = await transport.CreateProducerAsync<StartWorkflow>("supersede.start", new ProducerOptions());
            await startProducer.ProduceAsync(new StartWorkflow("wf-sup-1"));

            var v1Ready = await TestAsyncHelpers.PollUntilAsync(
                async () => (await stateStore.GetAsync("wf-sup-1"))?.Phase == "V1",
                TimeSpan.FromSeconds(3));
            Assert.True(v1Ready);

            await using var extendProducer = await transport.CreateProducerAsync<ExtendDeadline>("supersede.extend", new ProducerOptions());
            await extendProducer.ProduceAsync(new ExtendDeadline("wf-sup-1"));

            var v2Ready = await TestAsyncHelpers.PollUntilAsync(
                async () => (await stateStore.GetAsync("wf-sup-1"))?.Phase == "V2",
                TimeSpan.FromSeconds(3));
            Assert.True(v2Ready);

            // Wait until Generation 2 timeout fires (around 220ms)
            var gen2Fired = await TestAsyncHelpers.PollUntilAsync(
                () => Task.FromResult(!firedTimeouts.IsEmpty),
                TimeSpan.FromSeconds(5));
            Assert.True(gen2Fired);

            var singleFired = Assert.Single(firedTimeouts);
            Assert.Equal(2, singleFired.Generation);

            var finalState = await stateStore.GetAsync("wf-sup-1");
            Assert.NotNull(finalState);
            Assert.Equal("TimedOutGen2", finalState!.Phase);
            Assert.Equal(2, finalState.FiredTimeoutGeneration);
        }
        finally
        {
            await listener.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task Given_CanceledTimeout_When_SubsequentStepReRequestsTimeout_Then_FirstTimeoutIsSuppressedAndSecondTimeoutFires()
    {
        var transport = new InMemoryTransport();
        var outboxStore = new InMemoryOutboxStore();
        var stateStore = new InMemoryStateStore<TimeoutSagaState>(outboxStore);
        var deferralStore = new InMemoryDeferralStore();
        var registry = new SagaRegistry();

        var firedTimeouts = new ConcurrentQueue<WorkflowTimeout>();

        var config = new SagaConfigurator<TimeoutSagaState>(registry);
        config.StartedBy<StartWorkflow>(
            "rearm.start",
            (msg, ctx) =>
            {
                ctx.RequestTimeout(new WorkflowTimeout(msg.CorrelationId, Generation: 1), TimeSpan.FromMilliseconds(90));
                return Task.FromResult(ctx.Transition(new TimeoutSagaState { Id = msg.CorrelationId, Phase = "Armed1" }));
            },
            m => m.CorrelationId);

        config.On<CancelDeadline>(
            "rearm.cancel",
            (state, _, ctx) =>
            {
                state.Phase = "Canceled";
                ctx.CancelTimeout<WorkflowTimeout>();
                return Task.FromResult(ctx.Transition(state));
            },
            m => m.CorrelationId);

        config.On<ReArmDeadline>(
            "rearm.rearm",
            (state, msg, ctx) =>
            {
                state.Phase = "Armed3";
                ctx.RequestTimeout(new WorkflowTimeout(msg.CorrelationId, Generation: 3), TimeSpan.FromMilliseconds(180));
                return Task.FromResult(ctx.Transition(state));
            },
            m => m.CorrelationId);

        config.OnTimeout<WorkflowTimeout>(
            (state, timeout, ctx) =>
            {
                firedTimeouts.Enqueue(timeout);
                state.FiredTimeoutGeneration = timeout.Generation;
                return Task.FromResult(ctx.Complete());
            },
            m => m.CorrelationId);
        config.Complete();

        var services = new ServiceCollection()
            .AddSingleton<ITransport>(transport)
            .AddSingleton<IOutboxStore>(outboxStore)
            .AddSingleton<IStateStore<TimeoutSagaState>>(stateStore)
            .AddSingleton<IDeferralStore>(deferralStore)
            .BuildServiceProvider();

        var listener = new TalariaListener(
            transport,
            new TopicRegistry(),
            registry,
            new TalariaOptions
            {
                ApplicationName = "rearm-app",
                OutboxRelayInterval = TimeSpan.FromMilliseconds(15),
                DeferralBackoff = TimeSpan.FromMilliseconds(20)
            },
            NullLogger<TalariaListener>.Instance,
            services);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await listener.StartAsync(cts.Token);

        try
        {
            await using var startProducer = await transport.CreateProducerAsync<StartWorkflow>("rearm.start", new ProducerOptions());
            await startProducer.ProduceAsync(new StartWorkflow("wf-rearm-1"));
            Assert.True(await TestAsyncHelpers.PollUntilAsync(async () => (await stateStore.GetAsync("wf-rearm-1"))?.Phase == "Armed1", TimeSpan.FromSeconds(3)));

            await using var cancelProducer = await transport.CreateProducerAsync<CancelDeadline>("rearm.cancel", new ProducerOptions());
            await cancelProducer.ProduceAsync(new CancelDeadline("wf-rearm-1"));
            Assert.True(await TestAsyncHelpers.PollUntilAsync(async () => (await stateStore.GetAsync("wf-rearm-1"))?.Phase == "Canceled", TimeSpan.FromSeconds(3)));

            await using var rearmProducer = await transport.CreateProducerAsync<ReArmDeadline>("rearm.rearm", new ProducerOptions());
            await rearmProducer.ProduceAsync(new ReArmDeadline("wf-rearm-1"));
            Assert.True(await TestAsyncHelpers.PollUntilAsync(async () => (await stateStore.GetAsync("wf-rearm-1"))?.Phase == "Armed3", TimeSpan.FromSeconds(3)));

            // Wait for the re-armed timeout (Generation 3) to fire and complete the saga
            Assert.True(await TestAsyncHelpers.PollUntilAsync(() => Task.FromResult(!firedTimeouts.IsEmpty), TimeSpan.FromSeconds(5)));

            var single = Assert.Single(firedTimeouts);
            Assert.Equal(3, single.Generation);
            Assert.Null(await stateStore.GetAsync("wf-rearm-1"));
        }
        finally
        {
            await listener.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task Given_SagaStepCallingDelayedDispatch_When_TransitionCommits_Then_OutboxEntryIsScheduledAtomicallyAndDeliveredOnceDue()
    {
        var transport = new InMemoryTransport();
        var outboxStore = new InMemoryOutboxStore();
        var stateStore = new InMemoryStateStore<TimeoutSagaState>(outboxStore);
        var deferralStore = new InMemoryDeferralStore();
        var registry = new SagaRegistry();

        var scheduledMetricCount = 0L;
        using var meterListener = new MeterListener();
        meterListener.InstrumentPublished = (instrument, listener) =>
        {
            if (instrument.Meter.Name == TalariaDiagnostics.MeterName && instrument.Name == "talaria.messaging.scheduled")
            {
                listener.EnableMeasurementEvents(instrument);
            }
        };
        meterListener.SetMeasurementEventCallback<long>((_, measurement, tags, _) =>
        {
            foreach (var tag in tags)
            {
                if (tag.Key == "messaging.destination.name" && Equals(tag.Value, "delayed-followup-topic"))
                {
                    Interlocked.Add(ref scheduledMetricCount, measurement);
                }
            }
        });
        meterListener.Start();

        var config = new SagaConfigurator<TimeoutSagaState>(registry);
        config.StartedBy<StartWorkflow>(
            "delayed-dispatch.start",
            (msg, ctx) =>
            {
                ctx.Dispatch(new FollowUpCommand(msg.CorrelationId, "RemindCustomer"), TimeSpan.FromMilliseconds(180));
                return Task.FromResult(ctx.Transition(new TimeoutSagaState { Id = msg.CorrelationId, Phase = "DispatchedDelayed" }));
            },
            m => m.CorrelationId);
        config.DispatchTo<FollowUpCommand>("delayed-followup-topic");
        config.Complete();

        var services = new ServiceCollection()
            .AddSingleton<ITransport>(transport)
            .AddSingleton<IOutboxStore>(outboxStore)
            .AddSingleton<IStateStore<TimeoutSagaState>>(stateStore)
            .AddSingleton<IDeferralStore>(deferralStore)
            .BuildServiceProvider();

        var listener = new TalariaListener(
            transport,
            new TopicRegistry(),
            registry,
            new TalariaOptions
            {
                ApplicationName = "delayed-dispatch-app",
                OutboxRelayInterval = TimeSpan.FromMilliseconds(15),
                DeferralBackoff = TimeSpan.FromMilliseconds(20)
            },
            NullLogger<TalariaListener>.Instance,
            services);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await listener.StartAsync(cts.Token);

        try
        {
            await using var startProducer = await transport.CreateProducerAsync<StartWorkflow>("delayed-dispatch.start", new ProducerOptions());
            await startProducer.ProduceAsync(new StartWorkflow("wf-delayed-1"));

            Assert.True(await TestAsyncHelpers.PollUntilAsync(
                async () => (await stateStore.GetAsync("wf-delayed-1"))?.Phase == "DispatchedDelayed",
                TimeSpan.FromSeconds(3)));

            // Outbox entry is staged with ScheduledAt in the future, so it should not be published immediately
            Assert.Equal(1, outboxStore.Count);
            Assert.Equal(1, Interlocked.Read(ref scheduledMetricCount));
            Assert.Empty(await transport.ReadAllFromTopicAsync<FollowUpCommand>("delayed-followup-topic"));

            // Once due, the outbox relay publishes it to the target topic
            FollowUpCommand? delivered = null;
            var arrived = await TestAsyncHelpers.PollUntilAsync(async () =>
            {
                var msgs = await transport.ReadAllFromTopicAsync<FollowUpCommand>("delayed-followup-topic");
                if (msgs.Count > 0)
                {
                    delivered = msgs[0].Payload;
                    return true;
                }
                return false;
            }, TimeSpan.FromSeconds(5));

            Assert.True(arrived);
            Assert.NotNull(delivered);
            Assert.Equal("wf-delayed-1", delivered!.CorrelationId);
            Assert.Equal("RemindCustomer", delivered.Action);
        }
        finally
        {
            await listener.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task Given_TimeoutMessageArrivesWhenNoStateExists_When_ProcessedBySagaConsumerEngine_Then_CompletesWithoutDeferringOrInvokingHandler()
    {
        var transport = new InMemoryTransport();
        var outboxStore = new InMemoryOutboxStore();
        var stateStore = new InMemoryStateStore<TimeoutSagaState>(outboxStore);
        var deferralStore = new InMemoryDeferralStore();
        var registry = new SagaRegistry();

        var handlerRuns = 0;
        var timeoutTopic = MessageNames.Destination(MessageNames.Contract(typeof(WorkflowTimeout)));

        var config = new SagaConfigurator<TimeoutSagaState>(registry);
        config.StartedBy<StartWorkflow>(
            "stale-timeout.start",
            (msg, ctx) => Task.FromResult(ctx.Transition(new TimeoutSagaState { Id = msg.CorrelationId })),
            m => m.CorrelationId);
        config.OnTimeout<WorkflowTimeout>(
            (state, _, ctx) =>
            {
                Interlocked.Increment(ref handlerRuns);
                return Task.FromResult(ctx.Transition(state));
            },
            m => m.CorrelationId);
        config.Complete();

        var services = new ServiceCollection()
            .AddSingleton<ITransport>(transport)
            .AddSingleton<IOutboxStore>(outboxStore)
            .AddSingleton<IStateStore<TimeoutSagaState>>(stateStore)
            .AddSingleton<IDeferralStore>(deferralStore)
            .BuildServiceProvider();

        var listener = new TalariaListener(
            transport,
            new TopicRegistry(),
            registry,
            new TalariaOptions
            {
                ApplicationName = "stale-timeout-app",
                OutboxRelayInterval = TimeSpan.FromMilliseconds(15),
                DeferralBackoff = TimeSpan.FromMilliseconds(20)
            },
            NullLogger<TalariaListener>.Instance,
            services);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await listener.StartAsync(cts.Token);

        try
        {
            await using var producer = await transport.CreateProducerAsync<WorkflowTimeout>(timeoutTopic, new ProducerOptions());
            await producer.ProduceAsync(new WorkflowTimeout("non-existent-saga", 1));

            var stayedEmpty = await TestAsyncHelpers.PollStableAsync(
                () => Task.FromResult(deferralStore.Count == 0 && Volatile.Read(ref handlerRuns) == 0),
                TimeSpan.FromMilliseconds(150));

            Assert.True(stayedEmpty);
        }
        finally
        {
            await listener.StopAsync(CancellationToken.None);
        }
    }

    public sealed class SagaStateA
    {
        public string Id { get; set; } = string.Empty;
        public string Status { get; set; } = string.Empty;
    }

    public sealed class SagaStateB
    {
        public string Id { get; set; } = string.Empty;
        public string Status { get; set; } = string.Empty;
    }

    public sealed record SharedDeadline(string CorrelationId);
    public sealed record EmptyDeadline();

    [Fact]
    public async Task Given_TwoSagasSharingTimeoutTypeAndCorrelationId_When_SagaATimeoutFires_Then_SagaBTimeoutHandlerIsNotInvoked()
    {
        var transport = new InMemoryTransport();
        var outboxStore = new InMemoryOutboxStore();
        var stateStoreA = new InMemoryStateStore<SagaStateA>(outboxStore);
        var stateStoreB = new InMemoryStateStore<SagaStateB>(outboxStore);
        var deferralStore = new InMemoryDeferralStore();
        var registry = new SagaRegistry();

        var sagaATimeoutRuns = 0;
        var sagaBTimeoutRuns = 0;

        var configA = new SagaConfigurator<SagaStateA>(registry);
        configA.StartedBy<StartWorkflow>(
            "shared-timeout.start",
            (msg, ctx) =>
            {
                ctx.RequestTimeout(new SharedDeadline(msg.CorrelationId), TimeSpan.FromMilliseconds(70));
                return Task.FromResult(ctx.Transition(new SagaStateA { Id = msg.CorrelationId, Status = "WaitingA" }));
            },
            m => m.CorrelationId);
        configA.OnTimeout<SharedDeadline>(
            (_, _, ctx) =>
            {
                Interlocked.Increment(ref sagaATimeoutRuns);
                return Task.FromResult(ctx.Complete());
            },
            m => m.CorrelationId);
        configA.Complete();

        var configB = new SagaConfigurator<SagaStateB>(registry);
        configB.StartedBy<StartWorkflow>(
            "shared-timeout.start",
            (msg, ctx) =>
            {
                ctx.RequestTimeout(new SharedDeadline(msg.CorrelationId), TimeSpan.FromHours(1));
                return Task.FromResult(ctx.Transition(new SagaStateB { Id = msg.CorrelationId, Status = "WaitingB" }));
            },
            m => m.CorrelationId);
        configB.OnTimeout<SharedDeadline>(
            (_, _, ctx) =>
            {
                Interlocked.Increment(ref sagaBTimeoutRuns);
                return Task.FromResult(ctx.Complete());
            },
            m => m.CorrelationId);
        configB.Complete();

        var services = new ServiceCollection()
            .AddSingleton<ITransport>(transport)
            .AddSingleton<IOutboxStore>(outboxStore)
            .AddSingleton<IStateStore<SagaStateA>>(stateStoreA)
            .AddSingleton<IStateStore<SagaStateB>>(stateStoreB)
            .AddSingleton<IDeferralStore>(deferralStore)
            .BuildServiceProvider();

        var listener = new TalariaListener(
            transport,
            new TopicRegistry(),
            registry,
            new TalariaOptions
            {
                ApplicationName = "shared-timeout-isolation-app",
                OutboxRelayInterval = TimeSpan.FromMilliseconds(15),
                DeferralBackoff = TimeSpan.FromMilliseconds(20)
            },
            NullLogger<TalariaListener>.Instance,
            services);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await listener.StartAsync(cts.Token);

        try
        {
            await using var startProducer = await transport.CreateProducerAsync<StartWorkflow>("shared-timeout.start", new ProducerOptions());
            await startProducer.ProduceAsync(new StartWorkflow("shared-corr-1"));

            Assert.True(await TestAsyncHelpers.PollUntilAsync(
                async () => (await stateStoreB.GetAsync("shared-corr-1"))?.Status == "WaitingB",
                TimeSpan.FromSeconds(3)));

            // Wait for Saga A's 70ms timeout to fire and complete Saga A
            Assert.True(await TestAsyncHelpers.PollUntilAsync(
                async () => Volatile.Read(ref sagaATimeoutRuns) == 1 && await stateStoreA.GetAsync("shared-corr-1") is null,
                TimeSpan.FromSeconds(5)));

            // Ensure Saga B's timeout handler was NOT invoked by Saga A's timeout and Saga B remains active
            var sagaBStayedActive = await TestAsyncHelpers.PollStableAsync(
                async () => Volatile.Read(ref sagaBTimeoutRuns) == 0 && (await stateStoreB.GetAsync("shared-corr-1"))?.Status == "WaitingB",
                TimeSpan.FromMilliseconds(150));

            Assert.True(sagaBStayedActive);
        }
        finally
        {
            await listener.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task Given_PropertylessTimeoutContract_When_TimeoutRequested_Then_PreservesSagaCorrelationIdInHeadersAndCompletesSaga()
    {
        var transport = new InMemoryTransport();
        var outboxStore = new InMemoryOutboxStore();
        var stateStore = new InMemoryStateStore<TimeoutSagaState>(outboxStore);
        var deferralStore = new InMemoryDeferralStore();
        var registry = new SagaRegistry();

        var timeoutFired = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var timeoutTopic = MessageNames.Destination(MessageNames.Contract(typeof(EmptyDeadline)));

        var config = new SagaConfigurator<TimeoutSagaState>(registry);
        config.StartedBy<StartWorkflow>(
            "empty-deadline.start",
            (msg, ctx) =>
            {
                ctx.RequestTimeout(new EmptyDeadline(), TimeSpan.Zero);
                return Task.FromResult(ctx.Transition(new TimeoutSagaState { Id = msg.CorrelationId, Phase = "Started" }));
            },
            m => m.CorrelationId);

        config.OnTimeout<EmptyDeadline>((state, _, ctx) =>
        {
            timeoutFired.TrySetResult(state.Id);
            return Task.FromResult(ctx.Complete());
        });
        config.Complete();

        var services = new ServiceCollection()
            .AddSingleton<ITransport>(transport)
            .AddSingleton<IOutboxStore>(outboxStore)
            .AddSingleton<IStateStore<TimeoutSagaState>>(stateStore)
            .AddSingleton<IDeferralStore>(deferralStore)
            .BuildServiceProvider();

        var listener = new TalariaListener(
            transport,
            new TopicRegistry(),
            registry,
            new TalariaOptions
            {
                ApplicationName = "empty-deadline-app",
                OutboxRelayInterval = TimeSpan.FromMilliseconds(15),
                DeferralBackoff = TimeSpan.FromMilliseconds(20)
            },
            NullLogger<TalariaListener>.Instance,
            services);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await listener.StartAsync(cts.Token);

        try
        {
            await using var startProducer = await transport.CreateProducerAsync<StartWorkflow>("empty-deadline.start", new ProducerOptions());
            await startProducer.ProduceAsync(new StartWorkflow("corr-empty-deadline-1"));

            var firedSagaId = await timeoutFired.Task.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.Equal("corr-empty-deadline-1", firedSagaId);

            Assert.True(await TestAsyncHelpers.PollUntilAsync(
                async () => await stateStore.GetAsync("corr-empty-deadline-1") is null,
                TimeSpan.FromSeconds(3)));

            Assert.Empty(await transport.ReadAllFromTopicAsync<EmptyDeadline>(timeoutTopic + ".dlq"));
        }
        finally
        {
            await listener.StopAsync(CancellationToken.None);
        }
    }

    private sealed class LegacySixArgStateStore<TState> : IStateStore<TState> where TState : class, new()
    {
        public int SixArgCalls { get; private set; }

        public Task<SagaSnapshot<TState>> ReadSnapshotAsync(string correlationId, string messageId, CancellationToken ct = default)
            => Task.FromResult(new SagaSnapshot<TState>(new TState(), 1, false, false));

        public Task<SagaCommitStatus> TryTransitionAsync(
            string correlationId,
            string messageId,
            long expectedVersion,
            TState? newState,
            IReadOnlyList<OutboxMessage> outbox,
            CancellationToken ct = default)
        {
            SixArgCalls++;
            return Task.FromResult(SagaCommitStatus.Committed);
        }

        public Task<TState?> GetAsync(string correlationId, CancellationToken ct = default) => Task.FromResult<TState?>(null);
        public Task SaveAsync(string correlationId, TState state, CancellationToken ct = default) => Task.CompletedTask;
        public Task DeleteAsync(string correlationId, CancellationToken ct = default) => Task.CompletedTask;
        public Task TransitionAsync(string correlationId, TState? newState, IReadOnlyList<OutboxMessage> outbox, CancellationToken ct = default) => Task.CompletedTask;
    }

    [Fact]
    public async Task Given_LegacyStateStoreWithoutAdditionalReceiptsOverride_When_TryTransitionCalledWithCancellationReceipts_Then_ThrowsNotSupportedException()
    {
        var legacyImpl = new LegacySixArgStateStore<TimeoutSagaState>();
        IStateStore<TimeoutSagaState> store = legacyImpl;

        // Empty additionalReceipts delegates cleanly to the 6-argument overload
        var status = await store.TryTransitionAsync("c1", "m1", 0, new TimeoutSagaState(), [], []);
        Assert.Equal(SagaCommitStatus.Committed, status);
        Assert.Equal(1, legacyImpl.SixArgCalls);

        // Non-empty additionalReceipts throws NotSupportedException instead of silently dropping cancellation receipts
        await Assert.ThrowsAsync<NotSupportedException>(() =>
            store.TryTransitionAsync("c1", "m2", 1, new TimeoutSagaState(), [], ["timeout-canceled:SomeTimeout:v1"]));
        Assert.Equal(1, legacyImpl.SixArgCalls);
    }

    private sealed class LegacySagaContext<TState> : ISagaContext<TState>
    {
        public CancellationToken CancellationToken => CancellationToken.None;
        public ISagaContext<TState> Dispatch<TMessage>(TMessage message) where TMessage : class => this;
        public SagaResult<TState> Complete() => SagaResult<TState>.Complete([]);
        public SagaResult<TState> Transition(TState newState) => SagaResult<TState>.Transition(newState, []);
        public SagaResult<TState> Defer() => SagaResult<TState>.Defer();
    }

    [Fact]
    public void Given_LegacyCustomISagaContextImplementation_When_CallingNewTimeoutAndDelayedDispatchMethods_Then_CompilesAndThrowsNotSupportedException()
    {
        ISagaContext<TimeoutSagaState> ctx = new LegacySagaContext<TimeoutSagaState>();

        Assert.Throws<ArgumentNullException>(() => ctx.Dispatch<FollowUpCommand>(null!, TimeSpan.FromSeconds(1)));
        Assert.Throws<ArgumentOutOfRangeException>(() => ctx.Dispatch(new FollowUpCommand("1", "a"), TimeSpan.FromSeconds(-1)));
        Assert.Throws<NotSupportedException>(() => ctx.Dispatch(new FollowUpCommand("1", "a"), TimeSpan.FromSeconds(5)));
        Assert.Throws<NotSupportedException>(() => ctx.Dispatch(new FollowUpCommand("1", "a"), DateTimeOffset.UtcNow.AddSeconds(5)));

        Assert.Throws<ArgumentNullException>(() => ctx.RequestTimeout<WorkflowTimeout>(null!, TimeSpan.FromSeconds(1)));
        Assert.Throws<ArgumentOutOfRangeException>(() => ctx.RequestTimeout(new WorkflowTimeout("1", 1), TimeSpan.FromSeconds(-1)));
        Assert.Throws<NotSupportedException>(() => ctx.RequestTimeout(new WorkflowTimeout("1", 1), TimeSpan.FromSeconds(5)));
        Assert.Throws<NotSupportedException>(() => ctx.RequestTimeout(new WorkflowTimeout("1", 1), DateTimeOffset.UtcNow.AddSeconds(5)));
        Assert.Throws<NotSupportedException>(() => ctx.CancelTimeout<WorkflowTimeout>());
    }
}

