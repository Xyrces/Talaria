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
}
