// SPDX-License-Identifier: Apache-2.0

using System.Diagnostics;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Talaria.Core;
using Talaria.Core.Abstractions;
using Talaria.Core.Diagnostics;
using Talaria.Core.Hosting;
using Talaria.Core.Registration;
using Talaria.Transports.InMemory;

namespace Talaria.Specs.Tests;

/// <summary>
/// Given/When/Then behavior specifications for scheduled send/publish and scheduled outbox visibility.
/// </summary>
public sealed class ScheduledMessagingBehaviorTests
{
    public sealed record ScheduledCommand(string Id);
    public sealed record ScheduledEvent(string Id);

    private sealed class FakeTimeProvider(DateTimeOffset initialUtcNow) : TimeProvider
    {
        private DateTimeOffset _utcNow = initialUtcNow;
        public override DateTimeOffset GetUtcNow() => _utcNow;
        public void Advance(TimeSpan delta) => _utcNow = _utcNow.Add(delta);
    }

    private sealed class RecordingMessageOutbox : IMessageOutbox
    {
        public List<OutboxMessage> Staged { get; } = [];

        public Task StageAsync(OutboxMessage message, CancellationToken ct = default)
        {
            Staged.Add(message);
            return Task.CompletedTask;
        }

        public Task FlushAsync(CancellationToken ct = default) => Task.CompletedTask;
    }

    [Fact]
    public async Task Given_ConventionCommandAndEvent_When_ScheduledByDelayAndDueAt_Then_EnqueuesWithExpectedHeadersAndDueTimes()
    {
        var transport = new InMemoryTransport();
        var deferralStore = new InMemoryDeferralStore();
        var baseTime = new DateTimeOffset(2026, 10, 14, 12, 0, 0, TimeSpan.Zero);
        var timeProvider = new FakeTimeProvider(baseTime);

        var services = new ServiceCollection();
        services.AddSingleton<TimeProvider>(timeProvider);
        services.AddTalaria(opts => opts.ApplicationName = "sched-conv-app")
            .UseInMemoryTransport(transport)
            .UseInMemoryDeferralStore();
        services.AddSingleton<IDeferralStore>(deferralStore);

        await using var provider = services.BuildServiceProvider();
        provider.MapCommand<ScheduledCommand>((ScheduledCommand _) => Task.CompletedTask);
        provider.MapEvent<ScheduledEvent>((ScheduledEvent _) => Task.CompletedTask);

        double scheduledMetricCount = 0;
        using var meterListener = new MeterListener();
        meterListener.InstrumentPublished = (instrument, listener) =>
        {
            if (instrument.Meter.Name == TalariaDiagnostics.MeterName && instrument.Name == "talaria.messaging.scheduled")
            {
                listener.EnableMeasurementEvents(instrument);
            }
        };
        meterListener.SetMeasurementEventCallback<long>((_, measurement, _, _) => scheduledMetricCount += measurement);
        meterListener.Start();

        await using var scope = provider.CreateAsyncScope();
        var bus = scope.ServiceProvider.GetRequiredService<IMessageBus>();

        var delay = TimeSpan.FromMinutes(5);
        await bus.ScheduleSendAsync(new ScheduledCommand("cmd-1"), delay);

        var explicitDueAt = baseTime.AddMinutes(10);
        await bus.SchedulePublishAsync(new ScheduledEvent("evt-1"), explicitDueAt);

        Assert.Equal(2, deferralStore.Count);
        Assert.Equal(2, scheduledMetricCount);

        // Not due before 5 minutes
        Assert.Empty(await deferralStore.AcquireDueAsync(baseTime.AddMinutes(4), TimeSpan.FromSeconds(30), 10));

        // First entry due at +5 minutes
        var dueAtFive = Assert.Single(await deferralStore.AcquireDueAsync(baseTime.AddMinutes(5), TimeSpan.FromSeconds(30), 10));
        Assert.Equal(MessageNames.Destination(MessageNames.Contract(typeof(ScheduledCommand))), dueAtFive.Message.Topic);
        Assert.Equal("command", dueAtFive.Message.Headers["talaria.intent"]);
        Assert.Equal(baseTime.Add(delay).ToString("O"), dueAtFive.Message.Headers["talaria.scheduled_due_at"]);
        Assert.False(string.IsNullOrWhiteSpace(dueAtFive.Message.Headers.MessageId));
        Assert.True(await deferralStore.CompleteAsync(dueAtFive.Lease));

        // Second entry due at +10 minutes
        var dueAtTen = Assert.Single(await deferralStore.AcquireDueAsync(baseTime.AddMinutes(10), TimeSpan.FromSeconds(30), 10));
        Assert.Equal(MessageNames.Destination(MessageNames.Contract(typeof(ScheduledEvent))), dueAtTen.Message.Topic);
        Assert.Equal("event", dueAtTen.Message.Headers["talaria.intent"]);
        Assert.Equal(explicitDueAt.ToString("O"), dueAtTen.Message.Headers["talaria.scheduled_due_at"]);
        Assert.True(await deferralStore.CompleteAsync(dueAtTen.Lease));
    }

    [Fact]
    public async Task Given_ExplicitDestinations_When_ScheduleSendAndSchedulePublishCalled_Then_RoutesToExplicitDestinationsAndPropagatesTraceContext()
    {
        var transport = new InMemoryTransport();
        var deferralStore = new InMemoryDeferralStore();
        var baseTime = new DateTimeOffset(2026, 10, 14, 12, 0, 0, TimeSpan.Zero);
        var timeProvider = new FakeTimeProvider(baseTime);

        var services = new ServiceCollection();
        services.AddSingleton<TimeProvider>(timeProvider);
        services.AddTalaria(opts => opts.ApplicationName = "sched-explicit-app")
            .UseInMemoryTransport(transport)
            .UseInMemoryDeferralStore();
        services.AddSingleton<IDeferralStore>(deferralStore);

        await using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        var bus = scope.ServiceProvider.GetRequiredService<IMessageBus>();

        using var activitySource = new ActivitySource("Talaria.Specs.ScheduledTest");
        using var activityListener = new ActivityListener
        {
            ShouldListenTo = s => s.Name == "Talaria.Specs.ScheduledTest",
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
        };
        ActivitySource.AddActivityListener(activityListener);
        using var activity = activitySource.StartActivity("schedule-op", ActivityKind.Producer);
        if (activity is not null)
        {
            activity.TraceStateString = "congo=t61rcWkgMzE";
        }

        await bus.ScheduleSendAsync("custom-commands-queue", new ScheduledCommand("cmd-explicit"), TimeSpan.FromSeconds(30));
        await bus.SchedulePublishAsync("custom-events-topic", new ScheduledEvent("evt-explicit"), baseTime.AddSeconds(45));
        await bus.ScheduleSendAsync("custom-commands-due", new ScheduledCommand("cmd-due"), baseTime.AddSeconds(60));
        await bus.SchedulePublishAsync("custom-events-delay", new ScheduledEvent("evt-delay"), TimeSpan.FromSeconds(75));

        var acquired = await deferralStore.AcquireDueAsync(baseTime.AddMinutes(2), TimeSpan.FromSeconds(30), 10);
        Assert.Equal(4, acquired.Count);
        Assert.Equal(
            new[] { "custom-commands-queue", "custom-events-topic", "custom-commands-due", "custom-events-delay" },
            acquired.Select(x => x.Message.Topic).ToArray());

        if (activity is not null)
        {
            Assert.All(acquired, item =>
            {
                Assert.Equal(activity.Id, item.Message.Headers.TraceParent);
                Assert.Equal("congo=t61rcWkgMzE", item.Message.Headers.TraceState);
            });
        }
    }

    [Fact]
    public async Task Given_NegativeDelayOrInvalidArguments_When_SchedulingMessage_Then_ThrowsExpectedArgumentExceptions()
    {
        var services = new ServiceCollection();
        services.AddTalaria().UseInMemory();
        await using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        var bus = scope.ServiceProvider.GetRequiredService<IMessageBus>();

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            bus.ScheduleSendAsync(new ScheduledCommand("1"), TimeSpan.FromMilliseconds(-1)));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            bus.ScheduleSendAsync("dest", new ScheduledCommand("1"), TimeSpan.FromTicks(-1)));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            bus.SchedulePublishAsync(new ScheduledEvent("1"), TimeSpan.FromSeconds(-5)));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            bus.SchedulePublishAsync("dest", new ScheduledEvent("1"), TimeSpan.FromSeconds(-5)));

        await Assert.ThrowsAnyAsync<ArgumentException>(() =>
            bus.ScheduleSendAsync("", new ScheduledCommand("1"), DateTimeOffset.UtcNow));
        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            bus.ScheduleSendAsync<ScheduledCommand>(null!, DateTimeOffset.UtcNow));
    }

    [Fact]
    public async Task Given_NoDeferralStoreAndNoMessageOutbox_When_SchedulingMessage_Then_ThrowsInvalidOperationExceptionWithGuidance()
    {
        var transport = new InMemoryTransport();
        var services = new ServiceCollection();
        services.AddTalaria(opts => opts.ApplicationName = "no-persistence-app")
            .UseTransport(transport);

        await using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        var bus = scope.ServiceProvider.GetRequiredService<IMessageBus>();

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            bus.ScheduleSendAsync(new ScheduledCommand("1"), TimeSpan.FromSeconds(10)));
        Assert.Contains("IDeferralStore", ex.Message);
        Assert.Contains("IMessageOutbox", ex.Message);
    }

    [Fact]
    public async Task Given_MessageOutboxIsRegistered_When_ScheduleSendAndPublishCalled_Then_StagesOutboxMessageWithScheduledAt()
    {
        var transport = new InMemoryTransport();
        var outbox = new RecordingMessageOutbox();
        var baseTime = new DateTimeOffset(2026, 10, 14, 15, 30, 0, TimeSpan.Zero);
        var timeProvider = new FakeTimeProvider(baseTime);

        var services = new ServiceCollection();
        services.AddSingleton<TimeProvider>(timeProvider);
        services.AddScoped<IMessageOutbox>(_ => outbox);
        services.AddTalaria(opts => opts.ApplicationName = "outbox-sched-app")
            .UseInMemoryTransport(transport);

        await using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        var bus = scope.ServiceProvider.GetRequiredService<IMessageBus>();

        var dueAt = baseTime.AddMinutes(15);
        await bus.ScheduleSendAsync("orders-cmd", new ScheduledCommand("tx-1"), dueAt);

        var staged = Assert.Single(outbox.Staged);
        Assert.Equal("orders-cmd", staged.Topic);
        Assert.Equal(baseTime, staged.CreatedAt);
        Assert.Equal(dueAt, staged.ScheduledAt);
        Assert.Equal("command", staged.Headers["talaria.intent"]);
        Assert.Equal(dueAt.ToString("O"), staged.Headers["talaria.scheduled_due_at"]);
    }

    [Fact]
    public async Task Given_ScheduledOutboxMessage_When_OutboxRelayPublishesEntry_Then_OutboxLagIsMeasuredFromScheduledAtInsteadOfCreatedAt()
    {
        var transport = new InMemoryTransport();
        var outboxStore = new InMemoryOutboxStore();
        var stateStore = new InMemoryStateStore<DummySagaState>(outboxStore);

        var now = DateTimeOffset.UtcNow;
        var createdAt = now.AddMinutes(-10); // Created 10 minutes ago
        var scheduledAt = now.AddMilliseconds(-50); // Scheduled to become eligible 50ms ago

        var outboxMessage = new OutboxMessage(
            Guid.NewGuid(),
            "scheduled-lag-topic",
            typeof(ScheduledEvent).FullName!,
            "{\"Id\":\"lag-1\"}",
            new MessageHeaders { MessageId = "lag-msg-1" },
            CreatedAt: createdAt,
            PartitionKey: null,
            ScheduledAt: scheduledAt);

        await stateStore.TransitionAsync("corr-lag", new DummySagaState(), [outboxMessage]);

        var recordedLags = new List<double>();
        using var meterListener = new MeterListener();
        meterListener.InstrumentPublished = (instrument, listener) =>
        {
            if (instrument.Meter.Name == TalariaDiagnostics.MeterName && instrument.Name == "talaria.outbox.lag")
            {
                listener.EnableMeasurementEvents(instrument);
            }
        };
        meterListener.SetMeasurementEventCallback<double>((_, measurement, tags, _) =>
        {
            foreach (var tag in tags)
            {
                if (tag.Key == "messaging.destination.name" && Equals(tag.Value, "scheduled-lag-topic"))
                {
                    recordedLags.Add(measurement);
                }
            }
        });
        meterListener.Start();

        await using var relay = new OutboxRelayEngine(
            outboxStore,
            transport,
            new TalariaOptions { OutboxRelayInterval = TimeSpan.FromMilliseconds(20) },
            NullLogger.Instance);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var relayTask = relay.RunAsync(cts.Token);

        var drained = await TestAsyncHelpers.PollUntilAsync(() => Task.FromResult(outboxStore.Count == 0), TimeSpan.FromSeconds(3));
        Assert.True(drained);
        await cts.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => relayTask);

        var lagMs = Assert.Single(recordedLags);
        // Should be measured relative to ScheduledAt (~50ms ago), NOT CreatedAt (600,000ms ago)
        Assert.True(lagMs < 30_000, $"Expected OutboxLag to be relative to ScheduledAt (< 30,000ms), but was {lagMs}ms.");
    }

    private sealed class DummySagaState
    {
        public string Id { get; set; } = "";
    }

    private sealed class LegacyFourMethodMessageBus : IMessageBus
    {
        public Task SendAsync<T>(T message, CancellationToken ct = default) => Task.CompletedTask;
        public Task PublishAsync<T>(T message, CancellationToken ct = default) => Task.CompletedTask;
        public Task SendAsync<T>(string destination, T message, CancellationToken ct = default) => Task.CompletedTask;
        public Task PublishAsync<T>(string destination, T message, CancellationToken ct = default) => Task.CompletedTask;
    }

    private sealed class MinimalDestinationScheduledMessageBus : IMessageBus
    {
        public List<(string Kind, string Destination, object Message, DateTimeOffset DueAt)> Calls { get; } = [];

        public Task SendAsync<T>(T message, CancellationToken ct = default) => Task.CompletedTask;
        public Task PublishAsync<T>(T message, CancellationToken ct = default) => Task.CompletedTask;
        public Task SendAsync<T>(string destination, T message, CancellationToken ct = default) => Task.CompletedTask;
        public Task PublishAsync<T>(string destination, T message, CancellationToken ct = default) => Task.CompletedTask;

        public Task ScheduleSendAsync<T>(string destination, T message, DateTimeOffset dueAt, CancellationToken ct = default)
        {
            Calls.Add(("send", destination, message!, dueAt));
            return Task.CompletedTask;
        }

        public Task SchedulePublishAsync<T>(string destination, T message, DateTimeOffset dueAt, CancellationToken ct = default)
        {
            Calls.Add(("publish", destination, message!, dueAt));
            return Task.CompletedTask;
        }
    }

    [Fact]
    public async Task Given_LegacyAndMinimalCustomIMessageBusImplementations_When_CallingScheduleMethods_Then_DefaultInterfaceMethodsProvideCompatibilityAndValidation()
    {
        IMessageBus legacyBus = new LegacyFourMethodMessageBus();
        await Assert.ThrowsAsync<ArgumentNullException>(() => legacyBus.ScheduleSendAsync<ScheduledCommand>(null!, TimeSpan.FromSeconds(1)));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => legacyBus.ScheduleSendAsync(new ScheduledCommand("1"), TimeSpan.FromSeconds(-1)));
        await Assert.ThrowsAsync<NotSupportedException>(() => legacyBus.ScheduleSendAsync(new ScheduledCommand("1"), TimeSpan.FromSeconds(5)));
        await Assert.ThrowsAsync<NotSupportedException>(() => legacyBus.SchedulePublishAsync(new ScheduledEvent("1"), TimeSpan.FromSeconds(5)));

        var customImpl = new MinimalDestinationScheduledMessageBus();
        IMessageBus customBus = customImpl;
        await customBus.ScheduleSendAsync(new ScheduledCommand("c1"), TimeSpan.FromSeconds(10));
        await customBus.SchedulePublishAsync(new ScheduledEvent("e1"), TimeSpan.FromSeconds(20));

        Assert.Equal(2, customImpl.Calls.Count);
        Assert.Equal("send", customImpl.Calls[0].Kind);
        Assert.Equal(MessageNames.Destination(MessageNames.Contract(typeof(ScheduledCommand))), customImpl.Calls[0].Destination);
        Assert.Equal("publish", customImpl.Calls[1].Kind);
        Assert.Equal(MessageNames.Destination(MessageNames.Contract(typeof(ScheduledEvent))), customImpl.Calls[1].Destination);
    }
}

