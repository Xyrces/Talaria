using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Talaria.Core.Abstractions;
using Talaria.Core.Hosting;
using Talaria.Core.Registration;
using Talaria.Transports.InMemory;

namespace Talaria.Core.Tests;

public class OperationsTests
{
    public sealed record Message(string Id);
    public sealed class State { }

    [Fact]
    public void SagaEndpointIdentityFitsAzureServiceBusSubscriptionLimit()
    {
        var identity = SagaConsumerEngine.EndpointName(new string('a', 40), typeof(State), new string('b', 80));
        Assert.True(identity.Length <= 50, $"Saga subscription name was {identity.Length} characters.");
    }

    private static IHost Host()
    {
        var builder = Microsoft.Extensions.Hosting.Host.CreateApplicationBuilder();
        builder.Logging.ClearProviders();
        builder.Services.AddTalaria(t => t.UseInMemory().Configure(o => o.ShutdownDrainTimeout = TimeSpan.FromSeconds(2)));
        return builder.Build();
    }

    [Fact]
    public async Task ShutdownDrainsActiveHandler_AndDoesNotStartBufferedWork()
    {
        using var host = Host();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var finish = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var calls = 0;
        CancellationToken handlerToken = default;
        host.MapCommand<Message>(async (Message _, CancellationToken ct) =>
        {
            handlerToken = ct;
            Interlocked.Increment(ref calls);
            entered.TrySetResult();
            await finish.Task.WaitAsync(ct);
        });
        await host.StartAsync();
        var listener = host.Services.GetRequiredService<TalariaListener>();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await listener.Health.WaitUntilReadyAsync(timeout.Token);
        using var scope = host.Services.CreateScope();
        var bus = scope.ServiceProvider.GetRequiredService<IMessageBus>();
        await bus.SendAsync(new Message("first"));
        await entered.Task.WaitAsync(timeout.Token);
        await bus.SendAsync(new Message("second"));
        var stop = host.StopAsync(timeout.Token);
        Assert.Equal(ListenerState.Draining, listener.Health.State);
        Assert.False(listener.Health.IsReady);
        Assert.False(stop.IsCompleted);
        Assert.False(handlerToken.IsCancellationRequested);
        finish.TrySetResult();
        await stop;
        Assert.Equal(1, calls);
        Assert.Equal(ListenerState.Stopped, listener.Health.State);
    }

    [Fact]
    public async Task FailureReplayTargetsOnlyTheFailedSubscriber()
    {
        using var host = Host();
        var audit = 0;
        var email = 0;
        var completed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        host.MapEvent<Message>((Message _) => { Interlocked.Increment(ref audit); }).WithName("audit");
        host.MapEvent<Message>((Message _) =>
        {
            if (Interlocked.Increment(ref email) == 1) throw new InvalidOperationException("failure");
            completed.TrySetResult();
        }).WithName("email");
        await host.StartAsync();
        using var scope = host.Services.CreateScope();
        await scope.ServiceProvider.GetRequiredService<IMessageBus>().PublishAsync(new Message("one"));
        var failures = scope.ServiceProvider.GetRequiredService<IFailedMessages>();
        var failed = await WaitForFailure(failures);
        Assert.Equal("handler_failed", failed.Reason);
        await failures.ReplayAsync(failed.Id);
        await completed.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await host.StopAsync();
        Assert.Equal(1, audit);
        Assert.Equal(2, email);
        Assert.Empty(await failures.ListAsync());
    }

    [Fact]
    public async Task RelayRetainsInvalidJson_AndCorrectedReplayDoesNotNeedOldClrType()
    {
        using var host = Host();
        var received = new TaskCompletionSource<Message>(TaskCreationOptions.RunContinuationsAsynchronously);
        host.Services.GetRequiredService<TopicRegistry>().MapTopic<Message>("legacy", (Message message, CancellationToken _) =>
        {
            received.TrySetResult(message);
            return Task.CompletedTask;
        });
        var outbox = host.Services.GetRequiredService<IStateStore<State>>();
        var id = Guid.NewGuid();
        await outbox.TransitionAsync("saga", new State(), [new OutboxMessage(id, "legacy", "Old.Contracts.Message, Missing.Assembly",
            "{broken", new MessageHeaders { MessageId = "old" }, DateTimeOffset.UtcNow, null)]);
        await host.StartAsync();
        using var scope = host.Services.CreateScope();
        var failures = scope.ServiceProvider.GetRequiredService<IFailedMessages>();
        var failed = await WaitForFailure(failures);
        Assert.Equal(id, failed.Id);
        Assert.Equal("{broken", failed.PayloadJson);
        await Assert.ThrowsAsync<System.Text.Json.JsonException>(() => failures.ReplayAsync(id));
        Assert.Single(await failures.ListAsync());
        await failures.ReplayAsync(id, "{\"Id\":\"recovered\"}");
        Assert.Equal("recovered", (await received.Task.WaitAsync(TimeSpan.FromSeconds(5))).Id);
        await host.StopAsync();
    }

    private static async Task<FailedMessage> WaitForFailure(IFailedMessages failures)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        while (true)
        {
            var items = await failures.ListAsync(ct: timeout.Token);
            if (items.Count > 0) return Assert.Single(items);
            await Task.Delay(10, timeout.Token);
        }
    }

    [Fact]
    public async Task Given_FailedMessageWithDeferralAttemptsAndGenericType_When_ReplayAsyncIsCalled_Then_DeferralAttemptIsResetAndTypeIsPreserved()
    {
        using var host = Host();
        MessageHeaders? capturedHeaders = null;
        var received = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        host.Services.GetRequiredService<TopicRegistry>().MapTopicWithEnvelope<Message>("replay-topic", (env, _) =>
        {
            capturedHeaders = env.Headers;
            received.TrySetResult();
            return Task.CompletedTask;
        });

        var store = host.Services.GetRequiredService<IFailedMessageStore>();
        var failureId = Guid.NewGuid();
        var genericType = "MyNamespace.Wrapper`1[[System.String, System.Private.CoreLib]], MyAssembly";
        await store.SaveAsync(new FailedMessage(
            failureId,
            "replay-topic",
            genericType,
            "{\"Id\":\"replayed\"}",
            new MessageHeaders
            {
                MessageId = "orig",
                [MessageHeaders.DeferralAttemptKey] = "5",
                DlqReason = "max_deferrals_exceeded",
            },
            null,
            "max_deferrals_exceeded",
            DateTimeOffset.UtcNow));

        await host.StartAsync();
        using var scope = host.Services.CreateScope();
        await scope.ServiceProvider.GetRequiredService<IFailedMessages>().ReplayAsync(failureId);
        await received.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await host.StopAsync();

        Assert.NotNull(capturedHeaders);
        Assert.False(capturedHeaders!.ContainsKey(MessageHeaders.DeferralAttemptKey));
        Assert.Equal("MyNamespace.Wrapper`1[[System.String, System.Private.CoreLib]]", capturedHeaders[MessageHeaders.MessageTypeKey]);
    }

    [Fact]
    public async Task Given_OutboxAndDeferralWithCorruptJsonAndNoFailedMessageStore_When_RelayAndSweeperRun_Then_CorruptEntriesAreQuarantinedWithoutDeletionOrBlockingValidMessages()
    {
        var transport = new InMemoryTransport();
        var outbox = new InMemoryOutboxStore();
        var deferral = new InMemoryDeferralStore();
        var stateStore = new InMemoryStateStore<State>(outbox);
        var now = DateTimeOffset.UtcNow;

        await stateStore.TransitionAsync("corr-1", new State(), [
            new OutboxMessage(Guid.NewGuid(), "orders", typeof(string).FullName!, "{corrupt-json", new MessageHeaders(), now.AddSeconds(-2)),
            new OutboxMessage(Guid.NewGuid(), "orders", typeof(string).FullName!, "\"valid-outbox\"", new MessageHeaders(), now.AddSeconds(-1))]);

        await deferral.EnqueueAsync(new DeferredMessage(
            Guid.NewGuid(), "deferred-orders", typeof(string).FullName!, "{corrupt-json", new MessageHeaders(), "corr-1", 1, now.AddSeconds(-2)));
        await deferral.EnqueueAsync(new DeferredMessage(
            Guid.NewGuid(), "deferred-orders", typeof(string).FullName!, "\"valid-deferral\"", new MessageHeaders(), "corr-1", 1, now.AddSeconds(-1)));

        await using var relay = new OutboxRelayEngine(
            outbox,
            transport,
            new TalariaOptions { OutboxRelayInterval = TimeSpan.FromMilliseconds(20) },
            Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance,
            failures: null);

        await using var sweeper = new DeferralSweeperEngine(
            deferral,
            transport,
            new TalariaOptions { DeferralBackoff = TimeSpan.FromMilliseconds(20) },
            Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance,
            failures: null);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var relayTask = relay.RunAsync(cts.Token);
        var sweeperTask = sweeper.RunAsync(cts.Token);

        while ((outbox.Count > 1 || deferral.Count > 1) && !cts.IsCancellationRequested)
        {
            await Task.Delay(15);
        }
        await cts.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => relayTask);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => sweeperTask);

        // Corrupt entries are retained (quarantined) in the stores, not deleted, and hidden from future sweeps.
        Assert.Equal(1, outbox.Count);
        Assert.Equal(1, deferral.Count);
        Assert.Empty(await outbox.AcquirePendingAsync(DateTimeOffset.UtcNow.AddDays(365), TimeSpan.FromSeconds(1), 10));
        Assert.Empty(await deferral.AcquireDueAsync(DateTimeOffset.UtcNow.AddDays(365), TimeSpan.FromSeconds(1), 10));
        Assert.Equal("valid-outbox", Assert.Single(await transport.ReadAllFromTopicAsync<string>("orders")).Payload);
        Assert.Equal("valid-deferral", Assert.Single(await transport.ReadAllFromTopicAsync<string>("deferred-orders")).Payload);
    }

    public sealed record SagaStart(string Id);
    public sealed record SagaStep(string Id);
    public sealed class HopSagaState { public string Id { get; set; } = ""; }

    [Fact]
    public async Task Given_SagaWithMaxHopCount_When_DeferredAndRepublished_Then_HopCountIncrementsByOneAndExceededHopCountRoutesToDlq()
    {
        var transport = new InMemoryTransport();
        var deferralStore = new InMemoryDeferralStore();
        var registry = new Talaria.Core.Sagas.SagaRegistry();
        var starterRuns = 0;

        registry.MapSaga<HopSagaState>(config =>
        {
            config.StartedBy<SagaStart>("s-hop-start",
                (msg, ctx) =>
                {
                    Interlocked.Increment(ref starterRuns);
                    return Task.FromResult(ctx.Transition(new HopSagaState { Id = msg.Id }));
                },
                m => m.Id);
            config.On<SagaStep>("s-hop-step",
                (state, msg, ctx) => Task.FromResult(ctx.Transition(state)),
                m => m.Id);
        });

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
                ApplicationName = "test-hop-app",
                MaxHopCount = 2,
                DeferralBackoff = TimeSpan.FromMilliseconds(30),
                MaxDeferralAttempts = 5,
            },
            Microsoft.Extensions.Logging.Abstractions.NullLogger<TalariaListener>.Instance,
            services);

        await listener.StartAsync();

        var startProducer = await transport.CreateProducerAsync<SagaStart>("s-hop-start", new ProducerOptions());
        await startProducer.ProduceAsync(new SagaStart("exceeded"), new MessageHeaders { HopCount = 1 });
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        List<MessageEnvelope<SagaStart>> dlq;
        while ((dlq = await transport.ReadAllFromTopicAsync<SagaStart>("s-hop-start.dlq")).Count == 0)
        {
            await Task.Delay(20, timeout.Token);
        }
        Assert.Single(dlq);
        Assert.Equal("max_hops_exceeded", dlq[0].Headers.DlqReason);
        Assert.Equal(0, Volatile.Read(ref starterRuns));

        var stepProducer = await transport.CreateProducerAsync<SagaStep>("s-hop-step", new ProducerOptions());
        await stepProducer.ProduceAsync(new SagaStep("c-hop"));
        while (deferralStore.Count == 0)
        {
            await Task.Delay(20, timeout.Token);
        }
        await startProducer.ProduceAsync(new SagaStart("c-hop"));
        var stepDeliveries = new List<MessageEnvelope<SagaStep>>();
        while (stepDeliveries.Count < 2)
        {
            stepDeliveries.AddRange(await transport.ReadAllFromTopicAsync<SagaStep>("s-hop-step"));
            if (stepDeliveries.Count < 2) await Task.Delay(20, timeout.Token);
        }
        var republished = Assert.Single(stepDeliveries, m => m.Headers.ContainsKey(MessageHeaders.DeferralAttemptKey));
        Assert.Equal(1, republished.Headers.HopCount);

        await listener.StopAsync();
    }
}
