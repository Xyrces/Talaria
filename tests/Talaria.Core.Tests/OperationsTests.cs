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
}
