using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Talaria.Core.Abstractions;
using Talaria.Core.Hosting;
using Talaria.Core.Registration;
using Talaria.Transports.InMemory;

namespace Talaria.Core.Tests;

public class MinimalMessagingTests
{
    public sealed record Order(string Id);
    public sealed class ScopedDependency : IAsyncDisposable
    {
        public Guid Id { get; } = Guid.NewGuid();
        public bool Disposed { get; private set; }
        public ValueTask DisposeAsync() { Disposed = true; return ValueTask.CompletedTask; }
    }

    private static IHost CreateHost()
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Logging.ClearProviders();
        builder.Services.AddScoped<ScopedDependency>();
        builder.Services.AddTalaria(t => t.UseInMemory());
        return builder.Build();
    }

    [Fact]
    public async Task CommandsBindScopedServicesAndContext_AndDisposeEachScope()
    {
        using var host = CreateHost();
        var received = System.Threading.Channels.Channel.CreateUnbounded<ScopedDependency>();
        host.MapCommand<Order>((Order message, ScopedDependency service, ConsumeContext<Order> context, CancellationToken ct) =>
        {
            Assert.Equal(message, context.Message);
            Assert.Same(service, context.Services.GetRequiredService<ScopedDependency>());
            received.Writer.TryWrite(service);
            return ValueTask.CompletedTask;
        });
        await host.StartAsync();
        using var scope = host.Services.CreateScope();
        var bus = scope.ServiceProvider.GetRequiredService<IMessageBus>();
        await bus.SendAsync(new Order("1"));
        await bus.SendAsync(new Order("2"));
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var first = await received.Reader.ReadAsync(timeout.Token);
        var second = await received.Reader.ReadAsync(timeout.Token);
        await host.StopAsync(timeout.Token);
        Assert.NotEqual(first.Id, second.Id);
        Assert.True(first.Disposed);
        Assert.True(second.Disposed);
    }

    [Fact]
    public async Task RetryTargetsOnlyFailingEventSubscriber()
    {
        using var host = CreateHost();
        var firstCalls = 0;
        var secondCalls = 0;
        var finished = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        host.MapEvent<Order>((Order message) => { Interlocked.Increment(ref firstCalls); }).WithName("audit");
        host.MapEvent<Order>((Order message) =>
        {
            if (Interlocked.Increment(ref secondCalls) == 1) throw new InvalidOperationException("retry me");
            finished.TrySetResult();
        }).WithName("email").WithRetryPolicy(new RetryPolicy
        {
            MaxRetryAttempts = 1, RetryInterval = TimeSpan.FromMilliseconds(100),
        });
        await host.StartAsync();
        using var scope = host.Services.CreateScope();
        await scope.ServiceProvider.GetRequiredService<IMessageBus>().PublishAsync(new Order("1"));
        await finished.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await host.StopAsync();
        Assert.Equal(1, firstCalls);
        Assert.Equal(2, secondCalls);
    }

    [Fact]
    public void UnregisteredServiceFailsDuringMapping()
    {
        using var host = CreateHost();
        Assert.Throws<ArgumentException>(() => host.MapCommand<Order>((Order message, IDisposable missing) => { }));
    }

    [Fact]
    public void AsyncVoidHandlersAreRejected_AndNamedSubscriptionsFitBrokerLimits()
    {
        using var host = CreateHost();
        Action<Order> unsafeHandler = async _ => await Task.Yield();
        Assert.Throws<ArgumentException>(() => host.MapCommand<Order>(unsafeHandler));
        host.MapEvent<Order>((Order _) => { }).WithName(new string('x', 200));
        var registration = Assert.Single(host.Services.GetRequiredService<TopicRegistry>().Registrations);
        Assert.True(registration.ConsumerGroup!.Length <= 50);
    }

    [Fact]
    public async Task CommandsCannotCreateIndependentSubscriptions_OrShareDestinationsWithEvents()
    {
        using var host = CreateHost();
        var endpoint = host.MapCommand<Order>((Order message) => { });
        Assert.Throws<InvalidOperationException>(() => endpoint.WithName("independent"));
        host.MapEvent<Order>((Order message) => { });
        await Assert.ThrowsAsync<InvalidOperationException>(() => host.StartAsync());
    }

    [Fact]
    public async Task EndpointOptionsAreSealedAtStart_AndTopologyIsExportable()
    {
        using var host = CreateHost();
        var endpoint = host.MapCommand<Order>((Order message) => { });
        Assert.Equal(TopologyEntityKind.Queue, Assert.Single(host.Services.GetRequiredService<TopicRegistry>().GetTopology()).Kind);
        await host.StartAsync();
        Assert.Throws<InvalidOperationException>(() => endpoint.WithName("late"));
        await host.StopAsync();
    }
}
