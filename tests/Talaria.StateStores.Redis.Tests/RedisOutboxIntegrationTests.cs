using Microsoft.Extensions.DependencyInjection;
using Talaria.Core.Abstractions;
using Talaria.Core.Registration;
using Testcontainers.Redis;
using Xunit;

namespace Talaria.StateStores.Redis.Tests;

/// <summary>
/// Integration coverage of the Redis transactional outbox: atomic staging via
/// IStateStore.TransitionAsync plus the lease/fencing semantics of the relay side.
/// Requires Docker — skipped automatically when unavailable.
/// </summary>
public class RedisOutboxIntegrationTests : IAsyncLifetime
{
    private class SagaState { public string Id { get; set; } = ""; public int Step { get; set; } }

    private RedisContainer? _redisContainer;
    private IServiceProvider _serviceProvider = null!;

    public async Task InitializeAsync()
    {
        if (!DockerFactAttribute.IsDockerRunning()) return;

        _redisContainer = new RedisBuilder("redis:7.2").Build();
        await _redisContainer.StartAsync();

        var services = new ServiceCollection();
        var builder = services.AddTalaria(opts =>
        {
            opts.ApplicationName = $"test-app-{Guid.NewGuid():N}";
        });

        builder.UseRedisStateStore(opts =>
        {
            opts.Configuration = _redisContainer!.GetConnectionString();
            opts.KeyPrefix = $"test-outbox-{Guid.NewGuid():N}:";
        });

        _serviceProvider = services.BuildServiceProvider();
    }

    public async Task DisposeAsync()
    {
        if (_redisContainer != null)
        {
            await _redisContainer.DisposeAsync();
        }
    }

    [DockerFact]
    public async Task Transition_Stages_State_And_Outbox_Atomically_Then_Relay_Drains()
    {
        var stateStore = _serviceProvider.GetRequiredService<IStateStore<SagaState>>();
        var outbox = _serviceProvider.GetRequiredService<IOutboxStore>();
        var lease = TimeSpan.FromSeconds(30);
        var now = DateTimeOffset.UtcNow;

        var entry = new OutboxMessage(
            Guid.NewGuid(),
            "orders-billed",
            "System.String",
            "\"billed\"",
            new MessageHeaders { MessageId = "minted-1" },
            now);

        // One atomic unit: state saved + entry staged.
        await stateStore.TransitionAsync("corr-1", new SagaState { Id = "corr-1", Step = 2 }, [entry]);

        // Staging stamps visibility with the store-side clock — acquire with fresh time.
        now = DateTimeOffset.UtcNow;

        var state = await stateStore.GetAsync("corr-1");
        Assert.NotNull(state);
        Assert.Equal(2, state!.Step);

        // The relay acquires the staged entry with all fields intact.
        var pending = await outbox.AcquirePendingAsync(now, lease, 10);
        var acquired = Assert.Single(pending);
        Assert.Equal(entry.Id, acquired.Message.Id);
        Assert.Equal("orders-billed", acquired.Message.Topic);
        Assert.Equal("\"billed\"", acquired.Message.PayloadJson);
        Assert.Equal("minted-1", acquired.Message.Headers.MessageId);

        // The lease hides it from a concurrent relay.
        Assert.Empty(await outbox.AcquirePendingAsync(now.AddSeconds(5), lease, 10));

        // Completion is fenced: a stale token fails, the current lease succeeds.
        Assert.False(await outbox.CompleteAsync(acquired.Lease with { Token = acquired.Lease.Token + 99 }));
        Assert.True(await outbox.CompleteAsync(acquired.Lease));
        Assert.Empty(await outbox.AcquirePendingAsync(now.Add(lease).AddSeconds(1), lease, 10));

        // Completion transition: state purged atomically with staging the final dispatch.
        await stateStore.TransitionAsync("corr-1", null, [entry with { Id = Guid.NewGuid() }]);
        Assert.Null(await stateStore.GetAsync("corr-1"));
        Assert.Single(await outbox.AcquirePendingAsync(DateTimeOffset.UtcNow, lease, 10));
    }

    [DockerFact]
    public async Task Lease_Expiry_Reacquires_With_Bumped_Token_And_Abandon_Reschedules()
    {
        var stateStore = _serviceProvider.GetRequiredService<IStateStore<SagaState>>();
        var outbox = _serviceProvider.GetRequiredService<IOutboxStore>();
        var lease = TimeSpan.FromSeconds(30);
        var now = DateTimeOffset.UtcNow;

        await stateStore.TransitionAsync("corr-2", new SagaState { Id = "corr-2" }, [new OutboxMessage(
            Guid.NewGuid(), "orders-retry", "System.String", "\"retry\"",
            new MessageHeaders { MessageId = "minted-2" }, now)]);

        // Staging stamps visibility with the store-side clock — acquire with fresh time.
        now = DateTimeOffset.UtcNow;

        var first = Assert.Single(await outbox.AcquirePendingAsync(now, lease, 10));

        // Crash simulation: never completed → after lease expiry another relay acquires
        // the same entry with a bumped fencing token.
        var reacquired = Assert.Single(await outbox.AcquirePendingAsync(now.Add(lease).AddSeconds(1), lease, 10));
        Assert.Equal(first.Message.Id, reacquired.Message.Id);
        Assert.True(reacquired.Lease.Token > first.Lease.Token);

        // The stale holder can neither complete nor abandon.
        Assert.False(await outbox.CompleteAsync(first.Lease));
        Assert.False(await outbox.AbandonAsync(first.Lease));

        // The current holder abandons into the future: hidden until then.
        var retryAt = now.AddHours(1);
        Assert.True(await outbox.AbandonAsync(reacquired.Lease, retryAt));
        Assert.Empty(await outbox.AcquirePendingAsync(now.AddMinutes(30), lease, 10));
        Assert.Single(await outbox.AcquirePendingAsync(retryAt, lease, 10));
    }
}
