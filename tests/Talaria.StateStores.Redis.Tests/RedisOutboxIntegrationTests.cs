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
            now,
            PartitionKey: "order-partition-7");

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
        Assert.Equal("order-partition-7", acquired.Message.PartitionKey);

        // The lease hides it from a concurrent relay.
        Assert.Empty(await outbox.AcquirePendingAsync(now.AddSeconds(5), lease, 10));

        // Completion is fenced: a stale token fails, the current lease succeeds.
        Assert.False(await outbox.CompleteAsync(acquired.Lease with { Token = acquired.Lease.Token + 99 }));
        Assert.True(await outbox.CompleteAsync(acquired.Lease));
        Assert.Empty(await outbox.AcquirePendingAsync(now.Add(lease).AddSeconds(1), lease, 10));

        // Re-enqueue the same stable outbox id after completion. The store-wide
        // monotonic token prevents the earlier lease from matching the new entry.
        await stateStore.TransitionAsync("corr-1", new SagaState { Id = "corr-1", Step = 3 }, [entry]);
        var reused = Assert.Single(await outbox.AcquirePendingAsync(DateTimeOffset.UtcNow.AddSeconds(1), lease, 10));
        Assert.True(reused.Lease.Token > acquired.Lease.Token);
        Assert.False(await outbox.CompleteAsync(acquired.Lease));
        Assert.True(await outbox.CompleteAsync(reused.Lease));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => outbox.AcquirePendingAsync(now, TimeSpan.Zero, 10));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => outbox.AcquirePendingAsync(now, lease, 0));

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
            new MessageHeaders { MessageId = "minted-2" }, now,
            PartitionKey: null)]);

        // Staging stamps visibility with the store-side clock — acquire with fresh time.
        now = DateTimeOffset.UtcNow;

        var first = Assert.Single(await outbox.AcquirePendingAsync(now, lease, 10));
        Assert.False(first.IsReacquired);

        // Crash simulation: never completed → after lease expiry another relay acquires
        // the same entry with a bumped fencing token.
        var reacquired = Assert.Single(await outbox.AcquirePendingAsync(now.Add(lease).AddSeconds(1), lease, 10));
        Assert.True(reacquired.IsReacquired);
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

    [DockerFact]
    public async Task VersionedTransition_IsAtomicAndIdempotent_AndUsesHashedClusterSlot()
    {
        var stateStore = _serviceProvider.GetRequiredService<IStateStore<SagaState>>();
        var outbox = _serviceProvider.GetRequiredService<IOutboxStore>();
        const string correlationId = "receipt-saga";
        const string messageId = "receipt-message";
        var initial = await stateStore.ReadSnapshotAsync(correlationId, messageId);
        var outbound = new OutboxMessage(Guid.NewGuid(), "receipt-topic", "System.String", "\"once\"",
            new MessageHeaders { MessageId = "receipt-outbound" }, DateTimeOffset.UtcNow, null);

        Assert.Equal(SagaCommitStatus.Committed, await stateStore.TryTransitionAsync(correlationId, messageId,
            initial.Version, new SagaState { Id = correlationId, Step = 1 }, [outbound]));
        Assert.Equal(SagaCommitStatus.AlreadyProcessed, await stateStore.TryTransitionAsync(correlationId, messageId,
            initial.Version, new SagaState { Id = correlationId, Step = 99 }, [outbound with { Id = Guid.NewGuid() }]));
        Assert.Equal(SagaCommitStatus.Conflict, await stateStore.TryTransitionAsync(correlationId, "different-message",
            initial.Version, new SagaState { Id = correlationId, Step = 3 }, []));

        var after = await stateStore.ReadSnapshotAsync(correlationId, messageId);
        Assert.True(after.MessageProcessed);
        Assert.Equal(1, after.State!.Step);
        Assert.Single(await outbox.AcquirePendingAsync(DateTimeOffset.UtcNow.AddSeconds(1), TimeSpan.FromSeconds(10), 10));
    }
}
