using Microsoft.Extensions.DependencyInjection;
using Talaria.Core.Abstractions;
using Talaria.Core.Registration;
using Microsoft.Extensions.Options;
using StackExchange.Redis;
using Testcontainers.Redis;

namespace Talaria.StateStores.Redis.Tests;

public sealed class RedisFailedMessageIntegrationTests : IAsyncLifetime
{
    private sealed class SagaState { public int Step { get; set; } }
    private RedisContainer? _container;
    private IServiceProvider _services = null!;

    public async Task InitializeAsync()
    {
        if (!DockerFactAttribute.IsDockerRunning())
        {
            if (Environment.GetEnvironmentVariable("TALARIA_REQUIRE_DOCKER") == "1")
                throw new InvalidOperationException("TALARIA_REQUIRE_DOCKER=1 but Docker is unavailable.");
            return;
        }
        _container = new RedisBuilder("redis:7.2").Build();
        await _container.StartAsync();
        var collection = new ServiceCollection();
        var builder = collection.AddTalaria(options => options.ApplicationName = $"failed-{Guid.NewGuid():N}");
        builder.UseRedisPersistence(options =>
        {
            options.Configuration = _container.GetConnectionString();
            options.KeyPrefix = $"failed-tests-{Guid.NewGuid():N}:";
        });
        _services = collection.BuildServiceProvider();
    }

    public async Task DisposeAsync()
    {
        if (_container is not null) await _container.DisposeAsync();
        if (_services is IDisposable disposable) disposable.Dispose();
    }

    [DockerFact]
    public async Task FailedMessages_RoundTripListBoundAndDelete()
    {
        var store = _services.GetRequiredService<IFailedMessageStore>();
        var first = new FailedMessage(Guid.NewGuid(), "orders", "Order", "{\"id\":1}",
            new MessageHeaders { MessageId = "failed-1" }, "order-1", "handler_failed", DateTimeOffset.UtcNow.AddMinutes(-1));
        var second = new FailedMessage(Guid.NewGuid(), "orders", "Order", "{\"id\":2}",
            new MessageHeaders { MessageId = "failed-2" }, null, "handler_failed", DateTimeOffset.UtcNow);

        await store.SaveAsync(first);
        await store.SaveAsync(second);
        var loaded = await store.GetAsync(first.Id);
        Assert.NotNull(loaded);
        Assert.Equal(first.Id, loaded.Id);
        Assert.Equal(first.Topic, loaded.Topic);
        Assert.Equal(first.MessageType, loaded.MessageType);
        Assert.Equal(first.PayloadJson, loaded.PayloadJson);
        Assert.Equal(first.Headers.MessageId, loaded.Headers.MessageId);
        Assert.Equal(first.PartitionKey, loaded.PartitionKey);
        Assert.Equal(first.Reason, loaded.Reason);
        Assert.Equal(first.FailedAt, loaded.FailedAt);
        Assert.Equal(second.Id, Assert.Single(await store.ListAsync(1)).Id);
        Assert.Equal(2, (await store.ListAsync()).Count);
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => store.ListAsync(1001));
        Assert.True(await store.DeleteAsync(first.Id));
        Assert.False(await store.DeleteAsync(first.Id));
        Assert.Null(await store.GetAsync(first.Id));
        Assert.Single(await store.ListAsync());

        // Exercise every Redis persistence namespace, then verify their keys
        // carry one shared Redis Cluster hash tag for atomic Lua operations.
        var saga = _services.GetRequiredService<IStateStore<SagaState>>();
        var now = DateTimeOffset.UtcNow;
        await saga.TransitionAsync("slot-check", new SagaState { Step = 1 }, [new OutboxMessage(
            Guid.NewGuid(), "events", "string", "\"event\"", new MessageHeaders(), now, null)]);
        var deferral = _services.GetRequiredService<IDeferralStore>();
        await deferral.EnqueueAsync(new DeferredMessage(Guid.NewGuid(), "events", "string", "\"later\"",
            new MessageHeaders(), null, 0, now.AddHours(1), null));
        var idempotency = _services.GetRequiredService<IIdempotencyStore>();
        var lockHandle = await idempotency.TryAcquireLockAsync("slot-check", "consumer", TimeSpan.FromMinutes(1));
        Assert.NotNull(lockHandle);
        var mux = _services.GetRequiredService<IConnectionMultiplexer>();
        var server = mux.GetServer(mux.GetEndPoints().First());
        var prefix = _services.GetRequiredService<IOptions<TalariaRedisOptions>>().Value.KeyPrefix;
        var keys = server.Keys(pattern: $"{prefix}v2:*").Select(key => key.ToString()).ToArray();
        Assert.Contains(keys, key => key.Contains(":idemp:", StringComparison.Ordinal));
        Assert.Contains(keys, key => key.Contains(":defer", StringComparison.Ordinal));
        Assert.Contains(keys, key => key.Contains(":outbox", StringComparison.Ordinal));
        Assert.Contains(keys, key => key.Contains(":failed", StringComparison.Ordinal));
        var tags = keys.Select(ExtractHashTag).Distinct(StringComparer.Ordinal).ToArray();
        Assert.Single(tags);
        await idempotency.ReleaseLockAsync(lockHandle!);
    }

    private static string ExtractHashTag(string key)
    {
        var open = key.IndexOf('{');
        var close = key.IndexOf('}', open + 1);
        Assert.True(open >= 0 && close > open, $"Redis key has no hash tag: {key}");
        return key[(open + 1)..close];
    }
}
