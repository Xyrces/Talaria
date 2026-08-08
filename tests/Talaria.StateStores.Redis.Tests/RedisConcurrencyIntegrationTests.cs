using Microsoft.Extensions.DependencyInjection;
using Talaria.Core.Abstractions;
using Talaria.Core.Registration;
using Testcontainers.Redis;
using Xunit;

namespace Talaria.StateStores.Redis.Tests;

public class RedisConcurrencyIntegrationTests : IAsyncLifetime
{
    private RedisContainer? _redisContainer;
    private IServiceProvider _serviceProvider = null!;

    public async Task InitializeAsync()
    {
        if (!DockerFactAttribute.IsDockerRunning()) return;

        _redisContainer = new RedisBuilder("redis:7.2")
            .Build();

        await _redisContainer.StartAsync();

        var services = new ServiceCollection();
        var builder = services.AddTalaria(opts =>
        {
            opts.ApplicationName = $"test-app-{Guid.NewGuid():N}";
        });

        var connectionString = _redisContainer!.GetConnectionString();
        var keyPrefix = $"test-concurrency-{Guid.NewGuid():N}:";

        builder.UseRedisIdempotencyStore(opts =>
        {
            opts.Configuration = connectionString;
            opts.KeyPrefix = keyPrefix;
        });
        builder.UseRedisDeferralStore(opts =>
        {
            opts.Configuration = connectionString;
            opts.KeyPrefix = keyPrefix;
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
    public async Task ConcurrentTryAcquireLock_SameKey_ExactlyOneWinner()
    {
        var store = _serviceProvider.GetRequiredService<IIdempotencyStore>();

        var messageId = Guid.NewGuid().ToString("N");
        const string consumerQueue = "concurrency-queue";

        var attempts = await Task.WhenAll(
            Enumerable.Range(0, 100)
                .Select(_ => store.TryAcquireLockAsync(messageId, consumerQueue, TimeSpan.FromMinutes(5))));

        var winner = Assert.Single(attempts.Where(a => a is not null));
        Assert.Equal(messageId, winner!.MessageId);
        Assert.Equal(consumerQueue, winner.ConsumerQueue);
        Assert.False(string.IsNullOrEmpty(winner.Token));
    }

    [DockerFact]
    public async Task ReleaseLock_WithStaleToken_KeepsLock_RealTokenReleases()
    {
        var store = _serviceProvider.GetRequiredService<IIdempotencyStore>();

        var messageId = Guid.NewGuid().ToString("N");
        const string consumerQueue = "fencing-queue";
        var expiration = TimeSpan.FromMinutes(5);

        var realLock = await store.TryAcquireLockAsync(messageId, consumerQueue, expiration);
        Assert.NotNull(realLock);

        // A forged lock with a stale fencing token must not release the real lock.
        var forgedLock = new IdempotencyLock(messageId, consumerQueue, "stale-token");
        await store.ReleaseLockAsync(forgedLock);

        Assert.Null(await store.TryAcquireLockAsync(messageId, consumerQueue, expiration));

        // The real owner's token releases it, and the lock can be re-acquired.
        await store.ReleaseLockAsync(realLock!);

        var reacquired = await store.TryAcquireLockAsync(messageId, consumerQueue, expiration);
        Assert.NotNull(reacquired);
        Assert.NotEqual(realLock!.Token, reacquired!.Token);
    }

    [DockerFact]
    public async Task DeferralStore_Roundtrip_PopsOnceAndRequeuesUntilDue()
    {
        var store = _serviceProvider.GetRequiredService<IDeferralStore>();

        var now = DateTimeOffset.UtcNow;

        var headers = new MessageHeaders
        {
            MessageId = "defer-1",
            TraceParent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
        };
        headers[MessageHeaders.CorrelationIdKey] = "corr-123";

        var message = new DeferredMessage(
            Guid.NewGuid(),
            "orders-topic",
            "System.String",
            "\"hello-deferred\"",
            headers,
            "corr-123",
            2,
            now.AddSeconds(-5));

        await store.EnqueueAsync(message);

        // Due in the past: popped once, with all fields surviving the roundtrip.
        var due = await store.PopDueAsync(now, 10);
        var popped = Assert.Single(due);
        Assert.Equal(message.Id, popped.Id);
        Assert.Equal("orders-topic", popped.Topic);
        Assert.Equal("System.String", popped.MessageType);
        Assert.Equal("\"hello-deferred\"", popped.PayloadJson);
        Assert.Equal("corr-123", popped.CorrelationId);
        Assert.Equal(2, popped.Attempt);
        Assert.Equal("defer-1", popped.Headers.MessageId);
        Assert.Equal("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", popped.Headers.TraceParent);
        Assert.Equal("corr-123", popped.Headers[MessageHeaders.CorrelationIdKey]);

        // Popping is an atomic claim — a second pop sees nothing.
        Assert.Empty(await store.PopDueAsync(now, 10));

        // Requeued into the future: not popped before it is due.
        var futureDue = now.AddHours(1);
        await store.RequeueAsync(popped, futureDue);
        Assert.Empty(await store.PopDueAsync(now.AddMinutes(30), 10));

        // Once due again, it pops with its fields intact.
        var dueAgain = await store.PopDueAsync(futureDue.AddMinutes(1), 10);
        var repopped = Assert.Single(dueAgain);
        Assert.Equal(message.Id, repopped.Id);
        Assert.Equal("defer-1", repopped.Headers.MessageId);
        Assert.Equal("corr-123", repopped.CorrelationId);
        Assert.Equal(2, repopped.Attempt);
    }
}
