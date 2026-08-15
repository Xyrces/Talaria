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
    public async Task DeferralStore_Roundtrip_LeasesCompleteAndAbandon()
    {
        var store = _serviceProvider.GetRequiredService<IDeferralStore>();
        var lease = TimeSpan.FromSeconds(30);

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
            now.AddSeconds(-5),
            "order-partition-7");

        await store.EnqueueAsync(message);

        // Due in the past: leased once, with all fields surviving the roundtrip.
        var due = await store.AcquireDueAsync(now, lease, 10);
        var acquired = Assert.Single(due);
        Assert.Equal(message.Id, acquired.Message.Id);
        Assert.Equal("orders-topic", acquired.Message.Topic);
        Assert.Equal("System.String", acquired.Message.MessageType);
        Assert.Equal("\"hello-deferred\"", acquired.Message.PayloadJson);
        Assert.Equal("order-partition-7", acquired.Message.PartitionKey);
        Assert.Equal("corr-123", acquired.Message.CorrelationId);
        Assert.Equal(2, acquired.Message.Attempt);
        Assert.Equal("defer-1", acquired.Message.Headers.MessageId);
        Assert.Equal("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", acquired.Message.Headers.TraceParent);
        Assert.Equal("corr-123", acquired.Message.Headers[MessageHeaders.CorrelationIdKey]);

        // The lease hides the entry — a concurrent sweep sees nothing.
        Assert.Empty(await store.AcquireDueAsync(now.AddSeconds(5), lease, 10));

        // Abandoning into the future reschedules it: not acquirable before it is due.
        var futureDue = now.AddHours(1);
        Assert.True(await store.AbandonAsync(acquired.Lease, futureDue));
        Assert.Empty(await store.AcquireDueAsync(now.AddMinutes(30), lease, 10));

        // Once due again, it leases with its fields intact and a bumped fencing token.
        var dueAgain = await store.AcquireDueAsync(futureDue.AddMinutes(1), lease, 10);
        var reacquired = Assert.Single(dueAgain);
        Assert.Equal(message.Id, reacquired.Message.Id);
        Assert.Equal("defer-1", reacquired.Message.Headers.MessageId);
        Assert.Equal("order-partition-7", reacquired.Message.PartitionKey);
        Assert.Equal("corr-123", reacquired.Message.CorrelationId);
        Assert.Equal(2, reacquired.Message.Attempt);
        Assert.True(reacquired.Lease.Token > acquired.Lease.Token);

        // Completion is fenced: the stale holder fails, the current owner succeeds,
        // and the entry is gone for good.
        Assert.False(await store.CompleteAsync(acquired.Lease));
        Assert.True(await store.CompleteAsync(reacquired.Lease));
        Assert.Empty(await store.AcquireDueAsync(futureDue.AddMinutes(2), lease, 10));
    }
}
