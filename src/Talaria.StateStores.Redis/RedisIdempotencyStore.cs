using StackExchange.Redis;
using Talaria.Core.Abstractions;

namespace Talaria.StateStores.Redis;

/// <summary>
/// A centralized Redis footprint providing exactly-once semantics and multi-node concurrency isolation.
/// </summary>
public sealed class RedisIdempotencyStore : IIdempotencyStore
{
    // Compare-and-delete: only the current lock owner (fencing token match) may release the lock.
    private const string ReleaseScript = """
        if redis.call('get', KEYS[1]) == ARGV[1] then
            return redis.call('del', KEYS[1])
        else
            return 0
        end
        """;

    private readonly IConnectionMultiplexer _redis;
    private readonly TalariaRedisOptions _options;
    private readonly IDatabase _db;

    private readonly string _prefix;

    public RedisIdempotencyStore(IConnectionMultiplexer redis, TalariaRedisOptions options)
    {
        _redis = redis;
        _options = options;
        _db = _redis.GetDatabase();
        _prefix = $"{_options.KeyPrefix}idemp:";
    }

    public async Task<IdempotencyLock?> TryAcquireLockAsync(string messageId, string consumerQueue, TimeSpan expiration, CancellationToken ct = default)
    {
        var key = $"{_prefix}{consumerQueue}:{messageId}";
        var token = Guid.NewGuid().ToString("N");

        // When.NotExists atomically guarantees that only ONE thread (or container replica) gets the lock.
        // It serves as a distributed CAS primitive.
        var acquired = await _db.StringSetAsync(key, token, expiration, When.NotExists);
        return acquired ? new IdempotencyLock(messageId, consumerQueue, token) : null;
    }

    public async Task MarkCompleteAsync(IdempotencyLock @lock, CancellationToken ct = default)
    {
        var key = $"{_prefix}{@lock.ConsumerQueue}:{@lock.MessageId}";

        // Persist the completion marker for the default generalized TTL (30 days by default, to ensure replays don't execute even weeks later)
        await _db.StringSetAsync(key, "COMPLETED", _options.DefaultStateTtl, When.Always);
    }

    public async Task ReleaseLockAsync(IdempotencyLock @lock, CancellationToken ct = default)
    {
        var key = $"{_prefix}{@lock.ConsumerQueue}:{@lock.MessageId}";

        // Free it up immediately so a Nack block can retry — but only if we still own the lock.
        await _db.ScriptEvaluateAsync(ReleaseScript, new RedisKey[] { key }, new RedisValue[] { @lock.Token });
    }
}
