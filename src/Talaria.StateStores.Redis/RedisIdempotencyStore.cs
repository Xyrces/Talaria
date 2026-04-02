using StackExchange.Redis;
using Talaria.Core.Abstractions;

namespace Talaria.StateStores.Redis;

/// <summary>
/// A centralized Redis footprint providing exactly-once semantics and multi-node concurrency isolation.
/// </summary>
public sealed class RedisIdempotencyStore : IIdempotencyStore
{
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

    public async Task<bool> TryAcquireLockAsync(string messageId, string consumerQueue, TimeSpan expiration, CancellationToken ct = default)
    {
        var key = $"{_prefix}{consumerQueue}:{messageId}";

        // When.NotExists atomically guarantees that only ONE thread (or container replica) gets the true result.
        // It serves as a distributed CAS primitive.
        return await _db.StringSetAsync(key, "PROCESSING", expiration, When.NotExists);
    }

    public async Task MarkCompleteAsync(string messageId, string consumerQueue, CancellationToken ct = default)
    {
        var key = $"{_prefix}{consumerQueue}:{messageId}";
        
        // Persist the lock for the default generalized TTL (30 days by default, to ensure replays don't execute even weeks later)
        await _db.StringSetAsync(key, "COMPLETED", _options.DefaultStateTtl, When.Always);
    }

    public async Task ReleaseLockAsync(string messageId, string consumerQueue, CancellationToken ct = default)
    {
        var key = $"{_prefix}{consumerQueue}:{messageId}";
        
        // Free it up immediately so a Nack block can retry.
        await _db.KeyDeleteAsync(key);
    }
}
