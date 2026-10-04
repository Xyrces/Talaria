// SPDX-License-Identifier: Apache-2.0

using StackExchange.Redis;
using Talaria.Core;
using Talaria.Core.Abstractions;

namespace Talaria.StateStores.Redis;

/// <summary>
/// Redis-backed duplicate suppression across nodes using distributed locks scoped per consumer queue.
/// </summary>
public sealed class RedisIdempotencyStore : IIdempotencyStore
{
    private const string AcquireScript = """
        local current = redis.call('GET', KEYS[1])
        if current == 'COMPLETED' then return 2 end
        if current then return 1 end
        redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
        return 0
        """;
    private const string CompleteScript = """
        if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
        redis.call('SET', KEYS[1], 'COMPLETED', 'PX', ARGV[2])
        return 1
        """;
    private const string RenewScript = """
        if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
        return redis.call('PEXPIRE', KEYS[1], ARGV[2])
        """;
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

    private string KeyFor(string messageId, string consumerQueue)
        => $"{_prefix}{RedisKeySpace.Component($"{consumerQueue.Length}:{consumerQueue}{messageId.Length}:{messageId}")}";

    /// <summary>
    /// Creates the store. Options are shared across all UseRedis* registrations.
    /// </summary>
    public RedisIdempotencyStore(
        IConnectionMultiplexer redis,
        Microsoft.Extensions.Options.IOptions<TalariaRedisOptions> options,
        Microsoft.Extensions.Options.IOptions<TalariaOptions> talariaOptions)
    {
        _redis = redis;
        _options = options.Value;
        _db = _redis.GetDatabase();
        _prefix = $"{RedisKeySpace.Prefix(_options, talariaOptions.Value.ApplicationName)}idemp:";
    }

    public async Task<IdempotencyAcquisition> AcquireAsync(string messageId, string consumerQueue, TimeSpan expiration, CancellationToken ct = default)
    {
        var key = KeyFor(messageId, consumerQueue);
        var token = Guid.NewGuid().ToString("N");

        ct.ThrowIfCancellationRequested();
        if (expiration <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(expiration));
        var status = (IdempotencyStatus)(int)await _db.ScriptEvaluateAsync(AcquireScript,
            new RedisKey[] { key }, new RedisValue[] { token, Math.Max(1, (long)expiration.TotalMilliseconds) });
        return new(status, status == IdempotencyStatus.Acquired ? new(messageId, consumerQueue, token) : null);
    }

    public async Task<IdempotencyLock?> TryAcquireLockAsync(string messageId, string consumerQueue, TimeSpan expiration, CancellationToken ct = default)
        => (await AcquireAsync(messageId, consumerQueue, expiration, ct)).Lock;

    public async Task<bool> RenewAsync(IdempotencyLock @lock, TimeSpan expiration, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        if (expiration <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(expiration));
        return (int)await _db.ScriptEvaluateAsync(RenewScript,
            new RedisKey[] { KeyFor(@lock.MessageId, @lock.ConsumerQueue) },
            new RedisValue[] { @lock.Token, Math.Max(1, (long)expiration.TotalMilliseconds) }) == 1;
    }

    public async Task MarkCompleteAsync(IdempotencyLock @lock, CancellationToken ct = default)
    {
        var key = KeyFor(@lock.MessageId, @lock.ConsumerQueue);

        ct.ThrowIfCancellationRequested();
        var completed = (int)await _db.ScriptEvaluateAsync(CompleteScript,
            new RedisKey[] { key }, new RedisValue[] { @lock.Token, Math.Max(1, (long)_options.DefaultStateTtl.TotalMilliseconds) });
        if (completed != 1) throw new IdempotencyLeaseLostException(@lock.MessageId);
    }

    public async Task ReleaseLockAsync(IdempotencyLock @lock, CancellationToken ct = default)
    {
        var key = KeyFor(@lock.MessageId, @lock.ConsumerQueue);

        // Free it up immediately so a Nack block can retry — but only if we still own the lock.
        await _db.ScriptEvaluateAsync(ReleaseScript, new RedisKey[] { key }, new RedisValue[] { @lock.Token });
    }
}
