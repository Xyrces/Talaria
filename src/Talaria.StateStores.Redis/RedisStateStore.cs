using System.Text.Json;
using Microsoft.Extensions.Options;
using StackExchange.Redis;
using Talaria.Core;
using Talaria.Core.Abstractions;

namespace Talaria.StateStores.Redis;

/// <summary>
/// Redis-backed state store for propagating saga configurations and persistence across pods.
/// <see cref="TransitionAsync"/> applies the state write/purge and stages outbound messages
/// in the shared outbox within a single Lua script, so a saga state transition and its
/// outbound messages are persisted atomically — the write half of the transactional
/// outbox pattern (the read half is <see cref="RedisOutboxStore"/>).
/// </summary>
public sealed class RedisStateStore<TState> : IStateStore<TState>
    where TState : class, new()
{
    // Atomically apply the state change and stage the outbox entries.
    // KEYS: 1=state key, 2=outbox zset, 3=outbox hash.
    // ARGV: 1=state json ('' = purge), 2=state TTL seconds (0 = no expiry),
    //       3=outbox entry count, then per entry: id, payload json, visible-at ms.
    private const string TransitionScript = """
        if ARGV[1] == '' then
            redis.call('DEL', KEYS[1])
        elseif tonumber(ARGV[2]) > 0 then
            redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
        else
            redis.call('SET', KEYS[1], ARGV[1])
        end
        local idx = 4
        for i = 1, tonumber(ARGV[3]) do
            redis.call('HSET', KEYS[3], ARGV[idx], ARGV[idx + 1])
            redis.call('ZADD', KEYS[2], ARGV[idx + 2], ARGV[idx])
            idx = idx + 3
        end
        return 1
        """;

    private readonly TalariaRedisOptions _options;
    private readonly IDatabase _db;

    // Type name is baked into the prefix to prevent key collision if the correlation IDs are identical across different sagas
    private readonly string _prefix;
    private readonly string _outboxKey;
    private readonly string _outboxEntriesKey;

    /// <summary>
    /// Creates the store. Options are shared across all UseRedis* registrations.
    /// </summary>
    public RedisStateStore(
        IConnectionMultiplexer redis,
        IOptions<TalariaRedisOptions> options,
        IOptions<TalariaOptions> talariaOptions)
    {
        _options = options.Value;
        _db = redis.GetDatabase();
        _prefix = $"{_options.KeyPrefix}{typeof(TState).Name.ToLowerInvariant()}:";
        _outboxKey = $"{_options.KeyPrefix}outbox:{talariaOptions.Value.ApplicationName}";
        _outboxEntriesKey = $"{_outboxKey}:entries";
    }

    public async Task<TState?> GetAsync(string correlationId, CancellationToken ct = default)
    {
        var key = $"{_prefix}{correlationId}";

        var value = await _db.StringGetAsync(key);
        if (value.IsNullOrEmpty)
            return null;

        return JsonSerializer.Deserialize<TState>(value.ToString());
    }

    public async Task SaveAsync(string correlationId, TState state, CancellationToken ct = default)
    {
        var key = $"{_prefix}{correlationId}";
        var json = JsonSerializer.Serialize(state);

        await _db.StringSetAsync(key, json, _options.DefaultStateTtl);
    }

    public async Task DeleteAsync(string correlationId, CancellationToken ct = default)
    {
        var key = $"{_prefix}{correlationId}";
        await _db.KeyDeleteAsync(key);
    }

    public async Task TransitionAsync(
        string correlationId,
        TState? newState,
        IReadOnlyList<OutboxMessage> outbox,
        CancellationToken ct = default)
    {
        var key = $"{_prefix}{correlationId}";

        var args = new List<RedisValue>(4 + outbox.Count * 3)
        {
            newState is null ? RedisValue.EmptyString : JsonSerializer.Serialize(newState),
            (long)_options.DefaultStateTtl.TotalSeconds,
            outbox.Count
        };

        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        foreach (var message in outbox)
        {
            args.Add(message.Id.ToString());
            args.Add(RedisOutboxStore.Serialize(message));
            args.Add(now);
        }

        await _db.ScriptEvaluateAsync(
            TransitionScript,
            new RedisKey[] { key, _outboxKey, _outboxEntriesKey },
            args.ToArray());
    }
}
