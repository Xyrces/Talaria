// SPDX-License-Identifier: Apache-2.0

using System.Text.Json;
using Microsoft.Extensions.Options;
using StackExchange.Redis;
using Talaria.Core;
using Talaria.Core.Abstractions;

namespace Talaria.StateStores.Redis;

/// <summary>
/// Redis-backed saga state store for persistence across pods.
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

    private const string SnapshotScript = """
        return {redis.call('GET', KEYS[1]) or '',
            redis.call('HGET', KEYS[2], 'version') or '0',
            redis.call('HGET', KEYS[2], 'completed') or '0',
            redis.call('HGET', KEYS[2], 'message:' .. ARGV[1]) or '0'}
        """;
    private static readonly string VersionedTransitionScript = """
        if redis.call('HGET', KEYS[4], 'message:' .. ARGV[#ARGV]) then return 1 end
        local version = redis.call('HGET', KEYS[4], 'version') or '0'
        if version ~= ARGV[#ARGV - 1] then return 2 end
        """ + "\n" + TransitionScript.Replace("return 1", """
        redis.call('HSET', KEYS[4], 'version', tonumber(version) + 1,
            'completed', ARGV[1] == '' and '1' or '0', 'message:' .. ARGV[#ARGV], '1')
        if tonumber(ARGV[2]) > 0 then redis.call('EXPIRE', KEYS[4], ARGV[2]) end
        return 0
        """);

    public async Task<SagaSnapshot<TState>> ReadSnapshotAsync(string correlationId, string messageId, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        var key = _prefix + RedisKeySpace.Component(correlationId);
        var result = (RedisResult[])(await _db.ScriptEvaluateAsync(SnapshotScript,
            new RedisKey[] { key, key + ":history" }, new RedisValue[] { messageId }))!;
        var json = (string?)result[0];
        return new(string.IsNullOrEmpty(json) ? null : JsonSerializer.Deserialize<TState>(json),
            (long)result[1], (string?)result[2] == "1", (string?)result[3] == "1");
    }

    public async Task<SagaCommitStatus> TryTransitionAsync(string correlationId, string messageId, long expectedVersion,
        TState? newState, IReadOnlyList<OutboxMessage> outbox, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        var key = _prefix + RedisKeySpace.Component(correlationId);
        var args = new List<RedisValue>
        {
            newState is null ? RedisValue.EmptyString : JsonSerializer.Serialize(newState),
            Math.Max(1, (long)_options.DefaultStateTtl.TotalSeconds), outbox.Count,
        };
        foreach (var message in outbox)
        {
            args.Add(message.Id.ToString());
            args.Add(RedisOutboxStore.Serialize(message));
            args.Add(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
        }
        args.Add(expectedVersion.ToString(System.Globalization.CultureInfo.InvariantCulture));
        args.Add(messageId);
        return (SagaCommitStatus)(int)await _db.ScriptEvaluateAsync(VersionedTransitionScript,
            new RedisKey[] { key, _outboxKey, _outboxEntriesKey, key + ":history" }, args.ToArray());
    }

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
        var prefix = RedisKeySpace.Prefix(_options, talariaOptions.Value.ApplicationName);
        _prefix = $"{prefix}saga:{typeof(TState).FullName}:";
        _outboxKey = $"{prefix}outbox";
        _outboxEntriesKey = $"{_outboxKey}:entries";
    }

    public async Task<TState?> GetAsync(string correlationId, CancellationToken ct = default)
    {
        var key = $"{_prefix}{RedisKeySpace.Component(correlationId)}";

        var value = await _db.StringGetAsync(key);
        if (value.IsNullOrEmpty)
            return null;

        return JsonSerializer.Deserialize<TState>(value.ToString());
    }

    public Task SaveAsync(string correlationId, TState state, CancellationToken ct = default)
        => TransitionAsync(correlationId, state, [], ct);

    public Task DeleteAsync(string correlationId, CancellationToken ct = default)
        => TransitionAsync(correlationId, null, [], ct);

    public async Task TransitionAsync(string correlationId, TState? newState,
        IReadOnlyList<OutboxMessage> outbox, CancellationToken ct = default)
    {
        var messageId = Guid.NewGuid().ToString("N");
        while (true)
        {
            var snapshot = await ReadSnapshotAsync(correlationId, messageId, ct);
            if (await TryTransitionAsync(correlationId, messageId, snapshot.Version, newState, outbox, ct)
                != SagaCommitStatus.Conflict) return;
        }
    }
}
