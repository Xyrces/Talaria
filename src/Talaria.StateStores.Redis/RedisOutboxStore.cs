using System.Text.Json;
using Microsoft.Extensions.Options;
using StackExchange.Redis;
using Talaria.Core;
using Talaria.Core.Abstractions;

namespace Talaria.StateStores.Redis;

/// <summary>
/// Redis-backed read side of the transactional outbox, with lease (visibility-timeout)
/// semantics identical to <see cref="RedisDeferralStore"/>: entry ids live in a sorted
/// set scored by visibility time; payloads and monotonic lease counters live in a
/// companion hash. Entries are staged atomically with saga state transitions by
/// <see cref="RedisStateStore{TState}.TransitionAsync"/>; the relay acquires, publishes,
/// and completes them. A relay crash never loses a staged message — the lease expires
/// and the entry is re-acquired, and the stable minted MessageId makes the resulting
/// duplicate publish a no-op downstream.
/// </summary>
public sealed class RedisOutboxStore : IOutboxStore
{
    // Atomically lease up to ARGV[3] entries visible at or before ARGV[1]: bump each
    // entry's fencing counter and hide it until ARGV[2] (lease expiry). Returns a flat
    // array of [id, lease token, payload json] triples.
    // KEYS: 1=zset, 2=hash. ARGV: 1=now ms, 2=lease-expiry ms, 3=max batch.
    private const string AcquireScript = """
        local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[3])
        local out = {}
        for i, id in ipairs(ids) do
            local lease = redis.call('HINCRBY', KEYS[2], id .. ':lease', 1)
            redis.call('ZADD', KEYS[1], ARGV[2], id)
            out[#out + 1] = id
            out[#out + 1] = tostring(lease)
            out[#out + 1] = redis.call('HGET', KEYS[2], id)
        end
        return out
        """;

    // Remove the entry only when the caller's fencing token is current.
    // KEYS: 1=zset, 2=hash. ARGV: 1=entry id, 2=lease token.
    private const string CompleteScript = """
        if redis.call('HGET', KEYS[2], ARGV[1] .. ':lease') == ARGV[2] then
            redis.call('ZREM', KEYS[1], ARGV[1])
            redis.call('HDEL', KEYS[2], ARGV[1], ARGV[1] .. ':lease')
            return 1
        end
        return 0
        """;

    // Make the entry visible again at ARGV[3] only when the caller's token is current.
    // KEYS: 1=zset, 2=hash. ARGV: 1=entry id, 2=lease token, 3=visible-at ms.
    private const string AbandonScript = """
        if redis.call('HGET', KEYS[2], ARGV[1] .. ':lease') == ARGV[2] then
            redis.call('ZADD', KEYS[1], ARGV[3], ARGV[1])
            return 1
        end
        return 0
        """;

    private static readonly JsonSerializerOptions SerializerOptions = new(JsonSerializerDefaults.Web);

    private readonly IDatabase _db;
    private readonly string _key;
    private readonly string _entriesKey;

    /// <summary>
    /// Creates the store. Keys match the ones <see cref="RedisStateStore{TState}"/>
    /// stages into, namespaced by key prefix and application name.
    /// </summary>
    public RedisOutboxStore(
        IConnectionMultiplexer redis,
        IOptions<TalariaRedisOptions> options,
        IOptions<TalariaOptions> talariaOptions)
    {
        _db = redis.GetDatabase();
        _key = $"{options.Value.KeyPrefix}outbox:{talariaOptions.Value.ApplicationName}";
        _entriesKey = $"{_key}:entries";
    }

    public async Task<IReadOnlyList<LeasedOutboxMessage>> AcquirePendingAsync(
        DateTimeOffset now,
        TimeSpan leaseDuration,
        int maxBatch,
        CancellationToken ct = default)
    {
        var result = await _db.ScriptEvaluateAsync(
            AcquireScript,
            new RedisKey[] { _key, _entriesKey },
            new RedisValue[]
            {
                now.ToUnixTimeMilliseconds(),
                now.Add(leaseDuration).ToUnixTimeMilliseconds(),
                maxBatch
            });

        var flat = (RedisValue[])result!;
        var leased = new List<LeasedOutboxMessage>(flat.Length / 3);
        for (var i = 0; i < flat.Length; i += 3)
        {
            var message = Deserialize((string)flat[i + 2]!);
            leased.Add(new LeasedOutboxMessage(
                message,
                new OutboxLease(message.Id, long.Parse((string)flat[i + 1]!))));
        }

        return leased;
    }

    public async Task<bool> CompleteAsync(OutboxLease lease, CancellationToken ct = default)
    {
        var result = await _db.ScriptEvaluateAsync(
            CompleteScript,
            new RedisKey[] { _key, _entriesKey },
            new RedisValue[] { lease.Id.ToString(), lease.Token.ToString() });

        return (long)result! == 1;
    }

    public async Task<bool> AbandonAsync(OutboxLease lease, DateTimeOffset? visibleAt = null, CancellationToken ct = default)
    {
        var result = await _db.ScriptEvaluateAsync(
            AbandonScript,
            new RedisKey[] { _key, _entriesKey },
            new RedisValue[]
            {
                lease.Id.ToString(),
                lease.Token.ToString(),
                (visibleAt ?? DateTimeOffset.UtcNow).ToUnixTimeMilliseconds()
            });

        return (long)result! == 1;
    }

    // Shared with RedisStateStore<TState>.TransitionAsync, which stages the entries
    // this store later reads — the wire format must be identical on both sides.
    internal static string Serialize(OutboxMessage message)
        => JsonSerializer.Serialize(new OutboxMessageDto(
            message.Id,
            message.Topic,
            message.MessageType,
            message.PayloadJson,
            message.Headers.ToDictionary(kvp => kvp.Key, kvp => kvp.Value),
            message.CreatedAt), SerializerOptions);

    private static OutboxMessage Deserialize(string json)
    {
        var dto = JsonSerializer.Deserialize<OutboxMessageDto>(json, SerializerOptions)
            ?? throw new JsonException("Outbox entry deserialized to null.");

        return new OutboxMessage(
            dto.Id,
            dto.Topic,
            dto.MessageType,
            dto.PayloadJson,
            new MessageHeaders(dto.Headers),
            dto.CreatedAt);
    }

    // Flat DTO so the wire format stays stable even if MessageHeaders internals change.
    private sealed record OutboxMessageDto(
        Guid Id,
        string Topic,
        string MessageType,
        string PayloadJson,
        Dictionary<string, string> Headers,
        DateTimeOffset CreatedAt);
}
