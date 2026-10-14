// SPDX-License-Identifier: Apache-2.0
using System.Text.Json;
using Microsoft.Extensions.Options;
using StackExchange.Redis;
using Talaria.Core;
using Talaria.Core.Abstractions;

namespace Talaria.StateStores.Redis;

/// <summary>Redis-backed, application-scoped failed-message retention with no automatic expiry.</summary>
public sealed class RedisFailedMessageStore : IFailedMessageStore
{
    private const string SaveScript = """
        redis.call('HSET', KEYS[2], ARGV[1], ARGV[2])
        redis.call('ZADD', KEYS[1], ARGV[3], ARGV[1])
        return 1
        """;
    private const string DeleteScript = """
        local removed = redis.call('HDEL', KEYS[2], ARGV[1])
        redis.call('ZREM', KEYS[1], ARGV[1])
        return removed
        """;

    private static readonly JsonSerializerOptions SerializerOptions = new(JsonSerializerDefaults.Web);
    private readonly IDatabase _db;
    private readonly string _key;
    private readonly string _entriesKey;

    public RedisFailedMessageStore(
        IConnectionMultiplexer redis,
        IOptions<TalariaRedisOptions> options,
        IOptions<TalariaOptions> talariaOptions)
    {
        _db = redis.GetDatabase();
        _key = RedisKeySpace.Prefix(options.Value, talariaOptions.Value.ApplicationName) + "failed";
        _entriesKey = _key + ":entries";
    }

    public async Task SaveAsync(FailedMessage message, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(message);
        ct.ThrowIfCancellationRequested();
        await _db.ScriptEvaluateAsync(SaveScript,
            new RedisKey[] { _key, _entriesKey },
            new RedisValue[] { message.Id.ToString("N"), Serialize(message), message.FailedAt.ToUnixTimeMilliseconds() });
    }

    public async Task<FailedMessage?> GetAsync(Guid id, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        var value = await _db.HashGetAsync(_entriesKey, id.ToString("N"));
        return value.IsNullOrEmpty ? null : Deserialize(value!);
    }

    public async Task<IReadOnlyList<FailedMessage>> ListAsync(int limit = 100, CancellationToken ct = default)
    {
        if (limit is < 1 or > 1000) throw new ArgumentOutOfRangeException(nameof(limit), "Limit must be between 1 and 1000.");
        ct.ThrowIfCancellationRequested();
        var ids = await _db.SortedSetRangeByRankAsync(_key, 0, limit - 1, Order.Descending);
        if (ids.Length == 0) return [];
        var values = await _db.HashGetAsync(_entriesKey, ids);
        return values.Where(value => !value.IsNullOrEmpty).Select(Deserialize).ToArray();
    }

    public async Task<bool> DeleteAsync(Guid id, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        return (long)await _db.ScriptEvaluateAsync(DeleteScript,
            new RedisKey[] { _key, _entriesKey }, new RedisValue[] { id.ToString("N") }) == 1;
    }

    private static string Serialize(FailedMessage message)
        => JsonSerializer.Serialize(new FailedMessageDto(message.Id, message.Topic, message.MessageType,
            message.PayloadJson, message.Headers.ToDictionary(x => x.Key, x => x.Value), message.PartitionKey,
            message.Reason, message.FailedAt), SerializerOptions);

    private static FailedMessage Deserialize(RedisValue value)
    {
        var dto = JsonSerializer.Deserialize<FailedMessageDto>((string)value!, SerializerOptions)
            ?? throw new JsonException("Failed message entry deserialized to null.");
        return new(dto.Id, dto.Topic, dto.MessageType, dto.PayloadJson, new MessageHeaders(dto.Headers),
            dto.PartitionKey, dto.Reason, dto.FailedAt);
    }

    private sealed record FailedMessageDto(Guid Id, string Topic, string MessageType, string PayloadJson,
        Dictionary<string, string> Headers, string? PartitionKey, string Reason, DateTimeOffset FailedAt);
}
