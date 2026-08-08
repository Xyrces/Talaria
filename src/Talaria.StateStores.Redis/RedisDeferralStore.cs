using System.Text.Json;
using Microsoft.Extensions.Options;
using StackExchange.Redis;
using Talaria.Core;
using Talaria.Core.Abstractions;

namespace Talaria.StateStores.Redis;

/// <summary>
/// Redis-backed deferral store. Deferred messages live in a sorted set keyed by
/// application name, scored by their due time (unix milliseconds). Popping due
/// messages is an atomic claim (Lua), so multiple nodes of the same application
/// can sweep concurrently without double-publishing.
/// </summary>
public sealed class RedisDeferralStore : IDeferralStore
{
    // Atomically fetch and remove up to ARGV[2] members scored at or before ARGV[1].
    private const string PopDueScript = """
        local members = redis.call('ZRANGEBYSCORE', KEYS[1], 0, ARGV[1], 'LIMIT', 0, ARGV[2])
        for i, member in ipairs(members) do
            redis.call('ZREM', KEYS[1], member)
        end
        return members
        """;

    private static readonly JsonSerializerOptions SerializerOptions = new(JsonSerializerDefaults.Web);

    private readonly IDatabase _db;
    private readonly string _key;

    public RedisDeferralStore(
        IConnectionMultiplexer redis,
        TalariaRedisOptions options,
        IOptions<TalariaOptions> talariaOptions)
    {
        _db = redis.GetDatabase();
        _key = $"{options.KeyPrefix}defer:{talariaOptions.Value.ApplicationName}";
    }

    public async Task EnqueueAsync(DeferredMessage message, CancellationToken ct = default)
    {
        var member = Serialize(message);
        await _db.SortedSetAddAsync(_key, member, message.DueAt.ToUnixTimeMilliseconds());
    }

    public async Task<IReadOnlyList<DeferredMessage>> PopDueAsync(DateTimeOffset now, int maxBatch, CancellationToken ct = default)
    {
        var result = await _db.ScriptEvaluateAsync(
            PopDueScript,
            new RedisKey[] { _key },
            new RedisValue[] { now.ToUnixTimeMilliseconds(), maxBatch });

        return ((RedisValue[])result!)
            .Select(v => Deserialize((string)v!))
            .ToList();
    }

    public Task CompleteAsync(Guid id, CancellationToken ct = default)
    {
        // Pop atomically removes the member, so completion is a confirmation no-op.
        return Task.CompletedTask;
    }

    public async Task RequeueAsync(DeferredMessage message, DateTimeOffset newDueAt, CancellationToken ct = default)
    {
        var member = Serialize(message);
        await _db.SortedSetAddAsync(_key, member, newDueAt.ToUnixTimeMilliseconds());
    }

    private static string Serialize(DeferredMessage message)
        => JsonSerializer.Serialize(new DeferredMessageDto(
            message.Id,
            message.Topic,
            message.MessageType,
            message.PayloadJson,
            message.Headers.ToDictionary(kvp => kvp.Key, kvp => kvp.Value),
            message.CorrelationId,
            message.Attempt,
            message.DueAt), SerializerOptions);

    private static DeferredMessage Deserialize(string json)
    {
        var dto = JsonSerializer.Deserialize<DeferredMessageDto>(json, SerializerOptions)
            ?? throw new JsonException("Deferred message entry deserialized to null.");

        return new DeferredMessage(
            dto.Id,
            dto.Topic,
            dto.MessageType,
            dto.PayloadJson,
            new MessageHeaders(dto.Headers),
            dto.CorrelationId,
            dto.Attempt,
            dto.DueAt);
    }

    // Flat DTO so the wire format stays stable even if MessageHeaders internals change.
    private sealed record DeferredMessageDto(
        Guid Id,
        string Topic,
        string MessageType,
        string PayloadJson,
        Dictionary<string, string> Headers,
        string? CorrelationId,
        int Attempt,
        DateTimeOffset DueAt);
}
