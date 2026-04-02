using System.Text.Json;
using StackExchange.Redis;
using Talaria.Core.Abstractions;

namespace Talaria.StateStores.Redis;

/// <summary>
/// Redis-backed state store for propagating saga configurations and persistence across pods.
/// </summary>
public sealed class RedisStateStore<TState> : IStateStore<TState>
    where TState : class, new()
{
    private readonly IConnectionMultiplexer _redis;
    private readonly TalariaRedisOptions _options;
    private readonly IDatabase _db;
    
    // Type name is baked into the prefix to prevent key collision if the correlation IDs are identical across different sagas
    private readonly string _prefix;

    public RedisStateStore(IConnectionMultiplexer redis, TalariaRedisOptions options)
    {
        _redis = redis;
        _options = options;
        _db = _redis.GetDatabase();
        _prefix = $"{_options.KeyPrefix}{typeof(TState).Name.ToLowerInvariant()}:";
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
}
