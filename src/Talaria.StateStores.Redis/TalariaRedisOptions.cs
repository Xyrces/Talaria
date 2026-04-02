namespace Talaria.StateStores.Redis;

/// <summary>
/// Configuration options for the Redis State Store.
/// </summary>
public sealed class TalariaRedisOptions
{
    /// <summary>
    /// The Redis connection string (Configuration).
    /// </summary>
    public string Configuration { get; set; } = "localhost:6379";

    /// <summary>
    /// An optional prefix applied to all Redis keys to isolate environments or tenants.
    /// Default is "talaria:".
    /// </summary>
    public string KeyPrefix { get; set; } = "talaria:";

    /// <summary>
    /// Defines a global TTL for saga states.
    /// By default sagas expire after 30 days of inactivity to prevent memory bloat.
    /// </summary>
    public TimeSpan DefaultStateTtl { get; set; } = TimeSpan.FromDays(30);
}
