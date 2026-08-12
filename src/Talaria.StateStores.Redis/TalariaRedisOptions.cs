// SPDX-License-Identifier: AGPL-3.0-or-later

namespace Talaria.StateStores.Redis;

/// <summary>
/// Configuration options for the Redis State Store.
/// </summary>
/// <since>1.0.0</since>
public sealed class TalariaRedisOptions
{
    /// <summary>
    /// The Redis connection string (Configuration). Required — registration throws
    /// when it is null or empty. For production deployments include TLS and auth,
    /// e.g. "host:6379,ssl=true,password=...".
    /// </summary>
    public string Configuration { get; set; } = string.Empty;

    /// <summary>
    /// An optional prefix applied to all Redis keys to isolate environments or tenants.
    /// Default is "talaria:".
    /// </summary>
    public string KeyPrefix { get; set; } = "talaria:";

    /// <summary>
    /// Global TTL applied both to persisted saga states AND to idempotency completion
    /// markers (lowering it shortens dedup retention as well as saga memory).
    /// By default entries expire after 30 days of inactivity to prevent memory bloat.
    /// </summary>
    public TimeSpan DefaultStateTtl { get; set; } = TimeSpan.FromDays(30);
}
