using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using StackExchange.Redis;
using Talaria.Core.Abstractions;
using Talaria.Core.Registration;

namespace Talaria.StateStores.Redis;

/// <summary>
/// Extensions for registering Redis state store with Talaria.
/// All UseRedis* methods share a single <see cref="TalariaRedisOptions"/> registration —
/// configure callbacks accumulate across calls instead of being silently discarded.
/// For production deployments include TLS and auth in the configuration,
/// e.g. "host:6379,ssl=true,password=...".
/// </summary>
public static class RedisStateStoreExtensions
{
    /// <summary>
    /// Configures Talaria to use the Redis state store (singleton, matching the InMemory store).
    /// Also registers the Redis transactional outbox: saga state transitions then stage
    /// their outbound messages atomically with the state write, and a background relay
    /// publishes them.
    /// </summary>
    public static TalariaBuilder UseRedisStateStore(
        this TalariaBuilder builder,
        Action<TalariaRedisOptions>? configure = null)
    {
        builder.ConfigureRedis(configure);

        builder.Services.TryAddSingleton(typeof(IStateStore<>), typeof(RedisStateStore<>));
        builder.Services.TryAddSingleton<IOutboxStore, RedisOutboxStore>();

        return builder;
    }

    /// <summary>
    /// Configures Talaria to use the Redis Idempotency store for exact-once messaging semantics.
    /// </summary>
    public static TalariaBuilder UseRedisIdempotencyStore(
        this TalariaBuilder builder,
        Action<TalariaRedisOptions>? configure = null)
    {
        builder.ConfigureRedis(configure);

        builder.Services.TryAddSingleton<IIdempotencyStore, RedisIdempotencyStore>();

        return builder;
    }

    /// <summary>
    /// Configures Talaria to use the Redis deferral store for durable saga deferrals.
    /// </summary>
    public static TalariaBuilder UseRedisDeferralStore(
        this TalariaBuilder builder,
        Action<TalariaRedisOptions>? configure = null)
    {
        builder.ConfigureRedis(configure);

        builder.Services.TryAddSingleton<IDeferralStore, RedisDeferralStore>();

        return builder;
    }

    /// <summary>
    /// Registers the shared options (callbacks accumulate) and a lazily connecting
    /// IConnectionMultiplexer. Connection happens on first resolve, so a missing
    /// Configuration fails fast at host startup rather than at registration time.
    /// </summary>
    private static TalariaBuilder ConfigureRedis(
        this TalariaBuilder builder,
        Action<TalariaRedisOptions>? configure)
    {
        builder.Services.AddOptions<TalariaRedisOptions>();
        if (configure != null)
        {
            builder.Services.Configure(configure);
        }

        builder.Services.TryAddSingleton<IConnectionMultiplexer>(sp =>
        {
            var options = sp.GetRequiredService<IOptions<TalariaRedisOptions>>().Value;
            if (string.IsNullOrWhiteSpace(options.Configuration))
            {
                throw new InvalidOperationException(
                    $"{nameof(TalariaRedisOptions.Configuration)} is required (e.g. \"localhost:6379\"). " +
                    "Set it via the configure callback of a UseRedis* method.");
            }

            return ConnectionMultiplexer.Connect(options.Configuration);
        });

        return builder;
    }
}
