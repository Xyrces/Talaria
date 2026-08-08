using Microsoft.Extensions.DependencyInjection;
using StackExchange.Redis;
using Talaria.Core.Abstractions;
using Talaria.Core.Registration;

namespace Talaria.StateStores.Redis;

/// <summary>
/// Extensions for registering Redis state store with Talaria.
/// </summary>
public static class RedisStateStoreExtensions
{
    /// <summary>
    /// Configures Talaria to use the Redis state store.
    /// </summary>
    public static TalariaBuilder UseRedisStateStore(
        this TalariaBuilder builder,
        Action<TalariaRedisOptions>? configure = null)
    {
        var options = new TalariaRedisOptions();
        configure?.Invoke(options);
        ValidateConfiguration(options);

        // Register the options
        builder.Services.AddSingleton(options);

        // Register IConnectionMultiplexer lazily
        builder.Services.AddSingleton<IConnectionMultiplexer>(sp => 
            ConnectionMultiplexer.Connect(options.Configuration));

        // Use the generic Redis State Store
        builder.Services.AddTransient(typeof(IStateStore<>), typeof(RedisStateStore<>));

        return builder;
    }

    /// <summary>
    /// Configures Talaria to use the Redis Idempotency store for exact-once messaging semantics.
    /// </summary>
    public static TalariaBuilder UseRedisIdempotencyStore(
        this TalariaBuilder builder,
        Action<TalariaRedisOptions>? configure = null)
    {
        var options = new TalariaRedisOptions();
        configure?.Invoke(options);
        ValidateConfiguration(options);

        // If not already registered via UseRedisStateStore
        if (!builder.Services.Any(d => d.ServiceType == typeof(TalariaRedisOptions)))
        {
            builder.Services.AddSingleton(options);
        }
        
        if (!builder.Services.Any(d => d.ServiceType == typeof(IConnectionMultiplexer)))
        {
            builder.Services.AddSingleton<IConnectionMultiplexer>(sp => 
                ConnectionMultiplexer.Connect(options.Configuration));
        }

        builder.UseIdempotencyStore<RedisIdempotencyStore>();

        return builder;
    }

    private static void ValidateConfiguration(TalariaRedisOptions options)
    {
        if (string.IsNullOrWhiteSpace(options.Configuration))
        {
            throw new ArgumentException(
                $"{nameof(TalariaRedisOptions.Configuration)} is required (e.g. \"localhost:6379\"). " +
                "Set it via the configure callback.");
        }
    }
}
