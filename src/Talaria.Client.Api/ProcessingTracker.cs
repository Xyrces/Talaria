// SPDX-License-Identifier: Apache-2.0

using System.Collections.Concurrent;
using StackExchange.Redis;

namespace Talaria.Client.Api;

/// <summary>
/// Demo/diagnostics counter for side effects (saga handler invocations, emails sent).
/// Used by the AppHost integration tests to assert idempotent duplicate suppression.
/// </summary>
/// <remarks>
/// When Redis is available the counter is stored in Redis so the diagnostics endpoint
/// returns a consistent total across all replicas. If Redis is not registered (e.g. the
/// InMemory transport configuration) the counter falls back to an in-memory dictionary.
/// </remarks>
public sealed class ProcessingTracker
{
    private const string KeyPrefix = "talaria:tracker:";

    private readonly ConcurrentDictionary<string, int> _counts = new(StringComparer.Ordinal);
    private readonly IConnectionMultiplexer? _redis;

    public ProcessingTracker(IConnectionMultiplexer? redis = null)
    {
        _redis = redis;
    }

    public void Increment(string key)
    {
        _counts.AddOrUpdate(key, 1, (_, count) => count + 1);

        if (_redis is null)
        {
            return;
        }

        try
        {
            _redis.GetDatabase().StringIncrement(KeyPrefix + key);
        }
        catch
        {
            // The in-memory count is already updated; best-effort Redis mirroring must
            // not break the handler if Redis is temporarily unreachable.
        }
    }

    public int Get(string key)
    {
        if (_redis is null)
        {
            return _counts.TryGetValue(key, out var count) ? count : 0;
        }

        try
        {
            var value = _redis.GetDatabase().StringGet(KeyPrefix + key);
            return (int?)value ?? 0;
        }
        catch
        {
            return _counts.TryGetValue(key, out var count) ? count : 0;
        }
    }
}
