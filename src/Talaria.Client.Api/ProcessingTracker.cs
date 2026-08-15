// SPDX-License-Identifier: Apache-2.0

using System.Collections.Concurrent;

namespace Talaria.Client.Api;

/// <summary>
/// Demo/diagnostics counter for side effects (saga handler invocations, emails sent).
/// Used by the AppHost integration tests to assert idempotent duplicate suppression.
/// </summary>
public sealed class ProcessingTracker
{
    private readonly ConcurrentDictionary<string, int> _counts = new(StringComparer.Ordinal);

    public void Increment(string key) => _counts.AddOrUpdate(key, 1, (_, count) => count + 1);

    public int Get(string key) => _counts.TryGetValue(key, out var count) ? count : 0;
}
