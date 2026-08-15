// SPDX-License-Identifier: Apache-2.0

using Talaria.Core.Abstractions;
using Talaria.Transports.InMemory;

namespace Talaria.Specs.Tests;

/// <summary>
/// Shared async polling helpers used by behavior tests to avoid fixed Task.Delay
/// waits and duplicated helper code across test classes.
/// </summary>
internal static class TestAsyncHelpers
{
    /// <summary>
    /// Polls a topic until it contains at least <paramref name="expectedCount"/> messages.
    /// </summary>
    public static async Task<List<MessageEnvelope<T>>> ReadUntilAsync<T>(
        InMemoryTransport transport,
        string topic,
        int expectedCount,
        TimeSpan? timeout = null)
    {
        var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(10));
        List<MessageEnvelope<T>> messages;
        do
        {
            messages = await transport.ReadAllFromTopicAsync<T>(topic);
            if (messages.Count >= expectedCount)
            {
                break;
            }

            await Task.Delay(50);
        }
        while (DateTime.UtcNow < deadline);

        return messages;
    }

    /// <summary>
    /// Polls a condition until it returns true or the timeout elapses.
    /// </summary>
    public static async Task<bool> PollUntilAsync(Func<Task<bool>> condition, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (await condition())
            {
                return true;
            }

            await Task.Delay(50);
        }

        return await condition();
    }

    /// <summary>
    /// Polls a condition continuously for the given window, returning true only if it
    /// holds for the entire window.
    /// </summary>
    public static async Task<bool> PollStableAsync(Func<Task<bool>> condition, TimeSpan window)
    {
        var deadline = DateTime.UtcNow + window;
        while (DateTime.UtcNow < deadline)
        {
            if (!await condition())
            {
                return false;
            }

            await Task.Delay(50);
        }

        return await condition();
    }
}
