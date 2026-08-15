// SPDX-License-Identifier: Apache-2.0

using Talaria.Core.Abstractions;
using Talaria.Transports.InMemory;

namespace Talaria.Core.Tests;

/// <summary>
/// Shared async polling helpers used by Core.Tests to avoid duplicated wait loops.
/// </summary>
internal static class TestAsyncHelpers
{
    /// <summary>
    /// Polls a condition until it returns true or the timeout elapses.
    /// </summary>
    public static async Task WaitUntilAsync(Func<bool> condition, TimeSpan? timeout = null)
    {
        var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(10));
        while (DateTime.UtcNow < deadline)
        {
            if (condition())
            {
                return;
            }

            await Task.Delay(25);
        }

        Assert.True(condition(), "Condition was not met within the timeout.");
    }

    /// <summary>
    /// Polls a condition until it returns true or the timeout elapses.
    /// </summary>
    public static async Task WaitUntilAsync(Func<Task<bool>> condition, TimeSpan? timeout = null)
    {
        var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(10));
        while (DateTime.UtcNow < deadline)
        {
            if (await condition())
            {
                return;
            }

            await Task.Delay(25);
        }

        Assert.True(await condition(), "Condition was not met within the timeout.");
    }

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
}
