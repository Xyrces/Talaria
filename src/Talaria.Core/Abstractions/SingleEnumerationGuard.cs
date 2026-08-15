// SPDX-License-Identifier: Apache-2.0

using System.Threading;

namespace Talaria.Core.Abstractions;

/// <summary>
/// Shared helper that enforces the <see cref="IConsumer{T}.ConsumeAsync"/> single-
/// enumeration contract: one call, and one enumerator, per consumer instance.
/// </summary>
public static class SingleEnumerationGuard
{
    /// <summary>
    /// Message used by all transports when the single-enumeration contract is violated.
    /// </summary>
    public const string Message =
        "ConsumeAsync may only be enumerated once per consumer instance. Create a new consumer to restart consumption.";

    /// <summary>
    /// Throws <see cref="InvalidOperationException"/> if <paramref name="field"/> is already
    /// non-zero, otherwise atomically sets it to 1.
    /// </summary>
    /// <param name="field">The guard flag. Must be initially 0.</param>
    /// <exception cref="InvalidOperationException">The guard has already been tripped.</exception>
    public static void ThrowIfAlreadyStarted(ref int field)
    {
        if (Interlocked.Exchange(ref field, 1) != 0)
        {
            throw new InvalidOperationException(Message);
        }
    }
}
