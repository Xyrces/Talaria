// SPDX-License-Identifier: AGPL-3.0-or-later

namespace Talaria.Core.Abstractions;

/// <summary>
/// Produces messages to a topic.
/// </summary>
public interface IProducer<T> : IAsyncDisposable
{
    /// <summary>
    /// Publishes a message to the configured topic.
    /// </summary>
    Task ProduceAsync(
        T message,
        MessageHeaders? headers = null,
        string? partitionKey = null,
        CancellationToken ct = default);
}
