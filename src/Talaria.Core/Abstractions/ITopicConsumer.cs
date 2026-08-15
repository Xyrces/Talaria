// SPDX-License-Identifier: Apache-2.0

namespace Talaria.Core.Abstractions;

/// <summary>
/// A class-based consumer for messages delivered to a topic.
/// Implementations are resolved from a per-message DI scope and invoked by
/// the consumer engine for each delivered envelope.
/// </summary>
/// <typeparam name="T">The CLR message type delivered on the topic.</typeparam>
/// <since>1.0.0</since>
public interface ITopicConsumer<T>
{
    /// <summary>
    /// Consumes a single message.
    /// </summary>
    /// <param name="context">The consume context, including the full envelope, headers, cancellation token, and scoped service provider.</param>
    /// <returns>A task that completes when processing is finished.</returns>
    Task ConsumeAsync(ConsumeContext<T> context);
}

/// <summary>
/// Context supplied to an <see cref="ITopicConsumer{T}"/> invocation.
/// </summary>
/// <typeparam name="T">The CLR message type delivered on the topic.</typeparam>
/// <since>1.0.0</since>
public sealed record ConsumeContext<T>
{
    /// <summary>The full envelope for the delivered message, including payload, headers, and routing metadata.</summary>
    public required MessageEnvelope<T> Envelope { get; init; }

    /// <summary>Cancellation token that is canceled when the consumer loop is shutting down.</summary>
    public required CancellationToken CancellationToken { get; init; }

    /// <summary>The service provider for the per-message DI scope.</summary>
    public required IServiceProvider Services { get; init; }

    /// <summary>Convenience accessor for the deserialized message payload.</summary>
    public T Message => Envelope.Payload!;

    /// <summary>Convenience accessor for the message headers.</summary>
    public MessageHeaders Headers => Envelope.Headers;
}
