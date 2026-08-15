// SPDX-License-Identifier: Apache-2.0

using Talaria.Core.Abstractions;

namespace Talaria.Core.Registration;

/// <summary>
/// Registry-based extension methods for mapping message handlers directly against
/// a <see cref="TopicRegistry"/>. These mirror the <see cref="IServiceProvider"/>
/// overloads in <see cref="TalariaEndpointExtensions"/> and are useful for
/// host-agnostic composition roots that build a <see cref="Hosting.TalariaListener"/>
/// manually.
/// </summary>
public static class TopicRegistryExtensions
{
    /// <summary>
    /// Maps a handler delegate to a message topic.
    /// </summary>
    /// <typeparam name="T">The CLR message type to deserialize from each envelope.</typeparam>
    /// <param name="registry">The topic registry to mutate.</param>
    /// <param name="topic">The topic name to subscribe to.</param>
    /// <param name="handler">Async handler invoked for each delivered message.</param>
    /// <param name="retryPolicy">Optional retry policy for this topic. Null falls back to <see cref="TalariaOptions.DefaultRetryPolicy"/>.</param>
    /// <returns>The same <paramref name="registry"/>, for chaining.</returns>
    public static TopicRegistry MapTopic<T>(
        this TopicRegistry registry,
        string topic,
        Func<T, CancellationToken, Task> handler,
        RetryPolicy? retryPolicy = null)
    {
        return AddTopicRegistration(registry, topic, typeof(T), retryPolicy, null, null,
            async (payload, _, _, ct) => await handler((T)payload, ct));
    }

    /// <summary>
    /// Maps a handler delegate to a message topic with an explicit consumer group.
    /// </summary>
    /// <typeparam name="T">The CLR message type to deserialize from each envelope.</typeparam>
    /// <param name="registry">The topic registry to mutate.</param>
    /// <param name="topic">The topic name to subscribe to.</param>
    /// <param name="consumerGroup">The consumer group identifier. Overrides <see cref="TalariaOptions.ConsumerGroupOverride"/>.</param>
    /// <param name="handler">Async handler invoked for each delivered message.</param>
    /// <param name="retryPolicy">Optional retry policy for this topic. Null falls back to <see cref="TalariaOptions.DefaultRetryPolicy"/>.</param>
    /// <returns>The same <paramref name="registry"/>, for chaining.</returns>
    public static TopicRegistry MapTopic<T>(
        this TopicRegistry registry,
        string topic,
        string consumerGroup,
        Func<T, CancellationToken, Task> handler,
        RetryPolicy? retryPolicy = null)
    {
        return AddTopicRegistration(registry, topic, typeof(T), retryPolicy, consumerGroup, null,
            async (payload, _, _, ct) => await handler((T)payload, ct));
    }

    /// <summary>
    /// Maps an envelope-aware handler to a message topic.
    /// </summary>
    /// <typeparam name="T">The CLR message type to deserialize from each envelope.</typeparam>
    /// <param name="registry">The topic registry to mutate.</param>
    /// <param name="topic">The topic name to subscribe to.</param>
    /// <param name="handler">Async handler invoked for each delivered message.</param>
    /// <param name="retryPolicy">Optional retry policy for this topic. Null falls back to <see cref="TalariaOptions.DefaultRetryPolicy"/>.</param>
    /// <returns>The same <paramref name="registry"/>, for chaining.</returns>
    public static TopicRegistry MapTopicWithEnvelope<T>(
        this TopicRegistry registry,
        string topic,
        Func<MessageEnvelope<T>, CancellationToken, Task> handler,
        RetryPolicy? retryPolicy = null)
    {
        return AddTopicRegistration(registry, topic, typeof(T), retryPolicy, null, null,
            async (payload, headers, metadata, ct) =>
            {
                var envelope = new MessageEnvelope<T>
                {
                    Payload = (T)payload,
                    Headers = headers,
                    SourceTopic = topic,
                    PartitionKey = metadata.PartitionKey,
                    Partition = metadata.Partition,
                    Offset = metadata.Offset,
                    Timestamp = metadata.Timestamp,
                    CorrelationId = metadata.CorrelationId,
                };
                await handler(envelope, ct);
            });
    }

    /// <summary>
    /// Maps a synchronous handler to a message topic.
    /// </summary>
    /// <typeparam name="T">The CLR message type to deserialize from each envelope.</typeparam>
    /// <param name="registry">The topic registry to mutate.</param>
    /// <param name="topic">The topic name to subscribe to.</param>
    /// <param name="handler">Synchronous handler invoked for each delivered message.</param>
    /// <param name="retryPolicy">Optional retry policy for this topic. Null falls back to <see cref="TalariaOptions.DefaultRetryPolicy"/>.</param>
    /// <returns>The same <paramref name="registry"/>, for chaining.</returns>
    public static TopicRegistry MapTopic<T>(
        this TopicRegistry registry,
        string topic,
        Action<T> handler,
        RetryPolicy? retryPolicy = null)
    {
        return registry.MapTopic<T>(topic, (msg, _) =>
        {
            handler(msg);
            return Task.CompletedTask;
        }, retryPolicy);
    }

    /// <summary>
    /// Maps a class-based consumer to a message topic. The consumer is resolved from
    /// a per-message DI scope by its concrete type <typeparamref name="TConsumer"/>.
    /// </summary>
    /// <typeparam name="TMessage">The CLR message type to deserialize from each envelope.</typeparam>
    /// <typeparam name="TConsumer">The concrete consumer type implementing <see cref="ITopicConsumer{TMessage}"/>.</typeparam>
    /// <param name="registry">The topic registry to mutate.</param>
    /// <param name="topic">The topic name to subscribe to.</param>
    /// <param name="retryPolicy">Optional retry policy for this topic. Null falls back to <see cref="TalariaOptions.DefaultRetryPolicy"/>.</param>
    /// <returns>The same <paramref name="registry"/>, for chaining.</returns>
    public static TopicRegistry MapTopic<TMessage, TConsumer>(
        this TopicRegistry registry,
        string topic,
        RetryPolicy? retryPolicy = null)
        where TConsumer : class, ITopicConsumer<TMessage>
    {
        return AddTopicRegistration(registry, topic, typeof(TMessage), retryPolicy, null, typeof(TConsumer), null);
    }

    /// <summary>
    /// Maps a class-based consumer to a message topic with an explicit consumer group.
    /// The consumer is resolved from a per-message DI scope by its concrete type <typeparamref name="TConsumer"/>.
    /// </summary>
    /// <typeparam name="TMessage">The CLR message type to deserialize from each envelope.</typeparam>
    /// <typeparam name="TConsumer">The concrete consumer type implementing <see cref="ITopicConsumer{TMessage}"/>.</typeparam>
    /// <param name="registry">The topic registry to mutate.</param>
    /// <param name="topic">The topic name to subscribe to.</param>
    /// <param name="consumerGroup">The consumer group identifier. Overrides <see cref="TalariaOptions.ConsumerGroupOverride"/>.</param>
    /// <param name="retryPolicy">Optional retry policy for this topic. Null falls back to <see cref="TalariaOptions.DefaultRetryPolicy"/>.</param>
    /// <returns>The same <paramref name="registry"/>, for chaining.</returns>
    public static TopicRegistry MapTopic<TMessage, TConsumer>(
        this TopicRegistry registry,
        string topic,
        string consumerGroup,
        RetryPolicy? retryPolicy = null)
        where TConsumer : class, ITopicConsumer<TMessage>
    {
        return AddTopicRegistration(registry, topic, typeof(TMessage), retryPolicy, consumerGroup, typeof(TConsumer), null);
    }

    internal static TopicRegistry AddTopicRegistration(
        TopicRegistry registry,
        string topic,
        Type messageType,
        RetryPolicy? retryPolicy,
        string? consumerGroup,
        Type? consumerType,
        Func<object, MessageHeaders, EnvelopeMetadata, CancellationToken, Task>? handler)
    {
        if (retryPolicy is not null)
        {
            var validation = TalariaOptionsValidator.ValidateRetryPolicy(retryPolicy, nameof(retryPolicy));
            if (validation is not null)
            {
                throw new ArgumentException(validation.FailureMessage, nameof(retryPolicy));
            }
        }

        if (consumerType is null && handler is null)
        {
            throw new ArgumentException(
                "A topic registration must specify either a delegate handler or a class consumer type.");
        }

        if (consumerType is not null && handler is not null)
        {
            throw new ArgumentException(
                "A topic registration cannot specify both a delegate handler and a class consumer type.");
        }

        registry.Add(new TopicRegistration
        {
            TopicName = topic,
            MessageType = messageType,
            ConsumerGroup = consumerGroup,
            RetryPolicy = retryPolicy,
            ConsumerType = consumerType,
            Handler = handler,
        });
        return registry;
    }
}
