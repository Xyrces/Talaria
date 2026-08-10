// SPDX-License-Identifier: AGPL-3.0-or-later

using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Talaria.Core.Abstractions;
using Talaria.Core.Sagas;

namespace Talaria.Core.Registration;

/// <summary>
/// Minimal API-style extension methods for mapping message handlers.
/// </summary>
/// <since>1.0.0</since>
public static class TalariaEndpointExtensions
{
    /// <summary>
    /// Maps a handler delegate to a message topic, similar to app.MapGet() in Minimal APIs.
    /// The handler receives the deserialized message payload.
    /// </summary>
    /// <typeparam name="T">The CLR message type to deserialize from each envelope.</typeparam>
    /// <param name="services">The application's service provider.</param>
    /// <param name="topic">The topic name to subscribe to.</param>
    /// <param name="handler">Async handler invoked for each delivered message.</param>
    /// <returns>The same <paramref name="services"/>, for chaining.</returns>
    public static IServiceProvider MapTopic<T>(
        this IServiceProvider services,
        string topic,
        Func<T, CancellationToken, Task> handler)
    {
        var registry = services.GetRequiredService<TopicRegistry>();
        registry.Add(new TopicRegistration
        {
            TopicName = topic,
            MessageType = typeof(T),
            Handler = async (payload, _, ct) => await handler((T)payload, ct),
        });
        return services;
    }

    /// <summary>
    /// Maps a handler delegate to a message topic with an explicit consumer group,
    /// similar to app.MapGet() in Minimal APIs.
    /// The handler receives the deserialized message payload.
    /// </summary>
    /// <typeparam name="T">The CLR message type to deserialize from each envelope.</typeparam>
    /// <param name="services">The application's service provider.</param>
    /// <param name="topic">The topic name to subscribe to.</param>
    /// <param name="consumerGroup">The consumer group identifier. Overrides <see cref="TalariaOptions.ConsumerGroupOverride"/>.</param>
    /// <param name="handler">Async handler invoked for each delivered message.</param>
    /// <returns>The same <paramref name="services"/>, for chaining.</returns>
    public static IServiceProvider MapTopic<T>(
        this IServiceProvider services,
        string topic,
        string consumerGroup,
        Func<T, CancellationToken, Task> handler)
    {
        var registry = services.GetRequiredService<TopicRegistry>();
        registry.Add(new TopicRegistration
        {
            TopicName = topic,
            MessageType = typeof(T),
            ConsumerGroup = consumerGroup,
            Handler = async (payload, _, ct) => await handler((T)payload, ct),
        });
        return services;
    }

    /// <summary>
    /// Maps an envelope-aware handler to a message topic.
    /// The handler receives the full envelope with headers, trace context, etc.
    /// </summary>
    /// <typeparam name="T">The CLR message type to deserialize from each envelope.</typeparam>
    /// <param name="services">The application's service provider.</param>
    /// <param name="topic">The topic name to subscribe to.</param>
    /// <param name="handler">Async handler invoked for each delivered message.</param>
    /// <returns>The same <paramref name="services"/>, for chaining.</returns>
    /// <remarks>
    /// Use this overload when the handler must inspect headers (e.g. read trace context,
    /// propagate baggage) or the source topic / partition metadata.
    /// </remarks>
    public static IServiceProvider MapTopicWithEnvelope<T>(
        this IServiceProvider services,
        string topic,
        Func<MessageEnvelope<T>, CancellationToken, Task> handler)
    {
        var registry = services.GetRequiredService<TopicRegistry>();
        registry.Add(new TopicRegistration
        {
            TopicName = topic,
            MessageType = typeof(T),
            Handler = async (payload, headers, ct) =>
            {
                var envelope = new MessageEnvelope<T>
                {
                    Payload = (T)payload,
                    Headers = headers,
                    SourceTopic = topic,
                };
                await handler(envelope, ct);
            },
        });
        return services;
    }

    /// <summary>
    /// Maps a synchronous handler to a message topic.
    /// </summary>
    /// <typeparam name="T">The CLR message type to deserialize from each envelope.</typeparam>
    /// <param name="services">The application's service provider.</param>
    /// <param name="topic">The topic name to subscribe to.</param>
    /// <param name="handler">Synchronous handler invoked for each delivered message.</param>
    /// <returns>The same <paramref name="services"/>, for chaining.</returns>
    public static IServiceProvider MapTopic<T>(
        this IServiceProvider services,
        string topic,
        Action<T> handler)
    {
        return services.MapTopic<T>(topic, (msg, _) =>
        {
            handler(msg);
            return Task.CompletedTask;
        });
    }

    /// <summary>
    /// Configures a saga workflow.
    /// </summary>
    /// <typeparam name="TState">The CLR saga state type. Must be a reference type with a public parameterless constructor.</typeparam>
    /// <param name="services">The application's service provider.</param>
    /// <param name="configure">A callback that uses <see cref="SagaConfigurator{TState}"/> to declare the saga's steps and dispatch routes.</param>
    /// <returns>The same <paramref name="services"/>, for chaining.</returns>
    /// <remarks>
    /// The registration is added to the <see cref="SagaRegistry"/> only after the
    /// configure callback returns normally; a throwing callback leaves nothing
    /// registered. Call <see cref="SagaConfigurator{TState}.DispatchTo{TMessage}"/>
    /// for every message type any step dispatches — the engine throws at dispatch time
    /// when a dispatched type has no mapping.
    /// </remarks>
    public static IServiceProvider MapSaga<TState>(
        this IServiceProvider services,
        Action<SagaConfigurator<TState>> configure) where TState : class, new()
    {
        var registry = services.GetRequiredService<SagaRegistry>();
        var configurator = new SagaConfigurator<TState>(registry);
        configure(configurator);
        configurator.Complete();

        return services;
    }
}
