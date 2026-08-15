// SPDX-License-Identifier: Apache-2.0

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
    /// <param name="retryPolicy">Optional retry policy for this topic. Null falls back to <see cref="TalariaOptions.DefaultRetryPolicy"/>.</param>
    /// <returns>The same <paramref name="services"/>, for chaining.</returns>
    public static IServiceProvider MapTopic<T>(
        this IServiceProvider services,
        string topic,
        Func<T, CancellationToken, Task> handler,
        RetryPolicy? retryPolicy = null)
    {
        var registry = services.GetRequiredService<TopicRegistry>();
        registry.MapTopic(topic, handler, retryPolicy);
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
    /// <param name="retryPolicy">Optional retry policy for this topic. Null falls back to <see cref="TalariaOptions.DefaultRetryPolicy"/>.</param>
    /// <returns>The same <paramref name="services"/>, for chaining.</returns>
    public static IServiceProvider MapTopic<T>(
        this IServiceProvider services,
        string topic,
        string consumerGroup,
        Func<T, CancellationToken, Task> handler,
        RetryPolicy? retryPolicy = null)
    {
        var registry = services.GetRequiredService<TopicRegistry>();
        registry.MapTopic(topic, consumerGroup, handler, retryPolicy);
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
    /// <param name="retryPolicy">Optional retry policy for this topic. Null falls back to <see cref="TalariaOptions.DefaultRetryPolicy"/>.</param>
    /// <returns>The same <paramref name="services"/>, for chaining.</returns>
    /// <remarks>
    /// Use this overload when the handler must inspect headers (e.g. read trace context,
    /// propagate baggage) or the source topic / partition metadata.
    /// </remarks>
    public static IServiceProvider MapTopicWithEnvelope<T>(
        this IServiceProvider services,
        string topic,
        Func<MessageEnvelope<T>, CancellationToken, Task> handler,
        RetryPolicy? retryPolicy = null)
    {
        var registry = services.GetRequiredService<TopicRegistry>();
        registry.MapTopicWithEnvelope(topic, handler, retryPolicy);
        return services;
    }

    /// <summary>
    /// Maps a synchronous handler to a message topic.
    /// </summary>
    /// <typeparam name="T">The CLR message type to deserialize from each envelope.</typeparam>
    /// <param name="services">The application's service provider.</param>
    /// <param name="topic">The topic name to subscribe to.</param>
    /// <param name="handler">Synchronous handler invoked for each delivered message.</param>
    /// <param name="retryPolicy">Optional retry policy for this topic. Null falls back to <see cref="TalariaOptions.DefaultRetryPolicy"/>.</param>
    /// <returns>The same <paramref name="services"/>, for chaining.</returns>
    public static IServiceProvider MapTopic<T>(
        this IServiceProvider services,
        string topic,
        Action<T> handler,
        RetryPolicy? retryPolicy = null)
    {
        var registry = services.GetRequiredService<TopicRegistry>();
        registry.MapTopic(topic, handler, retryPolicy);
        return services;
    }

    /// <summary>
    /// Maps a class-based consumer to a message topic. The consumer is resolved from
    /// a per-message DI scope by its concrete type <typeparamref name="TConsumer"/>.
    /// </summary>
    /// <typeparam name="TMessage">The CLR message type to deserialize from each envelope.</typeparam>
    /// <typeparam name="TConsumer">The concrete consumer type implementing <see cref="ITopicConsumer{TMessage}"/>.</typeparam>
    /// <param name="services">The application's service provider.</param>
    /// <param name="topic">The topic name to subscribe to.</param>
    /// <param name="retryPolicy">Optional retry policy for this topic. Null falls back to <see cref="TalariaOptions.DefaultRetryPolicy"/>.</param>
    /// <returns>The same <paramref name="services"/>, for chaining.</returns>
    /// <exception cref="InvalidOperationException">The <typeparamref name="TConsumer"/> is not registered in the service provider and <see cref="IServiceProviderIsService"/> is available.</exception>
    public static IServiceProvider MapTopic<TMessage, TConsumer>(
        this IServiceProvider services,
        string topic,
        RetryPolicy? retryPolicy = null)
        where TConsumer : class, ITopicConsumer<TMessage>
    {
        return MapTopicCore<TMessage, TConsumer>(services, topic, null, retryPolicy);
    }

    /// <summary>
    /// Maps a class-based consumer to a message topic with an explicit consumer group.
    /// The consumer is resolved from a per-message DI scope by its concrete type <typeparamref name="TConsumer"/>.
    /// </summary>
    /// <typeparam name="TMessage">The CLR message type to deserialize from each envelope.</typeparam>
    /// <typeparam name="TConsumer">The concrete consumer type implementing <see cref="ITopicConsumer{TMessage}"/>.</typeparam>
    /// <param name="services">The application's service provider.</param>
    /// <param name="topic">The topic name to subscribe to.</param>
    /// <param name="consumerGroup">The consumer group identifier. Overrides <see cref="TalariaOptions.ConsumerGroupOverride"/>.</param>
    /// <param name="retryPolicy">Optional retry policy for this topic. Null falls back to <see cref="TalariaOptions.DefaultRetryPolicy"/>.</param>
    /// <returns>The same <paramref name="services"/>, for chaining.</returns>
    /// <exception cref="InvalidOperationException">The <typeparamref name="TConsumer"/> is not registered in the service provider and <see cref="IServiceProviderIsService"/> is available.</exception>
    public static IServiceProvider MapTopic<TMessage, TConsumer>(
        this IServiceProvider services,
        string topic,
        string consumerGroup,
        RetryPolicy? retryPolicy = null)
        where TConsumer : class, ITopicConsumer<TMessage>
    {
        return MapTopicCore<TMessage, TConsumer>(services, topic, consumerGroup, retryPolicy);
    }

    private static IServiceProvider MapTopicCore<TMessage, TConsumer>(
        IServiceProvider services,
        string topic,
        string? consumerGroup,
        RetryPolicy? retryPolicy)
        where TConsumer : class, ITopicConsumer<TMessage>
    {
        var isService = services.GetService<IServiceProviderIsService>();
        if (isService is not null && !isService.IsService(typeof(TConsumer)))
        {
            throw new InvalidOperationException(
                $"Consumer '{typeof(TConsumer).FullName}' is not registered in the service provider. " +
                "Register it before calling MapTopic, e.g. services.AddScoped<TConsumer>().");
        }

        var registry = services.GetRequiredService<TopicRegistry>();
        if (consumerGroup is null)
        {
            registry.MapTopic<TMessage, TConsumer>(topic, retryPolicy);
        }
        else
        {
            registry.MapTopic<TMessage, TConsumer>(topic, consumerGroup, retryPolicy);
        }

        return services;
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
        registry.MapSaga(configure);

        return services;
    }
}
