using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Talaria.Core.Abstractions;
using Talaria.Core.Sagas;

namespace Talaria.Core.Registration;

/// <summary>
/// Minimal API-style extension methods for mapping message handlers.
/// </summary>
public static class TalariaEndpointExtensions
{
    /// <summary>
    /// Maps a handler delegate to a message topic, similar to app.MapGet() in Minimal APIs.
    /// The handler receives the deserialized message payload.
    /// </summary>
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
    /// Maps an envelope-aware handler to a message topic.
    /// The handler receives the full envelope with headers, trace context, etc.
    /// </summary>
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
    public static IServiceProvider MapSaga<TState>(
        this IServiceProvider services,
        Action<SagaConfigurator<TState>> configure) where TState : class, new()
    {
        var registry = services.GetRequiredService<SagaRegistry>();
        var configurator = new SagaConfigurator<TState>(registry);
        configure(configurator);
        
        return services;
    }
}
