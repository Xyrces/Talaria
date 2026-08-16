// SPDX-License-Identifier: Apache-2.0

using Talaria.Core.Abstractions;

namespace Talaria.Core.Registration;

/// <summary>
/// Represents a registered topic subscription — the binding between a topic name,
/// a message type, and either a handler delegate or a class-based consumer type.
/// </summary>
/// <remarks>
/// Exactly one of <see cref="Handler"/>, <see cref="ConsumerType"/>, <see cref="RequestHandler"/>,
/// or <see cref="RequestConsumerType"/> must be set. The <c>MapTopic</c> and <c>MapRequest</c>
/// overloads in <see cref="TopicRegistryExtensions"/> enforce this invariant.
/// </remarks>
/// <since>1.0.0</since>
public sealed class TopicRegistration
{
    /// <summary>The topic name to subscribe to.</summary>
    public required string TopicName { get; init; }

    /// <summary>The CLR message type each envelope will be deserialized into.</summary>
    public required Type MessageType { get; init; }

    /// <summary>
    /// The erased async handler invoked for each delivered message. Null when
    /// <see cref="ConsumerType"/> is set and the engine resolves an <see cref="ITopicConsumer{T}"/> from DI,
    /// or when <see cref="RequestHandler"/> is set and the engine resolves an <see cref="IRequestConsumer{TRequest, TResponse}"/>.
    /// </summary>
    public Func<object, MessageHeaders, EnvelopeMetadata, CancellationToken, Task>? Handler { get; init; }

    /// <summary>
    /// The concrete consumer type implementing <see cref="ITopicConsumer{T}"/> for this topic.
    /// When set, the engine creates a per-message DI scope and resolves the consumer by this type.
    /// Null when a delegate <see cref="Handler"/> is registered.
    /// </summary>
    public Type? ConsumerType { get; init; }

    /// <summary>
    /// The erased async request handler invoked for each delivered request message.
    /// When set, the engine publishes the returned response to the topic named in
    /// the <c>talaria.reply_to</c> header. Mutually exclusive with <see cref="Handler"/>
    /// and <see cref="ConsumerType"/>.
    /// </summary>
    public Func<object, MessageHeaders, EnvelopeMetadata, CancellationToken, Task<object>>? RequestHandler { get; init; }

    /// <summary>
    /// The concrete consumer type implementing <see cref="IRequestConsumer{TRequest, TResponse}"/> for this topic.
    /// When set, the engine creates a per-message DI scope, resolves the consumer by this type, and
    /// publishes the returned response. Mutually exclusive with <see cref="Handler"/>,
    /// <see cref="ConsumerType"/>, and <see cref="RequestHandler"/>.
    /// </summary>
    public Type? RequestConsumerType { get; init; }

    /// <summary>The CLR response type produced by this request handler, when this is a request registration.</summary>
    public Type? ResponseType { get; init; }

    /// <summary>Optional explicit consumer group. Null falls back to <see cref="TalariaOptions.ConsumerGroupOverride"/> then auto-generated.</summary>
    public string? ConsumerGroup { get; init; }

    /// <summary>Optional retry policy for this topic. Null falls back to <see cref="TalariaOptions.DefaultRetryPolicy"/>.</summary>
    public RetryPolicy? RetryPolicy { get; init; }
}

/// <summary>
/// Registry of all topic subscriptions configured via MapTopic.
/// </summary>
/// <since>1.0.0</since>
public sealed class TopicRegistry
{
    private readonly List<TopicRegistration> _registrations = new();
    private readonly object _lock = new();
    private bool _sealed;

    /// <summary>True if the registry has been sealed and no further registrations can be added.</summary>
    public bool IsSealed
    {
        get
        {
            lock (_lock)
            {
                return _sealed;
            }
        }
    }

    /// <summary>The registrations added so far, in insertion order.</summary>
    public IReadOnlyList<TopicRegistration> Registrations
    {
        get
        {
            lock (_lock)
            {
                return _registrations.ToList();
            }
        }
    }

    /// <summary>
    /// Seals the registry so no further topic registrations can be added.
    /// Idempotent: subsequent calls have no effect.
    /// </summary>
    internal void Seal()
    {
        lock (_lock)
        {
            _sealed = true;
        }
    }

    internal void Add(TopicRegistration registration)
    {
        lock (_lock)
        {
            if (_sealed)
            {
                throw new InvalidOperationException(
                    "Topic registrations are captured when the host starts. " +
                    "Call MapTopic/MapRequest before the host runs (e.g. during startup, before app.Run()).");
            }

            // Plain multi-registration per topic is existing fan-out behavior and is preserved.
            // Request/response registrations, however, own the consumer loop for the topic and
            // cannot coexist with any other registration for the same topic.
            var isRequestRegistration = registration.RequestHandler is not null || registration.RequestConsumerType is not null;
            var hasExistingForTopic = _registrations.Any(r => r.TopicName == registration.TopicName);
            var hasExistingRequestForTopic = _registrations.Any(r =>
                r.TopicName == registration.TopicName &&
                (r.RequestHandler is not null || r.RequestConsumerType is not null));

            if (hasExistingForTopic && (isRequestRegistration || hasExistingRequestForTopic))
            {
                throw new InvalidOperationException(
                    $"Topic '{registration.TopicName}' already has a registration. " +
                    "A topic cannot have both plain and request/response registrations, or multiple request/response registrations.");
            }

            _registrations.Add(registration);
        }
    }
}
