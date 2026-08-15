// SPDX-License-Identifier: Apache-2.0

using Talaria.Core.Abstractions;

namespace Talaria.Core.Registration;

/// <summary>
/// Represents a registered topic handler — the binding between a topic name,
/// a message type, and the handler delegate.
/// </summary>
/// <since>1.0.0</since>
public sealed class TopicRegistration
{
    /// <summary>The topic name to subscribe to.</summary>
    public required string TopicName { get; init; }

    /// <summary>The CLR message type each envelope will be deserialized into.</summary>
    public required Type MessageType { get; init; }

    /// <summary>
    /// The erased async handler invoked for each delivered message. Null when
    /// <see cref="ConsumerType"/> is set and the engine resolves an <see cref="ITopicConsumer{T}"/> from DI.
    /// </summary>
    public Func<object, MessageHeaders, EnvelopeMetadata, CancellationToken, Task>? Handler { get; init; }

    /// <summary>
    /// The concrete consumer type implementing <see cref="ITopicConsumer{T}"/> for this topic.
    /// When set, the engine creates a per-message DI scope and resolves the consumer by this type.
    /// Null when a delegate <see cref="Handler"/> is registered.
    /// </summary>
    public Type? ConsumerType { get; init; }

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
                    "Call MapTopic before the host runs (e.g. during startup, before app.Run()).");
            }

            _registrations.Add(registration);
        }
    }
}
