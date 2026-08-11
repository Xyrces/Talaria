// SPDX-License-Identifier: AGPL-3.0-or-later

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

    /// <summary>The erased async handler invoked for each delivered message.</summary>
    public required Func<object, MessageHeaders, CancellationToken, Task> Handler { get; init; }

    /// <summary>Optional explicit consumer group. Null falls back to <see cref="TalariaOptions.ConsumerGroupOverride"/> then auto-generated.</summary>
    public string? ConsumerGroup { get; init; }
}

/// <summary>
/// Registry of all topic subscriptions configured via MapTopic.
/// </summary>
/// <since>1.0.0</since>
public sealed class TopicRegistry
{
    private readonly List<TopicRegistration> _registrations = new();

    /// <summary>The registrations added so far, in insertion order.</summary>
    public IReadOnlyList<TopicRegistration> Registrations => _registrations;

    internal void Add(TopicRegistration registration)
    {
        _registrations.Add(registration);
    }
}
