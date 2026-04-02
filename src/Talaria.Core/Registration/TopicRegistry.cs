using Talaria.Core.Abstractions;

namespace Talaria.Core.Registration;

/// <summary>
/// Represents a registered topic handler — the binding between a topic name,
/// a message type, and the handler delegate.
/// </summary>
public sealed class TopicRegistration
{
    public required string TopicName { get; init; }
    public required Type MessageType { get; init; }
    public required Func<object, MessageHeaders, CancellationToken, Task> Handler { get; init; }
    public string? ConsumerGroup { get; init; }
}

/// <summary>
/// Registry of all topic subscriptions configured via MapTopic.
/// </summary>
public sealed class TopicRegistry
{
    private readonly List<TopicRegistration> _registrations = new();

    public IReadOnlyList<TopicRegistration> Registrations => _registrations;

    internal void Add(TopicRegistration registration)
    {
        _registrations.Add(registration);
    }
}
