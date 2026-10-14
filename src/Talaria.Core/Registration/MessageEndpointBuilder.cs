// SPDX-License-Identifier: Apache-2.0
using Talaria.Core.Abstractions;

namespace Talaria.Core.Registration;

/// <summary>Overrides for one explicitly mapped messaging endpoint.</summary>
public sealed class MessageEndpointBuilder
{
    private readonly TopicRegistry _registry;
    private TopicRegistration _registration;
    private readonly string _defaultGroup;
    internal MessageEndpointBuilder(TopicRegistry registry, TopicRegistration registration)
    {
        _registry = registry;
        _registration = registration;
        _defaultGroup = registration.ConsumerGroup!;
    }

    public MessageEndpointBuilder WithName(string name)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        if (_registration.EntityKind == TopologyEntityKind.Queue)
            throw new InvalidOperationException("Commands have one shared consumer identity. Use WithDestination to route a command, or MapEvent for independent subscribers.");
        return Update(_registration with { ConsumerGroup = MessageNames.Destination(_defaultGroup + "." + name) });
    }

    public MessageEndpointBuilder WithDestination(string destination)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(destination);
        return Update(_registration with
        {
            TopicName = destination,
            ConsumerGroup = _registration.EntityKind == TopologyEntityKind.Queue
                ? "command." + MessageNames.Destination(destination) : _registration.ConsumerGroup,
        });
    }

    public MessageEndpointBuilder WithRetries(int attempts)
        => WithRetryPolicy(new RetryPolicy
        {
            MaxRetryAttempts = attempts,
            RetryInterval = TimeSpan.FromSeconds(1),
            BackoffType = RetryBackoffType.Exponential,
            MaxRetryInterval = TimeSpan.FromSeconds(30),
            UseJitter = true,
        });

    /// <summary>Commit business changes, received-message completion, and outgoing messages together.</summary>
    public MessageEndpointBuilder WithTransaction() => Update(_registration with { Transactional = true });

    public MessageEndpointBuilder WithRetryPolicy(RetryPolicy policy)
    {
        ArgumentNullException.ThrowIfNull(policy);
        var invalid = TalariaOptionsValidator.ValidateRetryPolicy(policy, nameof(policy));
        if (invalid is not null) throw new ArgumentException(invalid.FailureMessage, nameof(policy));
        return Update(_registration with { RetryPolicy = policy });
    }

    private MessageEndpointBuilder Update(TopicRegistration next)
    {
        _registry.Replace(_registration, next);
        _registration = next;
        return this;
    }
}
