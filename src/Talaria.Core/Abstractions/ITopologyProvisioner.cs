// SPDX-License-Identifier: Apache-2.0

namespace Talaria.Core.Abstractions;

/// <summary>
/// Kind of transport entity a <see cref="TopologyDeclaration"/> describes.
/// </summary>
/// <remarks>
/// Some transports (Kafka) auto-create entities on first publish/subscribe and
/// treat the topic/partition as the routing primitive. Others (Azure Service
/// Bus) require explicit provisioning of queues, topics, and topic
/// subscriptions before any traffic can flow, with separate per-entity
/// settings for lock duration, max delivery count, and DLQ forwarding.
/// <see cref="ITopologyProvisioner"/> exposes the explicit-provisioning
/// model so the same engine can drive both transport families.
/// </remarks>
/// <since>1.0.0</since>
public enum TopologyEntityKind
{
    /// <summary>
    /// A competing-consumer queue (Azure Service Bus queue, SQS, etc.).
    /// </summary>
    Queue = 0,

    /// <summary>
    /// A pub/sub topic (Azure Service Bus topic, SNS topic, etc.).
    /// </summary>
    Topic = 1,

    /// <summary>
    /// A subscription on a topic that fans messages out to a competing
    /// consumer group (Azure Service Bus subscription, etc.).
    /// </summary>
    Subscription = 2,
}

/// <summary>
/// Declarative description of a single transport entity that
/// <see cref="ITopologyProvisioner.ProvisionAsync"/> should create or update.
/// </summary>
/// <param name="Kind">What kind of entity to provision.</param>
/// <param name="Name">
/// Entity name. For <see cref="TopologyEntityKind.Subscription"/> the name is
/// typically the same as the consumer group that will receive the messages.
/// </param>
/// <param name="ParentName">
/// Parent entity name (topic for a subscription). Null for queues/topics.
/// </param>
/// <param name="LockDuration">
/// Visibility-timeout / peek-lock duration. Null leaves the transport
/// default in effect.
/// </param>
/// <param name="MaxDeliveryCount">
/// Maximum delivery attempts before the transport routes the message to its
/// native dead-letter queue. Null disables the limit (rely on the host's
/// MaxHopCount instead).
/// </param>
/// <param name="EnableDeadLettering">
/// When true, the entity forwards to a native dead-letter queue on
/// expiration or max-delivery. Null defers to the transport default.
/// </param>
/// <param name="AutoDeleteOnIdle">
/// Optional idle timeout after which the transport may delete the entity.
/// Null disables auto-deletion.
/// </param>
/// <since>1.0.0</since>
public sealed record TopologyDeclaration(
    TopologyEntityKind Kind,
    string Name,
    string? ParentName = null,
    TimeSpan? LockDuration = null,
    int? MaxDeliveryCount = null,
    bool? EnableDeadLettering = null,
    TimeSpan? AutoDeleteOnIdle = null);

/// <summary>
/// Explicit provisioning surface for transport entities (queues, topics,
/// subscriptions). Hosts that target explicit-provisioning transports
/// (Azure Service Bus, Cloudflare Pub/Sub, etc.) call
/// <see cref="ProvisionAsync"/> at startup to declare the entities their
/// saga topology references.
/// </summary>
/// <remarks>
/// The provisioner is transport-specific: an Azure Service Bus
/// implementation creates queues, topics, and subscriptions through the
/// service bus management client; an SQS implementation creates queues; a
/// Kafka implementation typically surfaces this interface as a no-op
/// because Kafka auto-creates topics on first use. The interface exists so
/// the host can declare its required topology once, in a transport-agnostic
/// way, and rely on the registered implementation to translate the
/// declarations into transport-native calls. Calling
/// <see cref="ProvisionAsync"/> is idempotent — declarations describe the
/// desired end state, not the diff, so a host may safely re-declare its
/// full topology on every startup.
/// </remarks>
/// <since>1.0.0</since>
public interface ITopologyProvisioner
{
    /// <summary>
    /// Creates or updates the entities described by <paramref name="declarations"/>.
    /// Implementations should be idempotent: re-declaring an existing entity
    /// with identical settings is a no-op, and re-declaring with changed
    /// settings updates the entity in place.
    /// </summary>
    /// <param name="declarations">
    /// The entities to ensure exist. May be empty — the call is then a
    /// no-op.
    /// </param>
    /// <param name="ct">Cancellation token.</param>
    Task ProvisionAsync(
        IEnumerable<TopologyDeclaration> declarations,
        CancellationToken ct = default);
}
