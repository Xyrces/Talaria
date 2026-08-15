// SPDX-License-Identifier: Apache-2.0

using Talaria.Core.Abstractions;

namespace Talaria.Core.Registration;

/// <summary>
/// Transport envelope metadata passed alongside the payload and headers when a
/// topic handler is registered with <see cref="TopicRegistryExtensions.MapTopicWithEnvelope{T}"/>
/// or <see cref="TalariaEndpointExtensions.MapTopicWithEnvelope{T}"/>.
/// This lets envelope-aware handlers reconstruct a <see cref="MessageEnvelope{T}"/>
/// with full fidelity without exposing transport-specific types.
/// </summary>
/// <since>1.0.0</since>
public sealed record EnvelopeMetadata(
    string? PartitionKey,
    int? Partition,
    long Offset,
    DateTimeOffset Timestamp,
    string? CorrelationId)
{
    /// <summary>
    /// An empty metadata instance for tests and infrastructure code that need a placeholder.
    /// The consumer engines always construct real <see cref="EnvelopeMetadata"/> values
    /// for actual deliveries; plain <c>MapTopic</c> handlers simply receive defaults.
    /// </summary>
    public static EnvelopeMetadata Empty { get; } = new(null, null, 0, default, null);
}
