// SPDX-License-Identifier: Apache-2.0

using System.Collections.Generic;
using Talaria.Core.Abstractions;

namespace Talaria.Transports.AzureServiceBus;

/// <summary>
/// Shared ASB routing rule: the broker's closest analogue to a Kafka partition key is
/// <see cref="Azure.Messaging.ServiceBus.ServiceBusMessage.SessionId"/>. The engine
/// prefers an explicit partition key, but falls back to the correlation id header so
/// session-aware entities keep affinity for saga-correlated messages even when no
/// partition key was supplied.
/// </summary>
internal static class ServiceBusMessageSessionIdHelper
{
    /// <summary>
    /// Resolves the SessionId from an explicit partition key and a header bag.
    /// </summary>
    /// <param name="partitionKey">Optional explicit partition key.</param>
    /// <param name="headers">Message headers, which may contain a correlation id.</param>
    /// <returns>The SessionId to use, or <c>null</c> when neither source is present.</returns>
    public static string? ResolveSessionId(string? partitionKey, MessageHeaders headers)
    {
        if (!string.IsNullOrEmpty(partitionKey))
        {
            return partitionKey;
        }

        if (headers.TryGetValue(MessageHeaders.CorrelationIdKey, out var correlationId)
            && !string.IsNullOrEmpty(correlationId))
        {
            return correlationId;
        }

        return null;
    }

    /// <summary>
    /// Resolves the SessionId from an explicit partition key and a flat application-properties
    /// bag (used by the broker-side scheduler, which receives headers already flattened).
    /// </summary>
    /// <param name="partitionKey">Optional explicit partition key.</param>
    /// <param name="applicationProperties">Flattened header/properties bag.</param>
    /// <returns>The SessionId to use, or <c>null</c> when neither source is present.</returns>
    public static string? ResolveSessionId(string? partitionKey, IReadOnlyDictionary<string, object> applicationProperties)
    {
        if (!string.IsNullOrEmpty(partitionKey))
        {
            return partitionKey;
        }

        if (applicationProperties is not null
            && applicationProperties.TryGetValue(MessageHeaders.CorrelationIdKey, out var correlationIdObj)
            && correlationIdObj is string correlationId
            && !string.IsNullOrEmpty(correlationId))
        {
            return correlationId;
        }

        return null;
    }
}
