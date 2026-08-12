// SPDX-License-Identifier: AGPL-3.0-or-later

using Azure.Messaging.ServiceBus;

namespace Talaria.Transports.AzureServiceBus;

/// <summary>
/// Configuration options for the Talaria Azure Service Bus transport.
/// </summary>
/// <remarks>
/// All defaults are tuned for the on-boarding saga sample: a single namespace,
/// short locks so consumer restarts don't redeliver excessively, and a small
/// prefetch that keeps the in-process pump bounded. Override per deployment
/// via the configuration callback passed to
/// <c>UseAzureServiceBusTransport(Action&lt;AzureServiceBusTransportOptions&gt;)</c>.
/// </remarks>
/// <since>1.0.0</since>
public sealed class AzureServiceBusTransportOptions
{
    /// <summary>
    /// Fully-qualified Service Bus namespace (e.g. <c>mysb.servicebus.windows.net</c>).
    /// Required when <see cref="ConnectionString"/> is not supplied.
    /// </summary>
    public string? FullyQualifiedNamespace { get; set; }

    /// <summary>
    /// Connection string copied from the Service Bus namespace's "Shared access
    /// policies" blade. Mutually exclusive with <see cref="FullyQualifiedNamespace"/> +
    /// credential wiring; supplying a connection string is the simplest path for
    /// local development and the saga sample.
    /// </summary>
    public string? ConnectionString { get; set; }

    /// <summary>
    /// Suffix appended to a topic or queue name to form its dead-letter entity
    /// (queue or topic). Defaults to <c>.dlq</c> to mirror the Kafka transport's
    /// DLQ naming convention. The DLQ entity itself is the host's responsibility
    /// to provision.
    /// </summary>
    public string DlqSuffix { get; set; } = ".dlq";

    /// <summary>
    /// Maximum number of concurrent messages a <see cref="ServiceBusProcessor"/>
    /// pulls into the in-process pump. Larger values trade latency for memory
    /// and are bounded by ASB's per-processor cap (default in SDK is 1; the
    /// transport explicitly widens this).
    /// </summary>
    public int PrefetchCount { get; set; } = 10;

    /// <summary>
    /// Peek-lock duration handed to the broker for each received message. When
    /// the host calls <c>CompleteMessageAsync</c> within this window the
    /// message is acknowledged; expiry without completion triggers redelivery.
    /// </summary>
    public TimeSpan LockDuration { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Maximum retry attempts the <see cref="ServiceBusProcessor"/> performs
    /// before forwarding a message to the transport's DLQ. The transport does
    /// not rely on broker-side retries for handler failures — engine-level
    /// dead-lettering via <see cref="Talaria.Core.Abstractions.IConsumer{T}.NackAsync"/>
    /// remains the primary path.
    /// </summary>
    public int MaxRetries { get; set; } = 3;

    /// <summary>
    /// Capacity of the in-process bounded channel between the Service Bus
    /// processor pump thread and the enumerator returned by
    /// the consumer enumeration. Larger values trade memory for
    /// smoother backpressure when the host falls behind.
    /// </summary>
    public int BufferCapacity { get; set; } = 100;
}
