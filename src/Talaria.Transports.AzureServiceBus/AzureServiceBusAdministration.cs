// SPDX-License-Identifier: Apache-2.0

using Azure.Messaging.ServiceBus.Administration;

namespace Talaria.Transports.AzureServiceBus;

/// <summary>Internal management seam for deterministic provisioning race tests.</summary>
internal interface IServiceBusAdministration
{
    Task<bool> QueueExistsAsync(string name, CancellationToken ct);
    Task CreateQueueAsync(CreateQueueOptions options, CancellationToken ct);
    Task<bool> TopicExistsAsync(string name, CancellationToken ct);
    Task CreateTopicAsync(string name, CancellationToken ct);
    Task<bool> SubscriptionExistsAsync(string topic, string subscription, CancellationToken ct);
    Task CreateSubscriptionAsync(CreateSubscriptionOptions options, CancellationToken ct);
}

internal sealed class ServiceBusAdministrationAdapter(ServiceBusAdministrationClient client) : IServiceBusAdministration
{
    public async Task<bool> QueueExistsAsync(string name, CancellationToken ct)
        => (await client.QueueExistsAsync(name, ct).ConfigureAwait(false)).Value;

    public async Task CreateQueueAsync(CreateQueueOptions options, CancellationToken ct)
        => await client.CreateQueueAsync(options, ct).ConfigureAwait(false);

    public async Task<bool> TopicExistsAsync(string name, CancellationToken ct)
        => (await client.TopicExistsAsync(name, ct).ConfigureAwait(false)).Value;

    public async Task CreateTopicAsync(string name, CancellationToken ct)
        => await client.CreateTopicAsync(name, ct).ConfigureAwait(false);

    public async Task<bool> SubscriptionExistsAsync(string topic, string subscription, CancellationToken ct)
        => (await client.SubscriptionExistsAsync(topic, subscription, ct).ConfigureAwait(false)).Value;

    public async Task CreateSubscriptionAsync(CreateSubscriptionOptions options, CancellationToken ct)
        => await client.CreateSubscriptionAsync(options, ct).ConfigureAwait(false);
}
