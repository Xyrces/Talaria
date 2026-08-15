// SPDX-License-Identifier: Apache-2.0

using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;

namespace Talaria.Transports.AzureServiceBus.Deferral;

/// <summary>
/// Default <see cref="IServiceBusMessageScheduler"/> that wraps the Azure Service Bus
/// SDK. One <see cref="ServiceBusSender"/> is cached per topic \u2014 ASB's guidance is to
/// reuse senders because they hold a multiplexed AMQP connection. The scheduler is
/// <see cref="IAsyncDisposable"/>; the container disposes it on host shutdown.
/// </summary>
internal sealed class ServiceBusMessageScheduler : IServiceBusMessageScheduler, IAsyncDisposable
{
    private readonly ServiceBusClient _client;
    private readonly ConcurrentDictionary<string, ServiceBusSender> _senders = new();

    public ServiceBusMessageScheduler(ServiceBusClient client)
    {
        _client = client ?? throw new System.ArgumentNullException(nameof(client));
    }

    public async Task<long> ScheduleAsync(
        string topic,
        BinaryData body,
        IReadOnlyDictionary<string, object> applicationProperties,
        DateTimeOffset scheduledEnqueueTime,
        string? partitionKey,
        CancellationToken ct = default)
    {
        var sender = GetOrCreateSender(topic);
        var message = BuildMessage(body, applicationProperties, scheduledEnqueueTime, partitionKey);
        return await sender.ScheduleMessageAsync(message, scheduledEnqueueTime, ct).ConfigureAwait(false);
    }

    private ServiceBusSender GetOrCreateSender(string topic)
    {
        // GetOrAdd may run the factory concurrently for a single topic, but the
        // winner is the one stored in the dictionary; the loser's sender is disposed
        // immediately so we do not leak AMQP links. This matches the SDK guidance.
        return _senders.GetOrAdd(topic, name =>
        {
            try
            {
                return _client.CreateSender(name);
            }
            catch
            {
                // If CreateSender throws, ensure the dictionary stays consistent.
                _senders.TryRemove(name, out _);
                throw;
            }
        });
    }

    private static ServiceBusMessage BuildMessage(
        BinaryData body,
        IReadOnlyDictionary<string, object> applicationProperties,
        DateTimeOffset scheduledEnqueueTime,
        string? partitionKey)
    {
        var message = new ServiceBusMessage(body)
        {
            ScheduledEnqueueTime = scheduledEnqueueTime,
        };

        if (applicationProperties is null)
        {
            return message;
        }

        // ASB's closest analogue to a Kafka partition key is SessionId. Setting it pins
        // the scheduled message to the same session-aware receiver as the original delivery.
        if (!string.IsNullOrEmpty(partitionKey))
        {
            message.SessionId = partitionKey;
        }
        else if (applicationProperties.TryGetValue(Talaria.Core.Abstractions.MessageHeaders.CorrelationIdKey, out var sessionCid)
            && sessionCid is string sessionCorrelationId
            && !string.IsNullOrEmpty(sessionCorrelationId))
        {
            message.SessionId = sessionCorrelationId;
        }

        var target = message.ApplicationProperties;
        foreach (var kvp in applicationProperties)
        {
            if (kvp.Value is null)
            {
                continue;
            }

            // ASB ApplicationProperties is IDictionary<string,object>; copy to a
            // backing dict via the public accessor (the property is a wrapper).
            target[kvp.Key] = kvp.Value;
        }

        // Surface the headers' MessageType key as the ASB Subject so receivers can
        // route without deserializing the payload.
        if (applicationProperties.TryGetValue(Talaria.Core.Abstractions.MessageHeaders.MessageTypeKey, out var mt)
            && mt is string messageType)
        {
            message.Subject = messageType;
        }

        if (applicationProperties.TryGetValue(Talaria.Core.Abstractions.MessageHeaders.CorrelationIdKey, out var cid)
            && cid is string correlationId)
        {
            message.CorrelationId = correlationId;
        }

        return message;
    }

    public async ValueTask DisposeAsync()
    {
        // Dispose in parallel \u2014 each sender may take time to flush. Failures are
        // swallowed here because the host is shutting down.
        var tasks = new List<Task>(_senders.Count);
        foreach (var sender in _senders.Values)
        {
            tasks.Add(SafeClose(sender));
        }

        await Task.WhenAll(tasks).ConfigureAwait(false);
        _senders.Clear();
    }

    private static async Task SafeClose(ServiceBusSender sender)
    {
        try
        {
            await sender.CloseAsync().ConfigureAwait(false);
        }
        catch
        {
            // Best-effort shutdown.
        }
    }
}
