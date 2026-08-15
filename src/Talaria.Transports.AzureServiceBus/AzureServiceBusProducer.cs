// SPDX-License-Identifier: Apache-2.0

using System.Text;
using System.Text.Json;
using Azure.Messaging.ServiceBus;
using Talaria.Core.Abstractions;

namespace Talaria.Transports.AzureServiceBus;

/// <summary>
/// Azure Service Bus producer that publishes JSON-serialized messages to a
/// queue or topic. The producer wraps a shared <see cref="ServiceBusSender"/>
/// — ASB's guidance is to reuse senders because they multiplex one AMQP
/// connection.
/// <para>
/// Engine-owned headers (MessageId, HopCount, MessageType, trace context) are
/// stamped on every produce, mirroring the Talaria Kafka producer
/// so the consumer pipeline sees a consistent envelope across transports.
/// </para>
/// </summary>
/// <since>1.0.0</since>
internal sealed class AzureServiceBusProducer<T> : IProducer<T>
{
    private readonly ServiceBusSender _sender;
    private readonly string _topic;

    /// <summary>
    /// Wraps a sender targeting <paramref name="topic"/>. The caller owns the
    /// sender's lifecycle — it lives on the transport's sender cache for the
    /// lifetime of the host.
    /// </summary>
    public AzureServiceBusProducer(ServiceBusSender sender, string topic)
    {
        _sender = sender ?? throw new ArgumentNullException(nameof(sender));
        _topic = topic ?? throw new ArgumentNullException(nameof(topic));
    }

    /// <inheritdoc />
    public async Task ProduceAsync(
        T message,
        MessageHeaders? headers = null,
        string? partitionKey = null,
        CancellationToken ct = default)
    {
        // Clone incoming headers: never mutate or store the caller's instance.
        var finalHeaders = headers is null ? new MessageHeaders() : new MessageHeaders(headers);

        if (System.Diagnostics.Activity.Current != null && string.IsNullOrEmpty(finalHeaders.TraceParent))
        {
            finalHeaders.TraceParent = System.Diagnostics.Activity.Current.Id;
            finalHeaders.TraceState = System.Diagnostics.Activity.Current.TraceStateString;
        }

        if (string.IsNullOrEmpty(finalHeaders.MessageId))
        {
            finalHeaders.MessageId = Guid.NewGuid().ToString("N");
        }

        // Engine-owned routing metadata: the CLR type of the payload, used by
        // consumers that fan a topic out to multiple typed handlers.
        finalHeaders[MessageHeaders.MessageTypeKey] = typeof(T).FullName ?? typeof(T).Name;

        // Engine-owned hop counter: fresh messages start at 0; forwarded
        // messages (already carrying a count) are incremented so cyclic flows
        // trip the max-hop guard.
        if (finalHeaders.ContainsKey(MessageHeaders.HopCountKey))
        {
            finalHeaders.HopCount = finalHeaders.HopCount + 1;
        }

        var payloadBytes = JsonSerializer.SerializeToUtf8Bytes(message);
        var sbMessage = new ServiceBusMessage(payloadBytes)
        {
            MessageId = finalHeaders.MessageId,
            ContentType = "application/json",
        };

        // ASB's closest analogue to a Kafka partition key is SessionId — it
        // pins all messages with the same session to the same receiver when
        // sessions are enabled on the entity. Setting it here is harmless when
        // sessions are disabled.
        if (!string.IsNullOrEmpty(partitionKey))
        {
            sbMessage.SessionId = partitionKey;
        }
        else
        {
            var correlationId = finalHeaders.TryGetValue(MessageHeaders.CorrelationIdKey, out var cid) ? cid : null;
            if (!string.IsNullOrEmpty(correlationId))
            {
                sbMessage.SessionId = correlationId;
            }
        }

        if (finalHeaders.TryGetValue(MessageHeaders.CorrelationIdKey, out var corrId) && !string.IsNullOrEmpty(corrId))
        {
            sbMessage.CorrelationId = corrId;
        }

        // Surface the CLR message type so receivers can route without
        // deserializing the payload (matches the KafkaProducer convention).
        if (finalHeaders.TryGetValue(MessageHeaders.MessageTypeKey, out var mt) && !string.IsNullOrEmpty(mt))
        {
            sbMessage.Subject = mt;
        }

        var applicationProperties = sbMessage.ApplicationProperties;
        foreach (var header in finalHeaders)
        {
            if (header.Value is null)
            {
                // ASB ApplicationProperties does not accept null values; null
                // is an engine convention (e.g. "no correlation id yet") so
                // we drop rather than coerce.
                continue;
            }

            applicationProperties[header.Key] = header.Value;
        }

        if (System.Diagnostics.Activity.Current != null)
        {
            System.Diagnostics.Activity.Current.SetTag("messaging.destination.name", _topic);
            System.Diagnostics.Activity.Current.SetTag("messaging.system", "azure-service-bus");
        }

        await _sender.SendMessageAsync(sbMessage, ct).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        // Sender is disposed via the transport-level sender cache when the
        // host shuts down — disposing here would prematurely tear down
        // senders shared with sibling producers.
        return ValueTask.CompletedTask;
    }
}
