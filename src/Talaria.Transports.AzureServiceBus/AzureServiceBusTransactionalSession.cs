// SPDX-License-Identifier: Apache-2.0

using System.Collections.Generic;
using System.Text.Json;
using Azure.Messaging.ServiceBus;
using Talaria.Core.Abstractions;

namespace Talaria.Transports.AzureServiceBus;

/// <summary>
/// A buffered transaction for the Azure Service Bus transport. Produces obtained
/// via <see cref="GetProducerAsync{T}"/> are queued in memory until the caller
/// invokes <see cref="CommitAsync"/>; <see cref="AbortAsync"/> (or disposing
/// without commit) discards them.
/// <para>
/// ASB exposes a true <c>ServiceBusReceiver</c> transaction primitive that
/// covers a single <c>Receive</c> + <c>Complete</c> + <c>Send</c> cycle. The
/// saga sample uses the simpler buffered pattern (mirrors the in-memory
/// transport) for two reasons: (a) the saga host issues produce-only
/// transactions today, and (b) the buffered pattern keeps consumer-group
/// offsets out of the transaction, matching the existing Kafka/InMemory
/// semantics the engine already understands. The buffered-produce commit path
/// is straightforward to swap for a real <c>ServiceBusTransaction</c> when
/// the engine grows consumer-offset transactional semantics.
/// </para>
/// <para>
/// As with the other Talaria transports, saga state stores do not participate
/// in this transaction. A crash between the state save and the buffered
/// commit replays the triggering message against transitioned state, so
/// saga step handlers must be idempotent — this contract is documented on
/// <see cref="ITransactionalSession"/>.
/// </para>
/// </summary>
/// <since>1.0.0</since>
internal sealed class AzureServiceBusTransactionalSession : ITransactionalSession
{
    private readonly AzureServiceBusTransport _transport;
    private readonly string? _consumerGroup;
    private readonly TransactionOffsetSource? _offsetSource;
    private readonly object _gate = new();
    private readonly List<ServiceBusMessage> _buffer = new();
    private readonly Dictionary<string, ServiceBusSender> _senderCache = new(StringComparer.Ordinal);
    private bool _completed;

    public AzureServiceBusTransactionalSession(
        AzureServiceBusTransport transport,
        string? consumerGroup,
        TransactionOffsetSource? offsetSource)
    {
        _transport = transport;
        _consumerGroup = consumerGroup;
        _offsetSource = offsetSource;
    }

    /// <inheritdoc />
    public Task<IProducer<T>> GetProducerAsync<T>(string topic, CancellationToken ct = default)
    {
        ThrowIfCompleted();

        // ASB doesn't have a producer-per-transaction primitive: senders are
        // connection-bound, not transaction-bound. We hand out a producer
        // that buffers into the session's in-memory queue; commit flushes
        // them through a sender pulled from the transport's cache.
        return Task.FromResult<IProducer<T>>(new AzureServiceBusBufferedProducer<T>(this, topic));
    }

    /// <inheritdoc />
    public async Task CommitAsync(CancellationToken ct = default)
    {
        ThrowIfCompleted();

        List<ServiceBusMessage> snapshot;
        lock (_gate)
        {
            snapshot = new List<ServiceBusMessage>(_buffer);
            _buffer.Clear();
            _completed = true;
        }

        // Group buffered messages by topic so each topic's sender flushes
        // in order. ASB's SendMessageAsync preserves order on a single sender,
        // which matches the in-memory semantics.
        var byTopic = new Dictionary<string, List<ServiceBusMessage>>(StringComparer.Ordinal);
        foreach (var msg in snapshot)
        {
            // ApplicationProperties "talaria.transactional.topic" tags the
            // message with its destination topic — see the buffered producer
            // for the stamping logic.
            if (!msg.ApplicationProperties.TryGetValue("talaria.transactional.topic", out var topicObj)
                || topicObj is not string topic
                || string.IsNullOrEmpty(topic))
            {
                throw new InvalidOperationException(
                    "Buffered ASB transaction message is missing its destination topic tag.");
            }

            if (!byTopic.TryGetValue(topic, out var list))
            {
                list = new List<ServiceBusMessage>();
                byTopic[topic] = list;
            }

            list.Add(msg);
        }

        foreach (var (topic, messages) in byTopic)
        {
            var sender = GetOrCreateSender(topic);
            foreach (var message in messages)
            {
                // Strip the internal topic tag before sending — it was only
                // needed for the commit-time grouping.
                message.ApplicationProperties.Remove("talaria.transactional.topic");
                await sender.SendMessageAsync(message, ct).ConfigureAwait(false);
            }
        }

        // Note: ASB doesn't expose a way to commit a consumed message's offset
        // alongside produces (no KIP-98-style offset transaction). The
        // consumer's own CompleteMessageAsync call is the acknowledgement path.
        // We surface this explicitly so callers don't expect exactly-once
        // parity with Kafka.
        _ = _consumerGroup;
        _ = _offsetSource;
    }

    /// <inheritdoc />
    public Task AbortAsync(CancellationToken ct = default)
    {
        lock (_gate)
        {
            _buffer.Clear();
            _completed = true;
        }

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        lock (_gate)
        {
            _buffer.Clear();
            _completed = true;
        }

        _senderCache.Clear();
        return ValueTask.CompletedTask;
    }

    private ServiceBusSender GetOrCreateSender(string topic)
    {
        if (_senderCache.TryGetValue(topic, out var cached))
        {
            return cached;
        }

        // Reuse the transport-level sender cache rather than opening a fresh
        // sender for the transaction lifetime — AMQP links are multiplexed by
        // the SDK so this is safe and avoids the cost of a fresh link per
        // transaction.
        var sender = _transport.CheckoutSender(topic);
        _senderCache[topic] = sender;
        return sender;
    }

    internal void Buffer(string topic, ServiceBusMessage message)
    {
        lock (_gate)
        {
            ThrowIfCompleted();
            _buffer.Add(message);
        }
    }

    private void ThrowIfCompleted()
    {
        if (_completed)
        {
            throw new InvalidOperationException("The transaction has already been committed or aborted.");
        }
    }
}

/// <summary>
/// Producer whose writes are buffered into an
/// <see cref="AzureServiceBusTransactionalSession"/> until commit.
/// </summary>
internal sealed class AzureServiceBusBufferedProducer<T> : IProducer<T>
{
    private readonly AzureServiceBusTransactionalSession _session;
    private readonly string _topic;

    public AzureServiceBusBufferedProducer(
        AzureServiceBusTransactionalSession session,
        string topic)
    {
        _session = session;
        _topic = topic;
    }

    public Task ProduceAsync(
        T message,
        MessageHeaders? headers = null,
        string? partitionKey = null,
        CancellationToken ct = default)
    {
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

        finalHeaders[MessageHeaders.MessageTypeKey] = typeof(T).FullName ?? typeof(T).Name;

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

        if (!string.IsNullOrEmpty(partitionKey))
        {
            sbMessage.SessionId = partitionKey;
        }
        else if (finalHeaders.TryGetValue(MessageHeaders.CorrelationIdKey, out var cid) && !string.IsNullOrEmpty(cid))
        {
            sbMessage.SessionId = cid;
        }

        if (finalHeaders.TryGetValue(MessageHeaders.CorrelationIdKey, out var corrId) && !string.IsNullOrEmpty(corrId))
        {
            sbMessage.CorrelationId = corrId;
        }

        if (finalHeaders.TryGetValue(MessageHeaders.MessageTypeKey, out var mt) && !string.IsNullOrEmpty(mt))
        {
            sbMessage.Subject = mt;
        }

        foreach (var header in finalHeaders)
        {
            if (header.Value is null)
            {
                continue;
            }

            sbMessage.ApplicationProperties[header.Key] = header.Value;
        }

        // Internal tag: lets the session's commit path route this message to
        // the right topic without re-parsing the headers.
        sbMessage.ApplicationProperties["talaria.transactional.topic"] = _topic;

        if (System.Diagnostics.Activity.Current != null)
        {
            System.Diagnostics.Activity.Current.SetTag("messaging.destination.name", _topic);
            System.Diagnostics.Activity.Current.SetTag("messaging.system", "azure-service-bus");
        }

        _session.Buffer(_topic, sbMessage);
        return Task.CompletedTask;
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
