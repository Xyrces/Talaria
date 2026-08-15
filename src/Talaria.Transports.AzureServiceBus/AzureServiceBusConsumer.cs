// SPDX-License-Identifier: Apache-2.0

using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Channels;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Logging;
using Talaria.Core.Abstractions;

namespace Talaria.Transports.AzureServiceBus;

/// <summary>
/// Azure Service Bus consumer. One instance is created per
/// <c>(topic-or-queue, consumer-group)</c> pair by the transport and disposed
/// when the host shuts down.
/// <para>
/// ASB delivers messages via an event-driven <see cref="ServiceBusProcessor"/>
/// pump; we marshal them into a bounded <see cref="Channel{T}"/> so the host
/// pipeline can iterate via the consumer enumeration. This mirrors the
/// Talaria Kafka consumer's single-pump-thread model: there is exactly one
/// writer (the processor handler) and one reader (the enumerator), so
/// <c>SingleWriter = true</c> and <c>SingleReader = true</c> enable the
/// lock-free fast path.
/// </para>
/// <para>
/// A consumer instance supports exactly one enumeration. The host restarts
/// consumption by creating a new consumer via
/// <see cref="ITransport.CreateConsumerAsync{T}"/>. Reusing a consumer
/// instance, or enumerating the returned <see cref="IAsyncEnumerable{T}"/>
/// more than once, throws <see cref="InvalidOperationException"/>.
/// </para>
/// <para>
/// Acknowledgement semantics: <see cref="CommitAsync"/> calls
/// <c>CompleteMessageAsync</c> to drop the message from the broker's
/// peek-lock; <see cref="NackAsync"/> forwards the message to the
/// DLQ-suffixed entity and then completes it. Disposing a consumer while
/// there are still uncommitted messages leaves the broker's lock to expire,
/// triggering ASB's native redelivery — the in-memory equivalent of Kafka
/// redelivering uncommitted messages.
/// </para>
/// </summary>
/// <since>1.0.0</since>
internal sealed class AzureServiceBusConsumer<T> : IConsumer<T>
{
    private readonly ServiceBusProcessor _processor;
    private readonly ServiceBusSender _dlqSender;
    private readonly string _topic;
    private readonly string _dlqEntity;
    private readonly int _bufferCapacity;
    private readonly bool _includeDlqExceptionDetails;
    private readonly ILogger? _logger;

    // Pending envelopes awaiting Commit/Nack, keyed by ASB sequence number.
    // A disposal re-abandon any still-pending entries by relying on the
    // broker's peek-lock expiry rather than calling Abandon explicitly.
    private readonly ConcurrentDictionary<long, PendingEntry> _pending = new();

    private Channel<MessageEnvelope<T>>? _activeChannel;
    private CancellationTokenSource? _activeReaderCts;
    private int _disposed;
    private int _consuming;
    private int _enumerating;

    public AzureServiceBusConsumer(
        ServiceBusProcessor processor,
        ServiceBusSender dlqSender,
        string topic,
        string dlqEntity,
        int bufferCapacity,
        bool includeDlqExceptionDetails,
        ILogger? logger = null)
    {
        _processor = processor;
        _dlqSender = dlqSender;
        _topic = topic;
        _dlqEntity = dlqEntity;
        _bufferCapacity = bufferCapacity > 0 ? bufferCapacity : 100;
        _includeDlqExceptionDetails = includeDlqExceptionDetails;
        _logger = logger;
    }

    private readonly record struct PendingEntry(ServiceBusReceivedMessage Message, ProcessMessageEventArgs Args, MessageEnvelope<T> Envelope);

    /// <inheritdoc />
    public IAsyncEnumerable<MessageEnvelope<T>> ConsumeAsync(CancellationToken ct = default)
    {
        SingleEnumerationGuard.ThrowIfAlreadyStarted(ref _consuming);
        return ConsumeAsyncCore(ct);
    }

    private async IAsyncEnumerable<MessageEnvelope<T>> ConsumeAsyncCore(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        SingleEnumerationGuard.ThrowIfAlreadyStarted(ref _enumerating);
        var channel = Channel.CreateBounded<MessageEnvelope<T>>(new BoundedChannelOptions(_bufferCapacity)
        {
            SingleWriter = true,
            SingleReader = true,
            FullMode = BoundedChannelFullMode.Wait,
        });

        // Subscribe to the processor on the first (and only) enumeration.
        // The SDK's ServiceBusProcessor is started once and torn down with
        // the consumer's disposal.
        //
        // IMPORTANT: the active channel must be assigned BEFORE the processor
        // can raise ProcessErrorAsync. A non-transient error raised during
        // startup (e.g. an AMQP link failure or credential expiry) completes
        // this channel so the host's supervised loop can restart with backoff.
        // If EnsureSubscribedAsync ran first, OnProcessorErrorAsync would see
        // _activeChannel == null and silently drop the fault.
        var previous = Interlocked.Exchange(ref _activeChannel, channel);
        previous?.Writer.TryComplete();
        _activeReaderCts?.Cancel();
        _activeReaderCts?.Dispose();
        _activeReaderCts = CancellationTokenSource.CreateLinkedTokenSource(ct);

        try
        {
            await EnsureSubscribedAsync().ConfigureAwait(false);
        }
        catch
        {
            // Subscription failed (e.g. StartProcessingAsync threw). The channel
            // will never be read, so complete it and detach so the fault is not
            // hidden from the caller and disposal does not leak a half-started
            // enumeration.
            Interlocked.CompareExchange(ref _activeChannel, null, channel);
            channel.Writer.TryComplete();
            throw;
        }

        try
        {
            // Use the linked CTS so DisposeAsync can cancel a blocked reader
            // independently of the caller's token.
            await foreach (var env in channel.Reader.ReadAllAsync(_activeReaderCts.Token).ConfigureAwait(false))
            {
                yield return env;
            }
        }
        finally
        {
            // Detach this enumeration's channel. The processor keeps running
            // until DisposeAsync; a host restart must create a new consumer
            // instance via ITransport.CreateConsumerAsync.
            Interlocked.CompareExchange(ref _activeChannel, null, channel);
            channel.Writer.TryComplete();
        }
    }

    private int _subscribed;
    private async Task EnsureSubscribedAsync()
    {
        if (Interlocked.CompareExchange(ref _subscribed, 1, 0) != 0)
        {
            return;
        }

        _processor.ProcessMessageAsync += OnProcessorMessageAsync;
        _processor.ProcessErrorAsync += OnProcessorErrorAsync;

        try
        {
            await _processor.StartProcessingAsync().ConfigureAwait(false);
        }
        catch
        {
            _processor.ProcessMessageAsync -= OnProcessorMessageAsync;
            _processor.ProcessErrorAsync -= OnProcessorErrorAsync;
            Interlocked.Exchange(ref _subscribed, 0);
            throw;
        }
    }

    private async Task OnProcessorMessageAsync(ProcessMessageEventArgs args)
    {
        var channel = _activeChannel;
        if (channel is null)
        {
            // No active enumeration — let ASB redeliver by abandoning the lock.
            return;
        }

        var sbMessage = args.Message;
        var headers = new MessageHeaders();
        foreach (var kvp in sbMessage.ApplicationProperties)
        {
            if (kvp.Value is null)
            {
                continue;
            }

            // ASB stores ApplicationProperties values as object — coerce to
            // string so the engine's IDictionary<string, string> contract
            // holds. Non-string values are stringified (callers that need
            // typed values should consult the underlying envelope).
            headers[kvp.Key] = kvp.Value.ToString() ?? string.Empty;
        }

        // Promote standard ASB metadata into Talaria headers.
        if (!string.IsNullOrEmpty(sbMessage.MessageId))
        {
            headers.MessageId = sbMessage.MessageId;
        }

        if (!string.IsNullOrEmpty(sbMessage.CorrelationId))
        {
            headers[MessageHeaders.CorrelationIdKey] = sbMessage.CorrelationId;
        }

        T? payload;
        try
        {
            payload = JsonSerializer.Deserialize<T>(sbMessage.Body.ToArray());
        }
        catch (Exception ex)
        {
            headers.DlqReason = "DeserializationFailed";
            headers.DlqException = _includeDlqExceptionDetails
                ? ex.Message
                : "Failed to deserialize the message payload. Enable IncludeExceptionDetailsInDlq for details.";

            await RouteToDlqAsync(sbMessage, headers, args.CancellationToken).ConfigureAwait(false);
            await args.CompleteMessageAsync(sbMessage).ConfigureAwait(false);
            return;
        }

        if (payload is null)
        {
            headers.DlqReason = "null_payload";
            await RouteToDlqAsync(sbMessage, headers, args.CancellationToken).ConfigureAwait(false);
            await args.CompleteMessageAsync(sbMessage).ConfigureAwait(false);
            return;
        }

        var envelope = new MessageEnvelope<T>
        {
            Payload = payload,
            Headers = headers,
            SourceTopic = _topic,
            PartitionKey = sbMessage.SessionId,
            CorrelationId = headers.TryGetValue(MessageHeaders.CorrelationIdKey, out var cid) ? cid : null,
            Timestamp = sbMessage.EnqueuedTime,
            // ASB exposes the broker-assigned sequence number; we use it as
            // the "offset" surrogate so CommitAsync can find the right entry.
            Offset = sbMessage.SequenceNumber,
        };

        _pending[envelope.Offset] = new PendingEntry(sbMessage, args, envelope);

        try
        {
            await channel.Writer.WriteAsync(envelope, args.CancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is OperationCanceledException or System.Threading.Channels.ChannelClosedException)
        {
            // Channel was closed mid-write (consumer went away, host shutdown).
            // Abandon by clearing the pending entry; the broker's peek-lock
            // will expire and trigger ASB's native redelivery.
            _pending.TryRemove(envelope.Offset, out _);
        }
    }

    private Task OnProcessorErrorAsync(ProcessErrorEventArgs args)
    {
        _logger?.LogError(
            args.Exception,
            "Azure Service Bus processor error on entity {Entity}: {ErrorSource} ({FullyQualifiedNamespace}).",
            _topic, args.ErrorSource, args.FullyQualifiedNamespace);

        var isTransient = args.Exception is ServiceBusException sbEx && sbEx.IsTransient;
        if (!isTransient)
        {
            // Fatal errors (non-transient ServiceBusException, AMQP link failures,
            // credentials expiry, or any other exception) must fault the active
            // enumeration so the host's supervised loop can restart with backoff.
            _activeChannel?.Writer.TryComplete(args.Exception);
        }

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public async Task CommitAsync(MessageEnvelope<T> message, CancellationToken ct = default)
    {
        if (!_pending.TryGetValue(message.Offset, out var entry))
        {
            // Already committed or never tracked — no-op. Commit-after-commit
            // is allowed by the host pipeline (idempotent retries).
            return;
        }

        try
        {
            await entry.Args.CompleteMessageAsync(entry.Message, ct).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger?.LogWarning(
                ex,
                "Failed to complete ASB message {SequenceNumber} on entity {Entity}; broker lock will expire and redeliver.",
                entry.Message.SequenceNumber, _topic);
            throw;
        }

        // Only drop the pending entry after the broker acknowledged the completion.
        // If CompleteMessageAsync threw, keeping the entry lets the uncommitted
        // delivery redeliver and keeps it visible to DisposeAsync cleanup.
        _pending.TryRemove(message.Offset, out _);
    }

    /// <inheritdoc />
    public async Task NackAsync(MessageEnvelope<T> message, CancellationToken ct = default)
    {
        // Move to DLQ first, then complete the original delivery so the
        // broker stops holding it.
        var sbMessage = _pending.TryGetValue(message.Offset, out var entry)
            ? entry.Message
            : null;

        var clone = new ServiceBusMessage(message.Payload is null
            ? Array.Empty<byte>()
            : JsonSerializer.SerializeToUtf8Bytes(message.Payload))
        {
            MessageId = message.Headers.MessageId ?? Guid.NewGuid().ToString("N"),
            ContentType = "application/json",
        };

        if (!string.IsNullOrEmpty(message.PartitionKey))
        {
            clone.SessionId = message.PartitionKey;
        }

        if (!string.IsNullOrEmpty(message.CorrelationId))
        {
            clone.CorrelationId = message.CorrelationId;
        }

        foreach (var header in message.Headers)
        {
            if (header.Value is null)
            {
                continue;
            }

            clone.ApplicationProperties[header.Key] = header.Value;
        }

        await _dlqSender.SendMessageAsync(clone, ct).ConfigureAwait(false);

        if (sbMessage is not null)
        {
            await CommitAsync(message, ct).ConfigureAwait(false);
        }
    }

    private async Task RouteToDlqAsync(ServiceBusReceivedMessage sbMessage, MessageHeaders headers, CancellationToken ct)
    {
        var clone = new ServiceBusMessage(sbMessage.Body)
        {
            MessageId = sbMessage.MessageId,
            ContentType = sbMessage.ContentType ?? "application/json",
        };

        if (!string.IsNullOrEmpty(sbMessage.SessionId))
        {
            clone.SessionId = sbMessage.SessionId;
        }

        if (!string.IsNullOrEmpty(sbMessage.CorrelationId))
        {
            clone.CorrelationId = sbMessage.CorrelationId;
        }

        if (!string.IsNullOrEmpty(sbMessage.Subject))
        {
            clone.Subject = sbMessage.Subject;
        }

        foreach (var header in headers)
        {
            if (header.Value is null)
            {
                continue;
            }

            clone.ApplicationProperties[header.Key] = header.Value;
        }

        await _dlqSender.SendMessageAsync(clone, ct).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        // Cancel any in-flight enumeration so the reader loop unwinds.
        _activeReaderCts?.Cancel();

        try
        {
            await _processor.StopProcessingAsync().ConfigureAwait(false);
        }
        catch
        {
            // Best effort — host is shutting down
        }

        // EnsureSubscribedAsync may never have run (e.g. the consumer was disposed
        // before enumeration started), so handler removal is best-effort.
        try { _processor.ProcessMessageAsync -= OnProcessorMessageAsync; }
        catch (ArgumentException) { /* handler was never attached */ }
        try { _processor.ProcessErrorAsync -= OnProcessorErrorAsync; }
        catch (ArgumentException) { /* handler was never attached */ }

        // Close any still-pending messages: rather than calling
        // Complete/Abandon explicitly, we let the broker's peek-lock
        // expire and redeliver. This matches the "uncommitted messages
        // are redelivered" guarantee Kafka gives the host.
        _pending.Clear();

        _activeChannel?.Writer.TryComplete();
        _activeChannel = null;
        _activeReaderCts?.Dispose();
        _activeReaderCts = null;

        await _processor.DisposeAsync().ConfigureAwait(false);
    }
}

