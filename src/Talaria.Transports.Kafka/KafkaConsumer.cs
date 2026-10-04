// SPDX-License-Identifier: Apache-2.0

using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Channels;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Talaria.Core.Abstractions;

namespace Talaria.Transports.Kafka;

/// <summary>
/// Apache Kafka consumer implementation.
/// All <see cref="IConsumer{T}"/> access happens on a single long-running poll thread:
/// commit requests from <see cref="CommitAsync"/>/<see cref="NackAsync"/> are marshaled
/// through an internal channel and drained by the poll loop (Confluent consumers are not
/// thread-safe). <see cref="CommitAsync"/> returns after the broker confirms the contiguous
/// settled offset; a crash before confirmation means redelivery, which idempotency stores cover.
/// <para>
/// A consumer instance supports exactly one enumeration. The host restarts consumption by
/// creating a new consumer via <see cref="ITransport.CreateConsumerAsync{T}"/>. Reusing a
/// consumer instance, or enumerating the returned <see cref="IAsyncEnumerable{T}"/> more
/// than once, throws <see cref="InvalidOperationException"/>.
/// </para>
/// </summary>
internal sealed class KafkaConsumer<T> : IConsumer<T>, IConsumerReadiness
{
    private readonly IConsumer<string, byte[]> _consumer;
    private readonly IProducer<string, byte[]> _producer;
    private readonly string _topic;
    private readonly string _dlqTopic;
    private readonly int _bufferCapacity;
    private readonly bool _includeDlqExceptionDetails;
    private readonly ILogger? _logger;

    // Commit requests marshaled to the poll thread (the only thread touching _consumer).
    private sealed record CommitRequest(TopicPartitionOffset Position, TaskCompletionSource Completion);
    private readonly SettlementTracker _settlements = new();
    private readonly List<CommitRequest> _waitingCommits = new();
    private readonly Channel<CommitRequest> _commitRequests =
        Channel.CreateUnbounded<CommitRequest>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false
        });

    private readonly CancellationTokenSource _disposeCts = new();
    private readonly TaskCompletionSource _ready;
    private volatile Task? _pumpTask;
    private int _disposed;
    private int _consuming;
    private int _enumerating;

    public Task Ready => _ready.Task;

    public KafkaConsumer(
        IConsumer<string, byte[]> consumer,
        IProducer<string, byte[]> producer,
        string topic,
        string dlqSuffix,
        ILogger? logger = null,
        int bufferCapacity = 100,
        bool includeDlqExceptionDetails = false,
        TaskCompletionSource? readiness = null)
    {
        _consumer = consumer;
        _producer = producer;
        _topic = topic;
        _dlqTopic = _topic + dlqSuffix;
        _logger = logger;
        _bufferCapacity = bufferCapacity > 0 ? bufferCapacity : 100;
        _includeDlqExceptionDetails = includeDlqExceptionDetails;
        _ready = readiness ?? new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    public IAsyncEnumerable<MessageEnvelope<T>> ConsumeAsync(CancellationToken ct = default)
    {
        SingleEnumerationGuard.ThrowIfAlreadyStarted(ref _consuming);
        return ConsumeAsyncCore(ct);
    }

    private async IAsyncEnumerable<MessageEnvelope<T>> ConsumeAsyncCore([EnumeratorCancellation] CancellationToken ct = default)
    {
        SingleEnumerationGuard.ThrowIfAlreadyStarted(ref _enumerating);
        var channel = Channel.CreateBounded<MessageEnvelope<T>>(new BoundedChannelOptions(_bufferCapacity)
        {
            SingleWriter = true,
            SingleReader = true,
            FullMode = BoundedChannelFullMode.Wait
        });

        // The pump stops when the caller cancels, when enumeration is abandoned
        // (finally below), or when the consumer is disposed.
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct, _disposeCts.Token);
        var pumpCt = linkedCts.Token;

        var pump = Task.Factory
            .StartNew(() => PumpAsync(channel.Writer, pumpCt), pumpCt, TaskCreationOptions.LongRunning, TaskScheduler.Default)
            .Unwrap();
        _pumpTask = pump;

        try
        {
            await foreach (var env in channel.Reader.ReadAllAsync(ct))
            {
                yield return env;
            }
        }
        finally
        {
            // Enumeration ended or was abandoned mid-stream — stop the pump so it never
            // blocks forever on a full channel or a Consume call, and observe its task.
            linkedCts.Cancel();
            try
            {
                await pump.ConfigureAwait(false);
            }
            catch
            {
                // Pump errors already surfaced to the enumerator via channel completion.
            }
        }
    }

    private async Task PumpAsync(ChannelWriter<MessageEnvelope<T>> writer, CancellationToken ct)
    {
        try
        {
            _consumer.Subscribe(_topic);

            while (!ct.IsCancellationRequested)
            {
                ConsumeResult<string, byte[]>? consumeResult = null;
                try
                {
                    consumeResult = _consumer.Consume(TimeSpan.FromMilliseconds(100));
                }
                catch (ConsumeException ex)
                {
                    _logger?.LogError(
                        ex,
                        "Kafka consume error on topic {Topic}: {ErrorCode} {ErrorReason} (fatal: {IsFatal}).",
                        _topic, ex.Error.Code, ex.Error.Reason, ex.Error.IsFatal);

                    if (IsFatal(ex.Error))
                    {
                        // Fatal/authorization errors cannot be retried — complete the channel
                        // with the error so the supervised loop restarts with backoff.
                        throw;
                    }

                    try { await Task.Delay(200, ct).ConfigureAwait(false); }
                    catch (OperationCanceledException) { break; }
                    continue;
                }

                DrainCommits();

                // A factory-created consumer signals readiness through Confluent's
                // assignment callback, including a successful empty assignment for an
                // extra replica. Directly constructed consumers use this fallback.
                if (!_ready.Task.IsCompleted && _consumer.Assignment.Count > 0)
                    _ready.TrySetResult();

                if (consumeResult == null || consumeResult.IsPartitionEOF)
                {
                    continue;
                }

                _settlements.Delivered(consumeResult.TopicPartitionOffset);

                var talariaHeaders = new MessageHeaders();
                if (consumeResult.Message.Headers != null)
                {
                    foreach (var header in consumeResult.Message.Headers)
                    {
                        talariaHeaders[header.Key] = Encoding.UTF8.GetString(header.GetValueBytes());
                    }
                }

                T? payload = default;
                try
                {
                    payload = JsonSerializer.Deserialize<T>(consumeResult.Message.Value);
                }
                catch (Exception ex)
                {
                    talariaHeaders.DlqReason = "DeserializationFailed";
                    talariaHeaders.DlqException = _includeDlqExceptionDetails
                        ? ex.Message
                        : "Failed to deserialize the message payload. Enable IncludeExceptionDetailsInDlq for details.";

                    await RouteToDlqAsync(consumeResult, talariaHeaders, ct).ConfigureAwait(false);
                    _settlements.Settle(consumeResult.TopicPartitionOffset);
                    DrainCommits();
                    continue;
                }

                if (payload == null)
                {
                    talariaHeaders.DlqReason = "null_payload";
                    await RouteToDlqAsync(consumeResult, talariaHeaders, ct).ConfigureAwait(false);
                    _settlements.Settle(consumeResult.TopicPartitionOffset);
                    DrainCommits();
                    continue;
                }

                var env = new MessageEnvelope<T>
                {
                    Payload = payload,
                    Headers = talariaHeaders,
                    SourceTopic = _topic,
                    PartitionKey = consumeResult.Message.Key,
                    Partition = consumeResult.Partition.Value,
                    CorrelationId = talariaHeaders.TryGetValue(MessageHeaders.CorrelationIdKey, out var cid) ? cid : null,
                    Timestamp = consumeResult.Message.Timestamp.UtcDateTime,
                    Offset = consumeResult.Offset.Value
                };

                if (!writer.TryWrite(env))
                {
                    var paused = _consumer.Assignment;
                    _consumer.Pause(paused);
                    try
                    {
                        while (!writer.TryWrite(env))
                        {
                            ct.ThrowIfCancellationRequested();
                            DrainCommits();
                            // Keep group membership alive while the application applies backpressure.
                            var unexpected = _consumer.Consume(TimeSpan.FromMilliseconds(10));
                            if (unexpected is not null && !unexpected.IsPartitionEOF)
                                throw new InvalidOperationException("Kafka assignment changed while buffered; restarting from settled offsets.");
                            await Task.Delay(10, ct);
                        }
                    }
                    finally { _consumer.Resume(paused.Intersect(_consumer.Assignment)); }
                }
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested) { }
        catch (Exception ex)
        {
            _ready.TrySetException(ex);
            writer.TryComplete(ex);
            return;
        }
        finally
        {
            // Best-effort flush of any commits queued just before the pump stops, then
            // leave the group: the next enumeration rejoins and resumes from committed offsets.
            try { DrainCommits(); }
            catch (Exception ex) { _logger?.LogError(ex, "Final Kafka settlement failed; records remain recoverable."); }
            _commitRequests.Writer.TryComplete();
            while (_commitRequests.Reader.TryRead(out var request)) _waitingCommits.Add(request);
            foreach (var request in _waitingCommits)
                request.Completion.TrySetException(new InvalidOperationException("Kafka consumer stopped before settlement was confirmed."));
            _waitingCommits.Clear();
            try { _consumer.Unsubscribe(); } catch { }
            writer.TryComplete();
        }
    }

    private static bool IsFatal(Error error)
        => error.IsFatal
           || error.Code is ErrorCode.TopicAuthorizationFailed
               or ErrorCode.GroupAuthorizationFailed
               or ErrorCode.ClusterAuthorizationFailed;

    /// <summary>
    /// Drains queued commit requests on the poll thread. Failures are logged and dropped —
    /// an uncommitted message is redelivered, which the idempotency stores cover.
    /// </summary>
    private void DrainCommits()
    {
        while (_commitRequests.Reader.TryRead(out var request))
        {
            _waitingCommits.Add(request);
            _settlements.Settle(request.Position);
        }
        var batch = _settlements.Ready();
        if (batch.Count == 0) return;
        try
        {
            _consumer.Commit(batch);
            foreach (var next in batch)
            {
                _settlements.Committed(next);
                foreach (var completed in _waitingCommits.Where(x => x.Position.TopicPartition.Equals(next.TopicPartition)
                    && x.Position.Offset.Value < next.Offset.Value).ToArray())
                {
                    completed.Completion.TrySetResult();
                    _waitingCommits.Remove(completed);
                }
            }
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Failed to commit offsets on topic {Topic}; affected messages will be redelivered.", _topic);
            foreach (var request in _waitingCommits) request.Completion.TrySetException(ex);
            _waitingCommits.Clear();
            throw;
        }
    }

    public Task CommitAsync(MessageEnvelope<T> message, CancellationToken ct = default)
    {
        if (!string.IsNullOrEmpty(message.SourceTopic) && message.Partition is int partition)
        {
            // Kafka committed offsets mean "the next offset to fetch", hence + 1.
            var tpo = new TopicPartitionOffset(message.SourceTopic, new Partition(partition), new Offset(message.Offset));
            var request = new CommitRequest(tpo, new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));
            if (!_commitRequests.Writer.TryWrite(request))
            {
                _logger?.LogError(
                    "Cannot queue offset commit for message on topic {Topic}: the consumer is shutting down. Leaving the message uncommitted for redelivery.",
                    message.SourceTopic);
                throw new InvalidOperationException("Kafka consumer is stopping; settlement was not queued.");
            }
            return request.Completion.Task.WaitAsync(ct);
        }
        else
        {
            // Never fall back to Commit() with no arguments — that commits the current position on
            // ALL assigned partitions and can skip unprocessed messages. Leave uncommitted for redelivery.
            _logger?.LogError(
                "Cannot commit offset for message on topic {Topic}: missing partition metadata. Leaving the message uncommitted for redelivery.",
                message.SourceTopic);
        }
        throw new InvalidOperationException("Kafka settlement requires source topic and partition metadata.");
    }

    public async Task NackAsync(MessageEnvelope<T> message, CancellationToken ct = default)
    {
        // Nack moves it to the DLQ and commits the original message.
        var headers = new Headers();
        foreach (var header in message.Headers)
        {
            if (header.Value != null)
                headers.Add(header.Key, Encoding.UTF8.GetBytes(header.Value));
        }

        var dlqMsg = new Message<string, byte[]>
        {
            Key = message.PartitionKey ?? message.CorrelationId ?? Guid.NewGuid().ToString("N"),
            Value = JsonSerializer.SerializeToUtf8Bytes(message.Payload),
            Headers = headers
        };

        await _producer.ProduceAsync(_dlqTopic, dlqMsg, ct);

        await CommitAsync(message, ct);
    }

    private async Task RouteToDlqAsync(ConsumeResult<string, byte[]> consumeResult, MessageHeaders headers, CancellationToken ct)
    {
        var kafkaHeaders = new Headers();
        foreach (var header in headers)
        {
            if (header.Value != null)
                kafkaHeaders.Add(header.Key, Encoding.UTF8.GetBytes(header.Value));
        }

        var correlationId = headers.TryGetValue(MessageHeaders.CorrelationIdKey, out var cid) ? cid : null;
        var dlqMsg = new Message<string, byte[]>
        {
            Key = consumeResult.Message.Key ?? correlationId ?? Guid.NewGuid().ToString("N"),
            Value = consumeResult.Message.Value, // Write raw payload back because it failed deserialization
            Headers = kafkaHeaders
        };

        await _producer.ProduceAsync(_dlqTopic, dlqMsg, ct);
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        // Stop the pump (if running), flush any commits queued after the last drain
        // (e.g. queued while no enumeration was active), then close and dispose the
        // underlying consumer. Close releases the subscription and group membership.
        _disposeCts.Cancel();
        if (_pumpTask is not null)
        {
            try { await _pumpTask.ConfigureAwait(false); } catch { }
        }

        // Safe to touch _consumer here: the pump (the only other accessor) has finished.
        // The pump settles and fails all outstanding requests before returning.

        try { _consumer.Close(); } catch { }
        _consumer.Dispose();
        _disposeCts.Dispose();
    }
}
