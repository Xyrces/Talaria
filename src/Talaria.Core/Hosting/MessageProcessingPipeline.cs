// SPDX-License-Identifier: Apache-2.0

using Microsoft.Extensions.Logging;
using Talaria.Core.Abstractions;

namespace Talaria.Core.Hosting;

/// <summary>
/// Shared message-processing building blocks used by both consumer engines
/// (topic handlers and sagas): the hop-count guard, idempotency acquisition with
/// fencing tokens, and the commit/complete and release/nack outcomes.
///
/// Failure policy: only user handlers should determine DLQ routing. Infrastructure
/// failures (store/commit/nack errors) are logged and leave the message uncommitted
/// so the transport redelivers it — a transient Redis/broker outage must never
/// dead-letter healthy messages.
/// </summary>
internal sealed class MessageProcessingPipeline
{
    private readonly IIdempotencyStore? _idempotencyStore;
    private readonly TalariaOptions _options;
    private readonly ILogger _logger;

    public MessageProcessingPipeline(
        IIdempotencyStore? idempotencyStore,
        TalariaOptions options,
        ILogger logger)
    {
        _idempotencyStore = idempotencyStore;
        _options = options;
        _logger = logger;
    }

    /// <summary>
    /// Result of the idempotency gate: whether dedup applies to this message at all,
    /// and the acquired fencing-token lock when this worker owns it.
    /// </summary>
    internal readonly record struct IdempotencyGate(bool Enabled, IdempotencyLock? Lock)
    {
        /// <summary>Dedup applies, but another worker owns or completed this message.</summary>
        public bool IsDuplicate => Enabled && Lock is null;
    }

    /// <summary>
    /// Applies the max-hop cycle guard. When it returns true the caller must nack the
    /// envelope (the DLQ reason is already set) and skip processing.
    /// </summary>
    public bool IsHopCountExceeded<T>(MessageEnvelope<T> envelope, string topic)
    {
        var rawHopCount = envelope.Headers.TryGetValue(MessageHeaders.HopCountKey, out var v) ? v : null;
        var hopCount = envelope.Headers.HopCount;

        if (rawHopCount is not null && !int.TryParse(rawHopCount, out _))
        {
            _logger.LogWarning(
                "malformed hop count header '{Value}' on message {MessageId}; treating as 0",
                rawHopCount, envelope.Headers.MessageId);
            hopCount = 0;
        }

        if (hopCount < _options.MaxHopCount)
        {
            return false;
        }

        _logger.LogWarning(
            "Message on '{Topic}' exceeded max hop count ({HopCount}/{Max}). Routing to DLQ.",
            topic, hopCount, _options.MaxHopCount);
        envelope.Headers.DlqReason = "max_hops_exceeded";
        return true;
    }

    /// <summary>
    /// Acquires the idempotency lock for the message, keyed by consumer group.
    /// </summary>
    public async Task<IdempotencyGate> AcquireAsync<T>(
        MessageEnvelope<T> envelope,
        string consumerGroup,
        CancellationToken ct)
    {
        if (_idempotencyStore is null)
        {
            return new IdempotencyGate(Enabled: false, Lock: null);
        }

        var msgId = envelope.Headers.MessageId;
        if (string.IsNullOrEmpty(msgId))
        {
            return new IdempotencyGate(Enabled: false, Lock: null);
        }

        var lck = await _idempotencyStore.TryAcquireLockAsync(msgId, consumerGroup, _options.IdempotencyLockTtl, ct);
        return new IdempotencyGate(Enabled: true, Lock: lck);
    }

    /// <summary>
    /// Marks the message complete (when locked) and commits its offset.
    /// </summary>
    public async Task CompleteAsync<T>(
        IdempotencyLock? lck,
        IConsumer<T> consumer,
        MessageEnvelope<T> envelope,
        CancellationToken ct)
    {
        if (lck is not null)
        {
            await _idempotencyStore!.MarkCompleteAsync(lck, ct);
        }

        await consumer.CommitAsync(envelope, ct);
    }

    /// <summary>
    /// Releases a held lock without any offset/DLQ action (used by the deferral path,
    /// where the caller commits the original envelope itself).
    /// </summary>
    public async Task ReleaseAsync(IdempotencyLock lck, CancellationToken ct)
    {
        await _idempotencyStore!.ReleaseLockAsync(lck, ct);
    }

    /// <summary>
    /// Releases the lock (best effort) and routes the message to the DLQ (best effort).
    /// A DLQ failure leaves the message uncommitted for redelivery rather than faulting the loop.
    /// </summary>
    public async Task FailAsync<T>(
        IdempotencyLock? lck,
        IConsumer<T> consumer,
        MessageEnvelope<T> envelope,
        Exception ex,
        string? dlqReason,
        CancellationToken ct)
    {
        if (lck is not null)
        {
            try
            {
                await _idempotencyStore!.ReleaseLockAsync(lck, ct);
            }
            catch (Exception releaseEx)
            {
                _logger.LogError(releaseEx, "Failed to release idempotency lock {MessageId}; it expires via TTL.", lck.MessageId);
            }
        }

        envelope.Headers.DlqException = _options.IncludeExceptionDetailsInDlq
            ? ex.Message
            : "An exception occurred while processing the message. Enable IncludeExceptionDetailsInDlq for details.";
        if (dlqReason is not null)
        {
            envelope.Headers.DlqReason = dlqReason;
        }

        try
        {
            await consumer.NackAsync(envelope, ct);
        }
        catch (Exception nackEx)
        {
            _logger.LogError(nackEx, "Failed to route message to the DLQ; it remains uncommitted for redelivery.");
        }
    }
}

/// <summary>
/// Runs a consumer loop with per-topic fault isolation: a faulting loop is logged and
/// restarted with capped exponential backoff instead of taking down every other consumer
/// or the host.
/// </summary>
internal static class ConsumerSupervision
{
    public static async Task RunSupervisedAsync(
        string name,
        Func<CancellationToken, Task> loop,
        ILogger logger,
        CancellationToken ct)
    {
        var backoff = TimeSpan.FromSeconds(1);
        var maxBackoff = TimeSpan.FromSeconds(30);

        while (!ct.IsCancellationRequested)
        {
            try
            {
                await loop(ct);
                return; // Clean exit (channel completed).
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Consumer loop '{Name}' faulted; restarting in {Backoff}.", name, backoff);

                try
                {
                    await Task.Delay(backoff, ct);
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    return;
                }

                backoff = TimeSpan.FromTicks(Math.Min(backoff.Ticks * 2, maxBackoff.Ticks));
            }
        }
    }
}
