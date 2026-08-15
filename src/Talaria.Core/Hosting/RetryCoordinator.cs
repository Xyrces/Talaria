// SPDX-License-Identifier: Apache-2.0

using System.Text.Json;
using Microsoft.Extensions.Logging;
using Talaria.Core.Abstractions;
using Talaria.Core.Registration;

namespace Talaria.Core.Hosting;

/// <summary>
/// Coordinates opt-in delayed retries for topic handlers and saga step handlers.
/// On handler exception: if retries are enabled, a deferred copy of the message is
/// enqueued in <see cref="IDeferralStore"/> and the original delivery is committed;
/// otherwise the message falls through to the existing DLQ path.
/// </summary>
internal sealed class RetryCoordinator
{
    private readonly IDeferralStore? _deferralStore;
    private readonly TalariaOptions _options;
    private readonly ILogger _logger;

    public RetryCoordinator(IDeferralStore? deferralStore, TalariaOptions options, ILogger logger)
    {
        _deferralStore = deferralStore;
        _options = options;
        _logger = logger;
    }

    /// <summary>
    /// Result of attempting to coordinate a retry after a handler exception.
    /// </summary>
    internal enum RetryOutcome
    {
        /// <summary>The exception was not eligible for retry (e.g. cancellation); caller must handle normally.</summary>
        NotRetryable,

        /// <summary>A retry was scheduled; the original envelope has been committed and the lock released.</summary>
        Scheduled,

        /// <summary>Retry attempts were exhausted; the message was routed to the DLQ.</summary>
        Exhausted,

        /// <summary>Retries are enabled but no deferral store is available, or enqueue failed; message was DLQ'd.</summary>
        Unavailable,
    }

    /// <summary>
    /// Attempts to schedule a delayed retry for a topic handler exception.
    /// </summary>
    /// <typeparam name="T">The message payload type.</typeparam>
    /// <param name="registration">The topic registration whose handler threw.</param>
    /// <param name="pipeline">The shared processing pipeline.</param>
    /// <param name="consumer">The consumer that delivered the message.</param>
    /// <param name="envelope">The failed message envelope.</param>
    /// <param name="ex">The handler exception.</param>
    /// <param name="lock">The acquired idempotency lock, if any.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>The outcome of the coordination attempt.</returns>
    internal async Task<RetryOutcome> TryCoordinateTopicRetryAsync<T>(
        TopicRegistration registration,
        MessageProcessingPipeline pipeline,
        IConsumer<T> consumer,
        MessageEnvelope<T> envelope,
        Exception ex,
        IdempotencyLock? @lock,
        CancellationToken ct)
    {
        var policy = registration.RetryPolicy ?? _options.DefaultRetryPolicy;
        return await TryCoordinateRetryAsync(
            policy,
            registration.TopicName,
            registration.MessageType,
            envelope,
            ex,
            pipeline,
            consumer,
            @lock,
            ct);
    }

    /// <summary>
    /// Attempts to schedule a delayed retry for a saga step handler exception.
    /// Saga steps do not yet support per-step retry policies, so the global default is used.
    /// </summary>
    /// <param name="topicName">The step topic name.</param>
    /// <param name="messageType">The step message type.</param>
    /// <param name="pipeline">The shared processing pipeline.</param>
    /// <param name="consumer">The consumer that delivered the message.</param>
    /// <param name="envelope">The failed message envelope.</param>
    /// <param name="ex">The handler exception.</param>
    /// <param name="lock">The acquired idempotency lock, if any.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>The outcome of the coordination attempt.</returns>
    internal async Task<RetryOutcome> TryCoordinateSagaRetryAsync(
        string topicName,
        Type messageType,
        MessageProcessingPipeline pipeline,
        IConsumer<JsonElement> consumer,
        MessageEnvelope<JsonElement> envelope,
        Exception ex,
        IdempotencyLock? @lock,
        CancellationToken ct)
    {
        var policy = _options.DefaultRetryPolicy;
        return await TryCoordinateRetryAsync(
            policy,
            topicName,
            messageType,
            envelope,
            ex,
            pipeline,
            consumer,
            @lock,
            ct);
    }

    private async Task<RetryOutcome> TryCoordinateRetryAsync<T>(
        RetryPolicy policy,
        string topicName,
        Type messageType,
        MessageEnvelope<T> envelope,
        Exception ex,
        MessageProcessingPipeline pipeline,
        IConsumer<T> consumer,
        IdempotencyLock? @lock,
        CancellationToken ct)
    {
        // OperationCanceledException is NEVER retried — it is not a user-handler failure.
        if (ex is OperationCanceledException)
        {
            return RetryOutcome.NotRetryable;
        }

        if (!RetryPolicy.IsEnabled(policy))
        {
            return RetryOutcome.NotRetryable;
        }

        var attempt = envelope.Headers.RetryAttempt;
        var nextAttempt = attempt + 1;

        if (nextAttempt > policy.MaxRetryAttempts)
        {
            _logger.LogWarning(
                "Message {MessageId} on '{Topic}' exhausted all {Max} retry attempts. Routing to DLQ.",
                envelope.Headers.MessageId, topicName, policy.MaxRetryAttempts);

            envelope.Headers.DlqReason = "retries_exhausted";
            envelope.Headers.DlqAttempts = attempt;

            Diagnostics.TalariaDiagnostics.RetryExhausted.Add(1,
                new KeyValuePair<string, object?>("messaging.destination.name", topicName));

            await RouteToDlqAsync(pipeline, consumer, envelope, ex, @lock, "retries_exhausted", ct);
            return RetryOutcome.Exhausted;
        }

        if (_deferralStore is null)
        {
            _logger.LogCritical(
                "Delayed retries are enabled for topic '{Topic}' but no IDeferralStore is registered. " +
                "The message will be routed to the DLQ with reason 'retry_unavailable'. " +
                "Register a deferral store via UseInMemoryDeferralStore() or UseRedisDeferralStore().",
                topicName);

            await RouteToDlqAsync(pipeline, consumer, envelope, ex, @lock, "retry_unavailable", ct);
            return RetryOutcome.Unavailable;
        }

        var delay = ComputeDelay(policy, attempt, _options.MinRetryDelay);
        var headers = BuildRetryHeaders(envelope.Headers, nextAttempt);
        var rootMessageId = headers.RetryRootMessageId ?? headers.MessageId ?? Guid.NewGuid().ToString("N");
        headers.MessageId = $"{rootMessageId}:retry:{nextAttempt}";

        var payloadJson = SerializePayload(envelope.Payload, messageType);

        var deferred = new DeferredMessage(
            Guid.NewGuid(),
            topicName,
            messageType.AssemblyQualifiedName ?? messageType.FullName!,
            payloadJson,
            headers,
            envelope.CorrelationId,
            nextAttempt,
            DateTimeOffset.UtcNow + delay,
            envelope.PartitionKey);

        try
        {
            await _deferralStore.EnqueueAsync(deferred, ct);
        }
        catch (Exception enqueueEx)
        {
            _logger.LogCritical(
                enqueueEx,
                "Failed to enqueue delayed retry for message {MessageId} on '{Topic}'. Routing to DLQ with reason 'retry_unavailable'.",
                envelope.Headers.MessageId, topicName);

            await RouteToDlqAsync(pipeline, consumer, envelope, ex, @lock, "retry_unavailable", ct);
            return RetryOutcome.Unavailable;
        }

        Diagnostics.TalariaDiagnostics.RetryScheduled.Add(1,
            new KeyValuePair<string, object?>("messaging.destination.name", topicName));
        Diagnostics.TalariaDiagnostics.RetryDelay.Record(delay.TotalMilliseconds,
            new KeyValuePair<string, object?>("messaging.destination.name", topicName));

        // Commit the original envelope BEFORE releasing the idempotency lock.
        // The idempotency key is consumerGroup:messageId; the retry copy carries a
        // freshly minted MessageId, so it is NOT gated by the original lock. Committing
        // first ensures the original cannot redeliver and re-run the handler concurrently
        // with the retry copy. If commit fails we leave the lock held: it expires via TTL
        // and the transport redelivers the original, which is safe.
        try
        {
            await consumer.CommitAsync(envelope, ct);
        }
        catch (Exception commitEx)
        {
            _logger.LogError(commitEx, "Failed to commit original envelope after scheduling retry for {MessageId}; it remains uncommitted for redelivery.", envelope.Headers.MessageId);
            return RetryOutcome.Scheduled;
        }

        if (@lock is not null)
        {
            try
            {
                await pipeline.ReleaseAsync(@lock, ct);
            }
            catch (Exception releaseEx)
            {
                _logger.LogError(releaseEx, "Failed to release idempotency lock {MessageId}; it expires via TTL.", @lock.MessageId);
            }
        }

        return RetryOutcome.Scheduled;
    }

    /// <summary>
    /// Routes the message to the DLQ with the given reason, updating metrics.
    /// </summary>
    private async Task RouteToDlqAsync<T>(
        MessageProcessingPipeline pipeline,
        IConsumer<T> consumer,
        MessageEnvelope<T> envelope,
        Exception ex,
        IdempotencyLock? @lock,
        string dlqReason,
        CancellationToken ct)
    {
        envelope.Headers.DlqReason = dlqReason;

        Diagnostics.TalariaDiagnostics.DlqRouted.Add(1,
            new KeyValuePair<string, object?>("messaging.destination.name", envelope.SourceTopic ?? "unknown"));

        await pipeline.FailAsync(@lock, consumer, envelope, ex, dlqReason, ct);
    }

    /// <summary>
    /// Computes the delay for the next retry attempt, applying backoff, optional cap,
    /// and the global minimum delay floor.
    /// </summary>
    internal static TimeSpan ComputeDelay(RetryPolicy policy, int currentAttempt, TimeSpan minDelay)
    {
        var baseDelay = policy.RetryInterval;

        TimeSpan computed = policy.BackoffType switch
        {
            RetryBackoffType.Exponential => ComputeExponentialDelay(baseDelay, currentAttempt, policy.MaxRetryInterval),
            _ => baseDelay,
        };

        if (policy.MaxRetryInterval.HasValue && computed > policy.MaxRetryInterval.Value)
        {
            computed = policy.MaxRetryInterval.Value;
        }

        if (computed < minDelay)
        {
            computed = minDelay;
        }

        return computed;
    }

    private static TimeSpan ComputeExponentialDelay(TimeSpan baseDelay, int currentAttempt, TimeSpan? maxInterval)
    {
        // Stop doubling once the next multiplication would exceed the configured cap
        // or overflow the backing long. This keeps intermediate values positive and bounded.
        var cap = maxInterval ?? TimeSpan.MaxValue;
        var current = baseDelay;

        for (var i = 0; i < currentAttempt; i++)
        {
            if (current >= cap)
            {
                return cap;
            }

            // Detect overflow before it happens.
            if (current.Ticks > long.MaxValue / 2)
            {
                return cap;
            }

            current = TimeSpan.FromTicks(current.Ticks * 2);
        }

        return current > cap ? cap : current;
    }

    /// <summary>
    /// Builds the headers for a retry copy: clones the original, preserves an existing
    /// root message id (or captures the original MessageId as the root), sets the retry
    /// attempt, and strips stale DLQ headers.
    /// </summary>
    internal static MessageHeaders BuildRetryHeaders(MessageHeaders original, int nextAttempt)
    {
        var headers = new MessageHeaders(original);

        // Preserve the root established on the first retry; only capture MessageId when
        // no root is already present. This prevents compounding ids such as
        // "root:retry:1:retry:2" on subsequent attempts.
        if (string.IsNullOrEmpty(headers.RetryRootMessageId))
        {
            var rootMessageId = original.MessageId;
            if (!string.IsNullOrEmpty(rootMessageId))
            {
                headers.RetryRootMessageId = rootMessageId;
            }
        }

        headers.RetryAttempt = nextAttempt;

        // Strip stale DLQ headers from the original failure so the retry copy starts clean.
        headers.DlqReason = null;
        headers.DlqException = null;
        headers.Remove(MessageHeaders.DlqSourceTopicKey);
        headers.Remove(MessageHeaders.DlqAttemptsKey);

        return headers;
    }

    /// <summary>
    /// Serializes the payload for the deferred retry copy. When the payload is already a
    /// <see cref="JsonElement"/> (saga fan-out path), its raw JSON text is reused to avoid
    /// double deserialization. Otherwise the payload is serialized as the declared message type.
    /// </summary>
    private static string SerializePayload<T>(T payload, Type messageType)
    {
        if (payload is JsonElement jsonElement)
        {
            // Clone the element before reading raw text to avoid sharing mutable JsonDocument
            // state with the original envelope.
            return jsonElement.Clone().GetRawText();
        }

        return JsonSerializer.Serialize(payload, messageType);
    }
}
