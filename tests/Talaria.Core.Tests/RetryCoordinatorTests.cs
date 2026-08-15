using Microsoft.Extensions.Logging.Abstractions;
using Talaria.Core.Abstractions;
using Talaria.Core.Hosting;
using Talaria.Core.Registration;

namespace Talaria.Core.Tests;

public class RetryCoordinatorTests
{
    private static TalariaOptions OptionsWithRetries(int maxAttempts = 2, TimeSpan? interval = null) =>
        new()
        {
            ApplicationName = "test-app",
            MinRetryDelay = TimeSpan.FromMilliseconds(10),
            DefaultRetryPolicy = new RetryPolicy
            {
                MaxRetryAttempts = maxAttempts,
                RetryInterval = interval ?? TimeSpan.FromMilliseconds(50),
                BackoffType = RetryBackoffType.Fixed,
            },
        };

    private static MessageEnvelope<T> Envelope<T>(T payload, string? messageId = null, int retryAttempt = 0)
    {
        var headers = new MessageHeaders();
        if (messageId is not null)
        {
            headers.MessageId = messageId;
        }

        if (retryAttempt > 0)
        {
            headers.RetryAttempt = retryAttempt;
        }

        return new MessageEnvelope<T>
        {
            Payload = payload,
            Headers = headers,
            SourceTopic = "test.topic",
            PartitionKey = "part-1",
        };
    }

    [Fact]
    public async Task TryCoordinateTopicRetryAsync_DisabledPolicy_FallsThroughToDLQ()
    {
        var store = new FakeDeferralStore();
        var options = new TalariaOptions { ApplicationName = "test-app" };
        var coordinator = new RetryCoordinator(store, options, NullLogger.Instance);
        var pipeline = new MessageProcessingPipeline(null, options, NullLogger.Instance);
        var consumer = new FakeConsumer<string>();
        var envelope = Envelope<string>("payload", "msg-1");
        var registration = new TopicRegistration
        {
            TopicName = "test.topic",
            MessageType = typeof(string),
            Handler = (_, _, _) => Task.CompletedTask,
        };

        var outcome = await coordinator.TryCoordinateTopicRetryAsync(
            registration, pipeline, consumer, envelope, new InvalidOperationException("boom"), null, default);

        Assert.Equal(RetryCoordinator.RetryOutcome.NotRetryable, outcome);
        Assert.Empty(store.Enqueued);
        Assert.Empty(consumer.Committed);
        Assert.Empty(consumer.Nacked);
    }

    [Fact]
    public async Task TryCoordinateTopicRetryAsync_Success_SchedulesDeferredCopy()
    {
        var store = new FakeDeferralStore();
        var options = OptionsWithRetries(maxAttempts: 2);
        var coordinator = new RetryCoordinator(store, options, NullLogger.Instance);
        var pipeline = new MessageProcessingPipeline(null, options, NullLogger.Instance);
        var consumer = new FakeConsumer<string>();
        var envelope = Envelope<string>("payload", "msg-1");
        var registration = new TopicRegistration
        {
            TopicName = "test.topic",
            MessageType = typeof(string),
            Handler = (_, _, _) => Task.CompletedTask,
        };

        var outcome = await coordinator.TryCoordinateTopicRetryAsync(
            registration, pipeline, consumer, envelope, new InvalidOperationException("boom"), null, default);

        Assert.Equal(RetryCoordinator.RetryOutcome.Scheduled, outcome);
        Assert.Single(store.Enqueued);
        var deferred = store.Enqueued[0];
        Assert.Equal("test.topic", deferred.Topic);
        Assert.Equal(typeof(string).AssemblyQualifiedName, deferred.MessageType);
        Assert.Equal(1, deferred.Attempt);
        Assert.Equal("part-1", deferred.PartitionKey);
        Assert.True(deferred.DueAt > DateTimeOffset.UtcNow);
        Assert.Equal("msg-1:retry:1", deferred.Headers.MessageId);
        Assert.Equal("msg-1", deferred.Headers.RetryRootMessageId);
        Assert.Equal(1, deferred.Headers.RetryAttempt);
        Assert.Null(deferred.Headers.DlqReason);
        Assert.Null(deferred.Headers.DlqException);
        Assert.Single(consumer.Committed);
        Assert.Empty(consumer.Nacked);
    }

    [Fact]
    public async Task TryCoordinateTopicRetryAsync_Exhausted_RoutesToDLQ()
    {
        var store = new FakeDeferralStore();
        var options = OptionsWithRetries(maxAttempts: 2);
        var coordinator = new RetryCoordinator(store, options, NullLogger.Instance);
        var pipeline = new MessageProcessingPipeline(null, options, NullLogger.Instance);
        var consumer = new FakeConsumer<string>();
        var envelope = Envelope<string>("payload", "msg-1", retryAttempt: 2);
        var registration = new TopicRegistration
        {
            TopicName = "test.topic",
            MessageType = typeof(string),
            Handler = (_, _, _) => Task.CompletedTask,
        };

        var outcome = await coordinator.TryCoordinateTopicRetryAsync(
            registration, pipeline, consumer, envelope, new InvalidOperationException("boom"), null, default);

        Assert.Equal(RetryCoordinator.RetryOutcome.Exhausted, outcome);
        Assert.Empty(store.Enqueued);
        Assert.Single(consumer.Nacked);
        Assert.Equal("retries_exhausted", consumer.Nacked[0].Headers.DlqReason);
        Assert.Equal(2, consumer.Nacked[0].Headers.DlqAttempts);
    }

    [Fact]
    public async Task TryCoordinateTopicRetryAsync_NoDeferralStore_RoutesToDLQ_AsRetryUnavailable()
    {
        var options = OptionsWithRetries(maxAttempts: 2);
        var coordinator = new RetryCoordinator(null, options, NullLogger.Instance);
        var pipeline = new MessageProcessingPipeline(null, options, NullLogger.Instance);
        var consumer = new FakeConsumer<string>();
        var envelope = Envelope<string>("payload", "msg-1");
        var registration = new TopicRegistration
        {
            TopicName = "test.topic",
            MessageType = typeof(string),
            Handler = (_, _, _) => Task.CompletedTask,
        };

        var outcome = await coordinator.TryCoordinateTopicRetryAsync(
            registration, pipeline, consumer, envelope, new InvalidOperationException("boom"), null, default);

        Assert.Equal(RetryCoordinator.RetryOutcome.Unavailable, outcome);
        Assert.Single(consumer.Nacked);
        Assert.Equal("retry_unavailable", consumer.Nacked[0].Headers.DlqReason);
    }

    [Fact]
    public async Task TryCoordinateTopicRetryAsync_EnqueueThrows_RoutesToDLQ_AsRetryUnavailable()
    {
        var store = new FakeDeferralStore { ThrowOnEnqueue = true };
        var options = OptionsWithRetries(maxAttempts: 2);
        var coordinator = new RetryCoordinator(store, options, NullLogger.Instance);
        var pipeline = new MessageProcessingPipeline(null, options, NullLogger.Instance);
        var consumer = new FakeConsumer<string>();
        var envelope = Envelope<string>("payload", "msg-1");
        var registration = new TopicRegistration
        {
            TopicName = "test.topic",
            MessageType = typeof(string),
            Handler = (_, _, _) => Task.CompletedTask,
        };

        var outcome = await coordinator.TryCoordinateTopicRetryAsync(
            registration, pipeline, consumer, envelope, new InvalidOperationException("boom"), null, default);

        Assert.Equal(RetryCoordinator.RetryOutcome.Unavailable, outcome);
        Assert.Single(consumer.Nacked);
        Assert.Equal("retry_unavailable", consumer.Nacked[0].Headers.DlqReason);
    }

    [Fact]
    public async Task TryCoordinateTopicRetryAsync_OperationCanceledException_IsNotRetried()
    {
        var store = new FakeDeferralStore();
        var options = OptionsWithRetries(maxAttempts: 2);
        var coordinator = new RetryCoordinator(store, options, NullLogger.Instance);
        var pipeline = new MessageProcessingPipeline(null, options, NullLogger.Instance);
        var consumer = new FakeConsumer<string>();
        var envelope = Envelope<string>("payload", "msg-1");
        var registration = new TopicRegistration
        {
            TopicName = "test.topic",
            MessageType = typeof(string),
            Handler = (_, _, _) => Task.CompletedTask,
        };

        var outcome = await coordinator.TryCoordinateTopicRetryAsync(
            registration, pipeline, consumer, envelope, new OperationCanceledException(), null, default);

        Assert.Equal(RetryCoordinator.RetryOutcome.NotRetryable, outcome);
        Assert.Empty(store.Enqueued);
        Assert.Empty(consumer.Nacked);
    }

    [Fact]
    public async Task TryCoordinateTopicRetryAsync_ReleasesLock_AndCommitsOriginal()
    {
        var store = new FakeDeferralStore();
        var options = OptionsWithRetries(maxAttempts: 2);
        var idempotencyStore = new FakeIdempotencyStore();
        var pipeline = new MessageProcessingPipeline(idempotencyStore, options, NullLogger.Instance);
        var coordinator = new RetryCoordinator(store, options, NullLogger.Instance);
        var consumer = new FakeConsumer<string>();
        var envelope = Envelope<string>("payload", "msg-1");
        var lck = await idempotencyStore.TryAcquireLockAsync("msg-1", "cg", TimeSpan.FromMinutes(1), default);
        Assert.NotNull(lck);
        var registration = new TopicRegistration
        {
            TopicName = "test.topic",
            MessageType = typeof(string),
            Handler = (_, _, _) => Task.CompletedTask,
        };

        var outcome = await coordinator.TryCoordinateTopicRetryAsync(
            registration, pipeline, consumer, envelope, new InvalidOperationException("boom"), lck, default);

        Assert.Equal(RetryCoordinator.RetryOutcome.Scheduled, outcome);
        Assert.Contains(lck, idempotencyStore.Released);
        Assert.Single(consumer.Committed);
    }

    [Fact]
    public async Task TryCoordinateTopicRetryAsync_TopicRetryPolicyOverridesDefault()
    {
        var store = new FakeDeferralStore();
        var options = new TalariaOptions
        {
            ApplicationName = "test-app",
            MinRetryDelay = TimeSpan.FromMilliseconds(10),
            DefaultRetryPolicy = new RetryPolicy
            {
                MaxRetryAttempts = 5,
                RetryInterval = TimeSpan.FromSeconds(1),
                BackoffType = RetryBackoffType.Fixed,
            },
        };
        var coordinator = new RetryCoordinator(store, options, NullLogger.Instance);
        var pipeline = new MessageProcessingPipeline(null, options, NullLogger.Instance);
        var consumer = new FakeConsumer<string>();
        var envelope = Envelope<string>("payload", "msg-1");
        var registration = new TopicRegistration
        {
            TopicName = "test.topic",
            MessageType = typeof(string),
            RetryPolicy = new RetryPolicy
            {
                MaxRetryAttempts = 1,
                RetryInterval = TimeSpan.FromMilliseconds(50),
                BackoffType = RetryBackoffType.Fixed,
            },
            Handler = (_, _, _) => Task.CompletedTask,
        };

        var outcome = await coordinator.TryCoordinateTopicRetryAsync(
            registration, pipeline, consumer, envelope, new InvalidOperationException("boom"), null, default);

        Assert.Equal(RetryCoordinator.RetryOutcome.Scheduled, outcome);
    }

    [Fact]
    public async Task TryCoordinateSagaRetryAsync_Success_SchedulesDeferredCopy()
    {
        var store = new FakeDeferralStore();
        var options = OptionsWithRetries(maxAttempts: 2);
        var coordinator = new RetryCoordinator(store, options, NullLogger.Instance);
        var pipeline = new MessageProcessingPipeline(null, options, NullLogger.Instance);
        var consumer = new FakeConsumer<System.Text.Json.JsonElement>();
        using var doc = System.Text.Json.JsonDocument.Parse("{\"id\":\"x\"}");
        var envelope = Envelope<System.Text.Json.JsonElement>(doc.RootElement, "saga-msg-1");

        var outcome = await coordinator.TryCoordinateSagaRetryAsync(
            "saga.topic", typeof(SagaRetryPayload), pipeline, consumer, envelope,
            new InvalidOperationException("boom"), null, default);

        Assert.Equal(RetryCoordinator.RetryOutcome.Scheduled, outcome);
        Assert.Single(store.Enqueued);
        Assert.Equal("saga.topic", store.Enqueued[0].Topic);
        Assert.Equal(typeof(SagaRetryPayload).AssemblyQualifiedName, store.Enqueued[0].MessageType);
        Assert.Equal("{\"id\":\"x\"}", store.Enqueued[0].PayloadJson);
    }

    private sealed class SagaRetryPayload { public string Id { get; set; } = ""; }

    [Fact]
    public void BuildRetryHeaders_PreservesExistingRootMessageId_AcrossAttempts()
    {
        var original = new MessageHeaders
        {
            MessageId = "root:retry:1",
            RetryRootMessageId = "root",
            RetryAttempt = 1,
        };

        var cloned = RetryCoordinator.BuildRetryHeaders(original, nextAttempt: 2);

        Assert.Equal("root", cloned.RetryRootMessageId);
        Assert.Equal("root:retry:1", cloned.MessageId);
        Assert.Equal(2, cloned.RetryAttempt);
    }

    [Fact]
    public void ComputeDelay_Exponential_DoesNotOverflow_ForLargeAttempts()
    {
        var policy = new RetryPolicy
        {
            MaxRetryAttempts = 100,
            RetryInterval = TimeSpan.FromMilliseconds(1),
            BackoffType = RetryBackoffType.Exponential,
            MaxRetryInterval = TimeSpan.FromMinutes(5),
        };

        var delay = RetryCoordinator.ComputeDelay(policy, currentAttempt: 99, TimeSpan.FromMilliseconds(1));

        Assert.Equal(TimeSpan.FromMinutes(5), delay);
        Assert.True(delay > TimeSpan.Zero);
    }

    [Fact]
    public void ComputeDelay_Exponential_NoCap_DoublesUntilBounded()
    {
        var policy = new RetryPolicy
        {
            MaxRetryAttempts = 100,
            RetryInterval = TimeSpan.FromMilliseconds(1),
            BackoffType = RetryBackoffType.Exponential,
        };

        var delay = RetryCoordinator.ComputeDelay(policy, currentAttempt: 99, TimeSpan.FromMilliseconds(1));

        Assert.True(delay > TimeSpan.Zero);
        Assert.True(delay <= TimeSpan.MaxValue);
    }

    [Fact]
    public async Task TryCoordinateTopicRetryAsync_CommitsBeforeReleasingLock()
    {
        var store = new FakeDeferralStore();
        var options = OptionsWithRetries(maxAttempts: 2);
        var idempotencyStore = new FakeIdempotencyStore();
        var pipeline = new MessageProcessingPipeline(idempotencyStore, options, NullLogger.Instance);
        var coordinator = new RetryCoordinator(store, options, NullLogger.Instance);
        var consumer = new FakeConsumer<string>();
        var envelope = Envelope<string>("payload", "msg-1");
        var lck = await idempotencyStore.TryAcquireLockAsync("msg-1", "cg", TimeSpan.FromMinutes(1), default);
        Assert.NotNull(lck);
        var registration = new TopicRegistration
        {
            TopicName = "test.topic",
            MessageType = typeof(string),
            Handler = (_, _, _) => Task.CompletedTask,
        };

        var outcome = await coordinator.TryCoordinateTopicRetryAsync(
            registration, pipeline, consumer, envelope, new InvalidOperationException("boom"), lck, default);

        Assert.Equal(RetryCoordinator.RetryOutcome.Scheduled, outcome);
        Assert.Single(consumer.Committed);
        Assert.Contains(lck, idempotencyStore.Released);
        // Commit must be recorded before the lock release.
        Assert.True(consumer.CommitCallCount <= idempotencyStore.ReleaseCallCount);
    }

    [Fact]
    public async Task TryCoordinateTopicRetryAsync_CommitFailure_DoesNotReleaseLock()
    {
        var store = new FakeDeferralStore();
        var options = OptionsWithRetries(maxAttempts: 2);
        var idempotencyStore = new FakeIdempotencyStore();
        var pipeline = new MessageProcessingPipeline(idempotencyStore, options, NullLogger.Instance);
        var coordinator = new RetryCoordinator(store, options, NullLogger.Instance);
        var consumer = new FakeConsumer<string> { ThrowOnCommit = true };
        var envelope = Envelope<string>("payload", "msg-1");
        var lck = await idempotencyStore.TryAcquireLockAsync("msg-1", "cg", TimeSpan.FromMinutes(1), default);
        Assert.NotNull(lck);
        var registration = new TopicRegistration
        {
            TopicName = "test.topic",
            MessageType = typeof(string),
            Handler = (_, _, _) => Task.CompletedTask,
        };

        var outcome = await coordinator.TryCoordinateTopicRetryAsync(
            registration, pipeline, consumer, envelope, new InvalidOperationException("boom"), lck, default);

        Assert.Equal(RetryCoordinator.RetryOutcome.Scheduled, outcome);
        Assert.Empty(idempotencyStore.Released);
    }

    [Fact]
    public void BuildRetryHeaders_SetsAttemptAndRootMessageId_AndStripsDlqHeaders()
    {
        var original = new MessageHeaders
        {
            MessageId = "root-1",
            DlqReason = "old_reason",
            DlqException = "old_exception",
            [MessageHeaders.DlqSourceTopicKey] = "old.topic",
            [MessageHeaders.DlqAttemptsKey] = "3",
        };

        var cloned = RetryCoordinator.BuildRetryHeaders(original, nextAttempt: 2);

        Assert.Equal("root-1", cloned.RetryRootMessageId);
        Assert.Equal(2, cloned.RetryAttempt);
        Assert.Equal("root-1", cloned.MessageId);
        Assert.Null(cloned.DlqReason);
        Assert.Null(cloned.DlqException);
        Assert.False(cloned.ContainsKey(MessageHeaders.DlqSourceTopicKey));
        Assert.False(cloned.ContainsKey(MessageHeaders.DlqAttemptsKey));
    }

    private sealed class FakeDeferralStore : IDeferralStore
    {
        public List<DeferredMessage> Enqueued { get; } = new();
        public bool ThrowOnEnqueue { get; set; }

        public Task EnqueueAsync(DeferredMessage message, CancellationToken ct = default)
        {
            if (ThrowOnEnqueue)
            {
                throw new InvalidOperationException("enqueue failed");
            }

            Enqueued.Add(message);
            return Task.CompletedTask;
        }

        public Task<IReadOnlyList<LeasedDeferral>> AcquireDueAsync(DateTimeOffset now, TimeSpan leaseDuration, int maxBatch, CancellationToken ct = default)
            => Task.FromResult<IReadOnlyList<LeasedDeferral>>(Array.Empty<LeasedDeferral>());

        public Task<bool> CompleteAsync(DeferralLease lease, CancellationToken ct = default) => Task.FromResult(true);

        public Task<bool> AbandonAsync(DeferralLease lease, DateTimeOffset? visibleAt = null, CancellationToken ct = default) => Task.FromResult(true);
    }

    private sealed class FakeConsumer<T> : IConsumer<T>
    {
        public List<MessageEnvelope<T>> Committed { get; } = new();
        public List<MessageEnvelope<T>> Nacked { get; } = new();
        public int CommitCallCount { get; private set; }
        public bool ThrowOnCommit { get; set; }

        public IAsyncEnumerable<MessageEnvelope<T>> ConsumeAsync(CancellationToken ct = default) => throw new NotImplementedException();

        public Task CommitAsync(MessageEnvelope<T> envelope, CancellationToken ct = default)
        {
            CommitCallCount++;
            if (ThrowOnCommit)
            {
                throw new InvalidOperationException("commit failed");
            }

            Committed.Add(envelope);
            return Task.CompletedTask;
        }

        public Task NackAsync(MessageEnvelope<T> envelope, CancellationToken ct = default)
        {
            Nacked.Add(envelope);
            return Task.CompletedTask;
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class FakeIdempotencyStore : IIdempotencyStore
    {
        private readonly Dictionary<string, IdempotencyLock> _locks = new();
        public List<IdempotencyLock> Released { get; } = new();
        public int ReleaseCallCount { get; private set; }

        public Task<IdempotencyLock?> TryAcquireLockAsync(string messageId, string consumerGroup, TimeSpan ttl, CancellationToken ct = default)
        {
            var lck = new IdempotencyLock(messageId, consumerGroup, "token-1");
            _locks[messageId] = lck;
            return Task.FromResult<IdempotencyLock?>(lck);
        }

        public Task MarkCompleteAsync(IdempotencyLock lck, CancellationToken ct = default) => Task.CompletedTask;

        public Task ReleaseLockAsync(IdempotencyLock lck, CancellationToken ct = default)
        {
            ReleaseCallCount++;
            Released.Add(lck);
            return Task.CompletedTask;
        }
    }
}
