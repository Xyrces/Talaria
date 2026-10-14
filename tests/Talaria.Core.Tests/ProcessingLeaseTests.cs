using Microsoft.Extensions.Logging.Abstractions;
using Talaria.Core.Abstractions;
using Talaria.Core.Hosting;

namespace Talaria.Core.Tests;

public sealed class ProcessingLeaseTests
{
    private sealed class BusyOnceStore : IIdempotencyStore
    {
        private int _calls;
        public IdempotencyLock Lock { get; } = new("msg", "endpoint", "token");
        public Task<IdempotencyAcquisition> AcquireAsync(string id, string queue, TimeSpan ttl, CancellationToken ct = default)
            => Task.FromResult(Interlocked.Increment(ref _calls) == 1
                ? new IdempotencyAcquisition(IdempotencyStatus.Busy)
                : new IdempotencyAcquisition(IdempotencyStatus.Acquired, Lock));
        public Task<bool> RenewAsync(IdempotencyLock lease, TimeSpan ttl, CancellationToken ct = default) => Task.FromResult(true);
        public Task<IdempotencyLock?> TryAcquireLockAsync(string id, string queue, TimeSpan ttl, CancellationToken ct = default)
            => throw new NotSupportedException();
        public Task MarkCompleteAsync(IdempotencyLock lease, CancellationToken ct = default) => Task.CompletedTask;
        public Task ReleaseLockAsync(IdempotencyLock lease, CancellationToken ct = default) => Task.CompletedTask;
    }

    private sealed class LeaseLostStore : IIdempotencyStore
    {
        public TaskCompletionSource RenewalAttempted { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public Task<IdempotencyAcquisition> AcquireAsync(string id, string queue, TimeSpan ttl, CancellationToken ct = default)
            => throw new NotSupportedException();
        public Task<bool> RenewAsync(IdempotencyLock lease, TimeSpan ttl, CancellationToken ct = default)
        {
            RenewalAttempted.TrySetResult();
            return Task.FromResult(false);
        }
        public Task<IdempotencyLock?> TryAcquireLockAsync(string id, string queue, TimeSpan ttl, CancellationToken ct = default)
            => throw new NotSupportedException();
        public Task MarkCompleteAsync(IdempotencyLock lease, CancellationToken ct = default) => Task.CompletedTask;
        public Task ReleaseLockAsync(IdempotencyLock lease, CancellationToken ct = default) => Task.CompletedTask;
    }

    [Fact]
    public async Task BusyInboxOwnershipIsRetriedUntilItCanBeAcquired()
    {
        var store = new BusyOnceStore();
        var pipeline = new MessageProcessingPipeline(store, new TalariaOptions(), NullLogger.Instance);
        var envelope = new MessageEnvelope<string>
        {
            Payload = "payload",
            Headers = new MessageHeaders { MessageId = "msg" },
            SourceTopic = "topic",
        };

        var gate = await pipeline.AcquireAsync(envelope, "endpoint", CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(3));

        Assert.True(gate.Enabled);
        Assert.Same(store.Lock, gate.Lock);
    }

    [Fact]
    public async Task FailedLeaseRenewalCancelsHandlerAndPreventsCompletion()
    {
        var store = new LeaseLostStore();
        await using var lease = new ProcessingLease(store, new IdempotencyLock("msg", "endpoint", "token"),
            TimeSpan.FromMilliseconds(30), CancellationToken.None);

        await store.RenewalAttempted.Task.WaitAsync(TimeSpan.FromSeconds(3));
        var canceled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var registration = lease.Token.Register(() => canceled.TrySetResult());
        await canceled.Task.WaitAsync(TimeSpan.FromSeconds(3));
        Assert.True(lease.Token.IsCancellationRequested);
        Assert.Throws<InvalidOperationException>(lease.ThrowIfLost);
    }
}
