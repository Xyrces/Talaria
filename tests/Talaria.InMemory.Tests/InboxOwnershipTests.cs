using Talaria.Core.Abstractions;
using Talaria.Transports.InMemory;
using Xunit;

namespace Talaria.InMemory.Tests;

public class InboxOwnershipTests
{
    private sealed class Clock : TimeProvider
    {
        public DateTimeOffset Now = DateTimeOffset.UtcNow;
        public override DateTimeOffset GetUtcNow() => Now;
    }

    [Fact]
    public async Task BusyAndCompletedAreDistinct_AndReleaseCannotEraseCompletion()
    {
        var store = new InMemoryIdempotencyStore();
        var first = await store.AcquireAsync("id", "endpoint", TimeSpan.FromMinutes(1));
        Assert.Equal(IdempotencyStatus.Acquired, first.Status);
        Assert.Equal(IdempotencyStatus.Busy, (await store.AcquireAsync("id", "endpoint", TimeSpan.FromMinutes(1))).Status);
        await store.MarkCompleteAsync(first.Lock!);
        await store.ReleaseLockAsync(first.Lock!);
        Assert.Equal(IdempotencyStatus.Completed, (await store.AcquireAsync("id", "endpoint", TimeSpan.FromMinutes(1))).Status);
    }

    [Fact]
    public async Task ExpiredOwnerCannotCompleteRenewOrReleaseNewOwnersLease()
    {
        var clock = new Clock();
        var store = new InMemoryIdempotencyStore(clock);
        var first = (await store.AcquireAsync("id", "endpoint", TimeSpan.FromSeconds(1))).Lock!;
        clock.Now += TimeSpan.FromSeconds(2);
        var second = (await store.AcquireAsync("id", "endpoint", TimeSpan.FromSeconds(1))).Lock!;
        await Assert.ThrowsAsync<IdempotencyLeaseLostException>(() => store.MarkCompleteAsync(first));
        Assert.False(await store.RenewAsync(first, TimeSpan.FromMinutes(1)));
        await store.ReleaseLockAsync(first);
        Assert.Equal(IdempotencyStatus.Busy, (await store.AcquireAsync("id", "endpoint", TimeSpan.FromMinutes(1))).Status);
        Assert.True(await store.RenewAsync(second, TimeSpan.FromMinutes(1)));
        clock.Now += TimeSpan.FromSeconds(2);
        await store.MarkCompleteAsync(second);
        Assert.Equal(IdempotencyStatus.Completed, (await store.AcquireAsync("id", "endpoint", TimeSpan.FromMinutes(1))).Status);
    }

    [Fact]
    public async Task ExpiredInboxEntriesArePrunedEvenWhenTheirKeysAreNeverReused()
    {
        var clock = new Clock();
        var store = new InMemoryIdempotencyStore(clock);
        var old = await store.AcquireAsync("old", "endpoint", TimeSpan.FromSeconds(1));
        await store.MarkCompleteAsync(old.Lock!);
        clock.Now += TimeSpan.FromDays(31);

        var current = await store.AcquireAsync("current", "endpoint", TimeSpan.FromMinutes(1));

        Assert.Equal(IdempotencyStatus.Acquired, current.Status);
        Assert.Equal(1, store.Count);
    }

    [Fact]
    public async Task HandlerMutationDoesNotChangeStoredStateUntilSaved()
    {
        var store = new InMemoryStateStore<State>();
        var original = new State { Count = 1 };
        await store.SaveAsync("id", original);
        original.Count = 2;
        var snapshot = await store.GetAsync("id");
        Assert.Equal(1, snapshot!.Count);
        snapshot.Count = 3;
        Assert.Equal(1, (await store.GetAsync("id"))!.Count);
    }

    public sealed class State { public int Count { get; set; } }

    [Fact]
    public async Task ConcurrentSagaSnapshotsCannotOverwriteCommittedState_AndCompletionSurvivesReplay()
    {
        var store = new InMemoryStateStore<State>();
        var first = await store.ReadSnapshotAsync("id", "first");
        var second = await store.ReadSnapshotAsync("id", "second");
        Assert.Equal(SagaCommitStatus.Committed, await store.TryTransitionAsync("id", "first", first.Version, new State { Count = 1 }, []));
        Assert.Equal(SagaCommitStatus.Conflict, await store.TryTransitionAsync("id", "second", second.Version, new State { Count = 2 }, []));
        Assert.Equal(1, (await store.GetAsync("id"))!.Count);
        var current = await store.ReadSnapshotAsync("id", "complete");
        Assert.Equal(SagaCommitStatus.Committed, await store.TryTransitionAsync("id", "complete", current.Version, null, []));
        Assert.Equal(SagaCommitStatus.AlreadyProcessed, await store.TryTransitionAsync("id", "first", first.Version, new State(), []));
        Assert.True((await store.ReadSnapshotAsync("id", "first")).IsCompleted);
        Assert.Null(await store.GetAsync("id"));
    }

    [Fact]
    public async Task LegacyWritesInvalidateSnapshots_AndClearRemovesReceipts()
    {
        var store = new InMemoryStateStore<State>();
        var before = await store.ReadSnapshotAsync("id", "message");
        await store.TransitionAsync("id", new State { Count = 1 }, []);
        Assert.Equal(SagaCommitStatus.Conflict, await store.TryTransitionAsync("id", "message", before.Version, new State(), []));
        var current = await store.ReadSnapshotAsync("id", "message");
        await store.TryTransitionAsync("id", "message", current.Version, null, []);
        store.Clear();
        Assert.Equal(new SagaSnapshot<State>(null, 0, false, false), await store.ReadSnapshotAsync("id", "message"));
    }

    [Fact]
    public async Task ExpiredColdSagaHistoriesArePrunedAndSaveAfterExpiryRetainsNewState()
    {
        var clock = new Clock();
        var store = new InMemoryStateStore<State>(timeProvider: clock);
        var first = await store.ReadSnapshotAsync("cold", "message");
        await store.TryTransitionAsync("cold", "message", first.Version, new State { Count = 1 }, []);
        clock.Now += TimeSpan.FromDays(31);

        await store.SaveAsync("new", new State { Count = 2 });

        Assert.Equal(1, store.TrackedCorrelationCount);
        Assert.Null(await store.GetAsync("cold"));
        Assert.Equal(2, (await store.GetAsync("new"))!.Count);
    }
}
