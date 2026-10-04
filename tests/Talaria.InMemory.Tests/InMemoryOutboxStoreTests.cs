using Talaria.Core.Abstractions;
using Talaria.Transports.InMemory;
using Xunit;

namespace Talaria.InMemory.Tests;

public class InMemoryOutboxStoreTests
{
    private static readonly TimeSpan Lease = TimeSpan.FromSeconds(30);

    private static OutboxMessage Message(string topic = "orders-out", string? partitionKey = null) =>
        new(Guid.NewGuid(), topic, typeof(object).AssemblyQualifiedName!, "{}",
            new MessageHeaders { MessageId = Guid.NewGuid().ToString("N") }, DateTimeOffset.UtcNow, partitionKey);

    [Fact]
    public async Task AcquirePendingAsync_HidesLeasedEntries_UntilLeaseExpires()
    {
        var store = new InMemoryOutboxStore();
        var stateStore = new InMemoryStateStore<State>(store);

        await stateStore.TransitionAsync("c1", new State(), [Message()]);
        var now = DateTimeOffset.UtcNow;

        var first = await store.AcquirePendingAsync(now, Lease, maxBatch: 10);
        var second = await store.AcquirePendingAsync(now.AddSeconds(10), Lease, maxBatch: 10);

        Assert.Single(first);
        Assert.Empty(second);
        Assert.Equal(1, store.Count);
    }

    [Fact]
    public async Task AcquirePendingAsync_ExpiredLease_Reacquires_WithBumpedToken()
    {
        var store = new InMemoryOutboxStore();
        var stateStore = new InMemoryStateStore<State>(store);

        await stateStore.TransitionAsync("c1", new State(), [Message()]);
        var now = DateTimeOffset.UtcNow;

        var first = Assert.Single(await store.AcquirePendingAsync(now, Lease, maxBatch: 10));
        var again = Assert.Single(await store.AcquirePendingAsync(now.Add(Lease).AddSeconds(1), Lease, maxBatch: 10));

        Assert.Equal(first.Message.Id, again.Message.Id);
        Assert.True(again.Lease.Token > first.Lease.Token);
    }

    [Fact]
    public async Task CompleteAsync_RemovesEntry_OnlyWithCurrentToken()
    {
        var store = new InMemoryOutboxStore();
        var stateStore = new InMemoryStateStore<State>(store);

        await stateStore.TransitionAsync("c1", new State(), [Message()]);
        var now = DateTimeOffset.UtcNow;

        var first = Assert.Single(await store.AcquirePendingAsync(now, Lease, maxBatch: 10));
        var second = Assert.Single(await store.AcquirePendingAsync(now.Add(Lease).AddSeconds(1), Lease, maxBatch: 10));

        Assert.False(await store.CompleteAsync(first.Lease));
        Assert.Equal(1, store.Count);

        Assert.True(await store.CompleteAsync(second.Lease));
        Assert.Equal(0, store.Count);
    }

    [Fact]
    public async Task AbandonAsync_Reschedules_OnlyWithCurrentToken()
    {
        var store = new InMemoryOutboxStore();
        var stateStore = new InMemoryStateStore<State>(store);

        await stateStore.TransitionAsync("c1", new State(), [Message()]);
        var now = DateTimeOffset.UtcNow;

        var leased = Assert.Single(await store.AcquirePendingAsync(now, Lease, maxBatch: 10));

        Assert.False(await store.AbandonAsync(leased.Lease with { Token = leased.Lease.Token + 99 }));

        var retryAt = now.AddMinutes(2);
        Assert.True(await store.AbandonAsync(leased.Lease, retryAt));

        Assert.Empty(await store.AcquirePendingAsync(now.AddMinutes(1), Lease, maxBatch: 10));
        Assert.Single(await store.AcquirePendingAsync(retryAt, Lease, maxBatch: 10));
    }

    [Fact]
    public async Task AcquirePendingAsync_HonorsMaxBatch_InStagedOrder()
    {
        var store = new InMemoryOutboxStore();
        var stateStore = new InMemoryStateStore<State>(store);

        await stateStore.TransitionAsync("c1", new State(), [Message(), Message(), Message()]);
        var now = DateTimeOffset.UtcNow;

        var batch = await store.AcquirePendingAsync(now, Lease, maxBatch: 2);
        Assert.Equal(2, batch.Count);
        Assert.Equal(3, store.Count);
    }

    [Fact]
    public async Task AcquirePendingAsync_RoundTripsPartitionKey_WhenPresent()
    {
        var store = new InMemoryOutboxStore();
        var stateStore = new InMemoryStateStore<State>(store);

        await stateStore.TransitionAsync("c1", new State(), [Message(partitionKey: "order-partition-7")]);

        var leased = Assert.Single(await store.AcquirePendingAsync(DateTimeOffset.UtcNow, Lease, maxBatch: 10));
        Assert.Equal("order-partition-7", leased.Message.PartitionKey);
    }

    [Fact]
    public async Task AcquirePendingAsync_RoundTripsNullPartitionKey_WhenAbsent()
    {
        var store = new InMemoryOutboxStore();
        var stateStore = new InMemoryStateStore<State>(store);

        await stateStore.TransitionAsync("c1", new State(), [Message()]);

        var leased = Assert.Single(await store.AcquirePendingAsync(DateTimeOffset.UtcNow, Lease, maxBatch: 10));
        Assert.Null(leased.Message.PartitionKey);
    }

    [Fact]
    public async Task StateTransitionAndAcquisitionCloneOutboxHeaders()
    {
        var outbox = new InMemoryOutboxStore();
        var state = new InMemoryStateStore<State>(outbox);
        var headers = new MessageHeaders { MessageId = "before" };
        var message = new OutboxMessage(Guid.NewGuid(), "orders-out", "Message", "{}", headers, DateTimeOffset.UtcNow);

        await state.TransitionAsync("saga", new State(), [message]);
        headers.MessageId = "changed-by-caller";
        var acquired = Assert.Single(await outbox.AcquirePendingAsync(DateTimeOffset.UtcNow, TimeSpan.FromSeconds(5), 1));
        Assert.Equal("before", acquired.Message.Headers.MessageId);
        acquired.Message.Headers.MessageId = "changed-by-reader";
        await outbox.AbandonAsync(acquired.Lease);
        var reacquired = Assert.Single(await outbox.AcquirePendingAsync(DateTimeOffset.UtcNow, TimeSpan.FromSeconds(5), 1));
        Assert.Equal("before", reacquired.Message.Headers.MessageId);
    }

    [Fact]
    public async Task InvalidOutboxBatchDoesNotPartiallyTransitionSagaState()
    {
        var outbox = new InMemoryOutboxStore();
        var state = new InMemoryStateStore<State>(outbox);
        var id = Guid.NewGuid();
        var duplicateBatch = new[]
        {
            new OutboxMessage(id, "one", "Message", "{}", new MessageHeaders(), DateTimeOffset.UtcNow),
            new OutboxMessage(id, "two", "Message", "{}", new MessageHeaders(), DateTimeOffset.UtcNow),
        };

        await Assert.ThrowsAsync<ArgumentException>(() => state.TransitionAsync("saga", new State(), duplicateBatch));

        Assert.Null(await state.GetAsync("saga"));
        Assert.Equal(0, outbox.Count);
    }

    [Fact]
    public async Task ExistingOutboxIdDoesNotPartiallyTransitionSagaState()
    {
        var outbox = new InMemoryOutboxStore();
        var state = new InMemoryStateStore<State>(outbox);
        var id = Guid.NewGuid();
        await state.TransitionAsync("saga", new State { Id = "before" },
            [new OutboxMessage(id, "one", "Message", "{}", new MessageHeaders(), DateTimeOffset.UtcNow)]);

        await Assert.ThrowsAsync<InvalidOperationException>(() => state.TransitionAsync("saga", new State { Id = "after" },
            [new OutboxMessage(id, "two", "Message", "{}", new MessageHeaders(), DateTimeOffset.UtcNow)]));

        Assert.Equal("before", (await state.GetAsync("saga"))!.Id);
        Assert.Equal(1, outbox.Count);
    }

    [Fact]
    public async Task OldLeaseCannotCompleteReenqueuedSameId()
    {
        var outbox = new InMemoryOutboxStore();
        var state = new InMemoryStateStore<State>(outbox);
        var id = Guid.NewGuid();
        OutboxMessage MessageWithId() => new(id, "orders", "Message", "{}", new MessageHeaders(), DateTimeOffset.UtcNow);

        await state.TransitionAsync("first", new State(), [MessageWithId()]);
        var stale = Assert.Single(await outbox.AcquirePendingAsync(DateTimeOffset.UtcNow, Lease, 1));
        Assert.True(await outbox.CompleteAsync(stale.Lease));

        await state.TransitionAsync("second", new State(), [MessageWithId()]);
        var current = Assert.Single(await outbox.AcquirePendingAsync(DateTimeOffset.UtcNow, Lease, 1));
        Assert.False(await outbox.CompleteAsync(stale.Lease));
        Assert.True(await outbox.CompleteAsync(current.Lease));
    }

    private class State { public string Id { get; set; } = ""; }
}
