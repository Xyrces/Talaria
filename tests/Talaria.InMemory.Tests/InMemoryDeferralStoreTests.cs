using Talaria.Core.Abstractions;
using Talaria.Transports.InMemory;
using Xunit;

namespace Talaria.InMemory.Tests;

public class InMemoryDeferralStoreTests
{
    private static readonly TimeSpan Lease = TimeSpan.FromSeconds(30);

    private static DeferredMessage Message(DateTimeOffset dueAt, string topic = "orders", string? partitionKey = null) =>
        new(Guid.NewGuid(), topic, typeof(object).AssemblyQualifiedName!, "{}",
            new MessageHeaders(), "corr-1", Attempt: 1, dueAt, partitionKey);

    [Fact]
    public async Task AcquireDueAsync_ReturnsOnlyDueMessages_InDueOrder()
    {
        var store = new InMemoryDeferralStore();
        var now = DateTimeOffset.UtcNow;
        var future = Message(now.AddMinutes(5));
        var dueLater = Message(now.AddMilliseconds(-1));
        var dueFirst = Message(now.AddMinutes(-2));

        await store.EnqueueAsync(future);
        await store.EnqueueAsync(dueLater);
        await store.EnqueueAsync(dueFirst);

        var due = await store.AcquireDueAsync(now, Lease, maxBatch: 10);

        Assert.Equal(2, due.Count);
        Assert.Equal(dueFirst.Id, due[0].Message.Id);
        Assert.Equal(dueLater.Id, due[1].Message.Id);
        Assert.Equal(3, store.Count); // leased entries stay in the store
    }

    [Fact]
    public async Task AcquireDueAsync_HidesLeasedEntries_UntilLeaseExpires()
    {
        var store = new InMemoryDeferralStore();
        var now = DateTimeOffset.UtcNow;
        await store.EnqueueAsync(Message(now.AddMinutes(-1)));

        var first = await store.AcquireDueAsync(now, Lease, maxBatch: 10);
        var second = await store.AcquireDueAsync(now.AddSeconds(10), Lease, maxBatch: 10);

        Assert.Single(first);
        Assert.Empty(second);
    }

    [Fact]
    public async Task AcquireDueAsync_ExpiredLease_CanBeReacquired_WithBumpedToken()
    {
        var store = new InMemoryDeferralStore();
        var now = DateTimeOffset.UtcNow;
        var message = Message(now.AddMinutes(-1));
        await store.EnqueueAsync(message);

        var first = await store.AcquireDueAsync(now, Lease, maxBatch: 10);
        var reacquired = await store.AcquireDueAsync(now.Add(Lease).AddSeconds(1), Lease, maxBatch: 10);

        var original = Assert.Single(first);
        var again = Assert.Single(reacquired);
        Assert.Equal(message.Id, again.Message.Id);
        Assert.True(again.Lease.Token > original.Lease.Token, "re-acquisition must bump the fencing token");
    }

    [Fact]
    public async Task AcquireDueAsync_HonorsMaxBatch()
    {
        var store = new InMemoryDeferralStore();
        var now = DateTimeOffset.UtcNow;
        for (var i = 0; i < 5; i++)
        {
            await store.EnqueueAsync(Message(now.AddMinutes(-1)));
        }

        var due = await store.AcquireDueAsync(now, Lease, maxBatch: 2);

        Assert.Equal(2, due.Count);
        Assert.Equal(5, store.Count);
    }

    [Fact]
    public async Task CompleteAsync_RemovesEntry_OnlyWithCurrentToken()
    {
        var store = new InMemoryDeferralStore();
        var now = DateTimeOffset.UtcNow;
        await store.EnqueueAsync(Message(now.AddMinutes(-1)));

        var first = Assert.Single(await store.AcquireDueAsync(now, Lease, maxBatch: 10));
        var second = Assert.Single(await store.AcquireDueAsync(now.Add(Lease).AddSeconds(1), Lease, maxBatch: 10));

        // Stale holder (lease expired) cannot remove the entry.
        Assert.False(await store.CompleteAsync(first.Lease));
        Assert.Equal(1, store.Count);

        // The current owner can.
        Assert.True(await store.CompleteAsync(second.Lease));
        Assert.Equal(0, store.Count);
    }

    [Fact]
    public async Task AbandonAsync_Reschedules_OnlyWithCurrentToken()
    {
        var store = new InMemoryDeferralStore();
        var now = DateTimeOffset.UtcNow;
        await store.EnqueueAsync(Message(now.AddMinutes(-1)));

        var leased = Assert.Single(await store.AcquireDueAsync(now, Lease, maxBatch: 10));

        Assert.False(await store.AbandonAsync(leased.Lease with { Token = leased.Lease.Token + 99 }));

        var newVisible = now.AddMinutes(10);
        Assert.True(await store.AbandonAsync(leased.Lease, newVisible));

        // Not visible before the rescheduled time, acquirable afterwards.
        Assert.Empty(await store.AcquireDueAsync(now.AddMinutes(5), Lease, maxBatch: 10));
        Assert.Single(await store.AcquireDueAsync(newVisible, Lease, maxBatch: 10));
    }

    [Fact]
    public async Task EnqueueAsync_PersistsAllFields()
    {
        var store = new InMemoryDeferralStore();
        var headers = new MessageHeaders { MessageId = "msg-1:defer:2", [MessageHeaders.DeferralAttemptKey] = "2" };
        var dueAt = DateTimeOffset.UtcNow.AddSeconds(-1);
        var message = new DeferredMessage(
            Guid.NewGuid(), "orders", typeof(object).AssemblyQualifiedName!, "{\"x\":1}",
            headers, "corr-9", Attempt: 2, dueAt, "part-9");

        await store.EnqueueAsync(message);

        var due = await store.AcquireDueAsync(DateTimeOffset.UtcNow, Lease, maxBatch: 10);
        var acquired = Assert.Single(due);
        Assert.Equal(message.Id, acquired.Message.Id);
        Assert.Equal("orders", acquired.Message.Topic);
        Assert.Equal("corr-9", acquired.Message.CorrelationId);
        Assert.Equal(2, acquired.Message.Attempt);
        Assert.Equal("msg-1:defer:2", acquired.Message.Headers.MessageId);
        Assert.Equal("part-9", acquired.Message.PartitionKey);
    }

    [Fact]
    public async Task EnqueueAsync_PartitionKeyNull_RoundTripsAsNull()
    {
        var store = new InMemoryDeferralStore();
        var message = Message(DateTimeOffset.UtcNow.AddSeconds(-1));

        await store.EnqueueAsync(message);

        var acquired = Assert.Single(await store.AcquireDueAsync(DateTimeOffset.UtcNow, Lease, maxBatch: 10));
        Assert.Null(acquired.Message.PartitionKey);
    }

    [Fact]
    public async Task EnqueueAsync_PartitionKeySet_RoundTripsWithSameKey()
    {
        var store = new InMemoryDeferralStore();
        var message = Message(DateTimeOffset.UtcNow.AddSeconds(-1), partitionKey: "order-42");

        await store.EnqueueAsync(message);

        var acquired = Assert.Single(await store.AcquireDueAsync(DateTimeOffset.UtcNow, Lease, maxBatch: 10));
        Assert.Equal("order-42", acquired.Message.PartitionKey);
    }
}
