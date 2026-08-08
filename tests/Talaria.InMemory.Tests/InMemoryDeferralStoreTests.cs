using Talaria.Core.Abstractions;
using Talaria.Transports.InMemory;
using Xunit;

namespace Talaria.InMemory.Tests;

public class InMemoryDeferralStoreTests
{
    private static DeferredMessage Message(DateTimeOffset dueAt, string topic = "orders") =>
        new(Guid.NewGuid(), topic, typeof(object).AssemblyQualifiedName!, "{}",
            new MessageHeaders(), "corr-1", Attempt: 1, dueAt);

    [Fact]
    public async Task PopDueAsync_ReturnsOnlyDueMessages_InDueOrder()
    {
        var store = new InMemoryDeferralStore();
        var now = DateTimeOffset.UtcNow;
        var future = Message(now.AddMinutes(5));
        var dueLater = Message(now.AddMilliseconds(-1));
        var dueFirst = Message(now.AddMinutes(-2));

        await store.EnqueueAsync(future);
        await store.EnqueueAsync(dueLater);
        await store.EnqueueAsync(dueFirst);

        var due = await store.PopDueAsync(now, maxBatch: 10);

        Assert.Equal(2, due.Count);
        Assert.Equal(dueFirst.Id, due[0].Id);
        Assert.Equal(dueLater.Id, due[1].Id);
        Assert.Equal(1, store.Count);
    }

    [Fact]
    public async Task PopDueAsync_ClaimsAtomically_SecondPopReturnsEmpty()
    {
        var store = new InMemoryDeferralStore();
        var now = DateTimeOffset.UtcNow;
        await store.EnqueueAsync(Message(now.AddMinutes(-1)));

        var first = await store.PopDueAsync(now, maxBatch: 10);
        var second = await store.PopDueAsync(now, maxBatch: 10);

        Assert.Single(first);
        Assert.Empty(second);
        Assert.Equal(0, store.Count);
    }

    [Fact]
    public async Task PopDueAsync_HonorsMaxBatch()
    {
        var store = new InMemoryDeferralStore();
        var now = DateTimeOffset.UtcNow;
        for (var i = 0; i < 5; i++)
        {
            await store.EnqueueAsync(Message(now.AddMinutes(-1)));
        }

        var due = await store.PopDueAsync(now, maxBatch: 2);

        Assert.Equal(2, due.Count);
        Assert.Equal(3, store.Count);
    }

    [Fact]
    public async Task RequeueAsync_ReschedulesClaimedMessage()
    {
        var store = new InMemoryDeferralStore();
        var now = DateTimeOffset.UtcNow;
        var message = Message(now.AddMinutes(-1));
        await store.EnqueueAsync(message);

        var popped = await store.PopDueAsync(now, maxBatch: 10);
        Assert.Single(popped);

        var newDue = now.AddMinutes(10);
        await store.RequeueAsync(popped[0], newDue);

        Assert.Empty(await store.PopDueAsync(now, maxBatch: 10));

        var rescheduled = await store.PopDueAsync(newDue, maxBatch: 10);
        Assert.Single(rescheduled);
        Assert.Equal(newDue, rescheduled[0].DueAt);
    }

    [Fact]
    public async Task EnqueueAsync_PersistsAllFields()
    {
        var store = new InMemoryDeferralStore();
        var headers = new MessageHeaders { MessageId = "msg-1:defer:2", [MessageHeaders.DeferralAttemptKey] = "2" };
        var dueAt = DateTimeOffset.UtcNow.AddSeconds(-1);
        var message = new DeferredMessage(
            Guid.NewGuid(), "orders", typeof(object).AssemblyQualifiedName!, "{\"x\":1}",
            headers, "corr-9", Attempt: 2, dueAt);

        await store.EnqueueAsync(message);

        var due = await store.PopDueAsync(DateTimeOffset.UtcNow, maxBatch: 10);
        var popped = Assert.Single(due);
        Assert.Equal(message.Id, popped.Id);
        Assert.Equal("orders", popped.Topic);
        Assert.Equal("corr-9", popped.CorrelationId);
        Assert.Equal(2, popped.Attempt);
        Assert.Equal("msg-1:defer:2", popped.Headers.MessageId);
    }
}
