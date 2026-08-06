using Talaria.Transports.InMemory;
using Xunit;

namespace Talaria.InMemory.Tests;

public class InMemoryIdempotencyStoreTests
{
    [Fact]
    public async Task TryAcquireLockAsync_AcquiresLockSuccessfully_WhenNotPresent()
    {
        var store = new InMemoryIdempotencyStore();
        var acquired = await store.TryAcquireLockAsync("msg-1", "queue-a", TimeSpan.FromMinutes(1));
        
        Assert.True(acquired);
        Assert.Equal(1, store.Count);
    }

    [Fact]
    public async Task TryAcquireLockAsync_FailsToAcquire_WhenLockActive()
    {
        var store = new InMemoryIdempotencyStore();
        var first = await store.TryAcquireLockAsync("msg-1", "queue-a", TimeSpan.FromMinutes(1));
        var second = await store.TryAcquireLockAsync("msg-1", "queue-a", TimeSpan.FromMinutes(1));

        Assert.True(first);
        Assert.False(second);
    }

    [Fact]
    public async Task TryAcquireLockAsync_ReacquiresLock_WhenExpired()
    {
        var store = new InMemoryIdempotencyStore();
        var first = await store.TryAcquireLockAsync("msg-1", "queue-a", TimeSpan.FromMilliseconds(10));
        Assert.True(first);

        await Task.Delay(30);

        var second = await store.TryAcquireLockAsync("msg-1", "queue-a", TimeSpan.FromMinutes(1));
        Assert.True(second);
    }

    [Fact]
    public async Task ReleaseLockAsync_RemovesLock_AllowingReacquire()
    {
        var store = new InMemoryIdempotencyStore();
        await store.TryAcquireLockAsync("msg-1", "queue-a", TimeSpan.FromMinutes(1));
        await store.ReleaseLockAsync("msg-1", "queue-a");

        var acquiredAfterRelease = await store.TryAcquireLockAsync("msg-1", "queue-a", TimeSpan.FromMinutes(1));
        Assert.True(acquiredAfterRelease);
    }

    [Fact]
    public async Task MarkCompleteAsync_PersistsCompletedLock_PreventingReacquire()
    {
        var store = new InMemoryIdempotencyStore();
        await store.TryAcquireLockAsync("msg-1", "queue-a", TimeSpan.FromMinutes(1));
        await store.MarkCompleteAsync("msg-1", "queue-a");

        var reacquire = await store.TryAcquireLockAsync("msg-1", "queue-a", TimeSpan.FromMinutes(1));
        Assert.False(reacquire);
    }
}
