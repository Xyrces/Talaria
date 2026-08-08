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
        
        Assert.NotNull(acquired);
        Assert.Equal(1, store.Count);
    }

    [Fact]
    public async Task TryAcquireLockAsync_FailsToAcquire_WhenLockActive()
    {
        var store = new InMemoryIdempotencyStore();
        var first = await store.TryAcquireLockAsync("msg-1", "queue-a", TimeSpan.FromMinutes(1));
        var second = await store.TryAcquireLockAsync("msg-1", "queue-a", TimeSpan.FromMinutes(1));

        Assert.NotNull(first);
        Assert.Null(second);
    }

    [Fact]
    public async Task TryAcquireLockAsync_ReacquiresLock_WhenExpired()
    {
        var store = new InMemoryIdempotencyStore();
        var first = await store.TryAcquireLockAsync("msg-1", "queue-a", TimeSpan.FromMilliseconds(10));
        Assert.NotNull(first);

        await Task.Delay(30);

        var second = await store.TryAcquireLockAsync("msg-1", "queue-a", TimeSpan.FromMinutes(1));
        Assert.NotNull(second);
    }

    [Fact]
    public async Task ReleaseLockAsync_RemovesLock_AllowingReacquire()
    {
        var store = new InMemoryIdempotencyStore();
        var acquired = await store.TryAcquireLockAsync("msg-1", "queue-a", TimeSpan.FromMinutes(1));
        Assert.NotNull(acquired);
        await store.ReleaseLockAsync(acquired);

        var acquiredAfterRelease = await store.TryAcquireLockAsync("msg-1", "queue-a", TimeSpan.FromMinutes(1));
        Assert.NotNull(acquiredAfterRelease);
    }

    [Fact]
    public async Task ReleaseLockAsync_WithStaleToken_DoesNotRemoveNewOwnersLock()
    {
        var store = new InMemoryIdempotencyStore();
        var first = await store.TryAcquireLockAsync("msg-1", "queue-a", TimeSpan.FromMilliseconds(10));
        Assert.NotNull(first);

        // First owner's lock expires; a second owner acquires the same key.
        await Task.Delay(30);
        var second = await store.TryAcquireLockAsync("msg-1", "queue-a", TimeSpan.FromMinutes(1));
        Assert.NotNull(second);

        // The stale first owner releases — it must NOT delete the second owner's lock.
        await store.ReleaseLockAsync(first);

        var third = await store.TryAcquireLockAsync("msg-1", "queue-a", TimeSpan.FromMinutes(1));
        Assert.Null(third);
    }

    [Fact]
    public async Task MarkCompleteAsync_PersistsCompletedLock_PreventingReacquire()
    {
        var store = new InMemoryIdempotencyStore();
        var acquired = await store.TryAcquireLockAsync("msg-1", "queue-a", TimeSpan.FromMinutes(1));
        Assert.NotNull(acquired);
        await store.MarkCompleteAsync(acquired);

        var reacquire = await store.TryAcquireLockAsync("msg-1", "queue-a", TimeSpan.FromMinutes(1));
        Assert.Null(reacquire);
    }
}
