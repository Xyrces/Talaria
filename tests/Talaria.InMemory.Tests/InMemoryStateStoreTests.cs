using Talaria.Transports.InMemory;

namespace Talaria.InMemory.Tests;

public class InMemoryStateStoreTests
{
    private class TestState
    {
        public string? Value { get; set; }
    }

    [Fact]
    public async Task SaveAsync_And_GetAsync_ShouldPersistState()
    {
        var store = new InMemoryStateStore<TestState>();
        var id = Guid.NewGuid().ToString();
        var state = new TestState { Value = "Test" };

        await store.SaveAsync(id, state);
        var retrieved = await store.GetAsync(id);

        Assert.NotNull(retrieved);
        Assert.Equal("Test", retrieved!.Value);
        Assert.Equal(1, store.Count);
    }

    [Fact]
    public async Task GetAsync_WhenNotExists_ShouldReturnNull()
    {
        var store = new InMemoryStateStore<TestState>();
        
        var retrieved = await store.GetAsync("missing");

        Assert.Null(retrieved);
    }

    [Fact]
    public async Task DeleteAsync_ShouldRemoveState()
    {
        var store = new InMemoryStateStore<TestState>();
        var id = Guid.NewGuid().ToString();
        
        await store.SaveAsync(id, new TestState());
        Assert.Equal(1, store.Count);

        await store.DeleteAsync(id);
        
        Assert.Equal(0, store.Count);
        var retrieved = await store.GetAsync(id);
        Assert.Null(retrieved);
    }

    [Fact]
    public async Task Clear_ShouldRemoveAllStates()
    {
        var store = new InMemoryStateStore<TestState>();
        
        await store.SaveAsync("id1", new TestState());
        await store.SaveAsync("id2", new TestState());
        Assert.Equal(2, store.Count);

        store.Clear();
        
        Assert.Equal(0, store.Count);
    }

    [Fact]
    public async Task TransitionAsync_StagesOutboxAtomically_WithStateSave()
    {
        var outbox = new InMemoryOutboxStore();
        var store = new InMemoryStateStore<TestState>(outbox);
        var entry = new Core.Abstractions.OutboxMessage(
            Guid.NewGuid(), "out-topic", typeof(object).AssemblyQualifiedName!, "{}",
            new Core.Abstractions.MessageHeaders { MessageId = "minted-1" }, DateTimeOffset.UtcNow);

        await store.TransitionAsync("c1", new TestState { Value = "v1" }, [entry]);

        // Both halves of the atomic unit are visible.
        Assert.Equal("v1", (await store.GetAsync("c1"))!.Value);
        Assert.Equal(1, outbox.Count);

        var leased = Assert.Single(await outbox.AcquirePendingAsync(
            DateTimeOffset.UtcNow, TimeSpan.FromSeconds(30), maxBatch: 10));
        Assert.Equal(entry.Id, leased.Message.Id);
        Assert.Equal("out-topic", leased.Message.Topic);
        Assert.Equal("minted-1", leased.Message.Headers.MessageId);
    }

    [Fact]
    public async Task TransitionAsync_NullState_Purges_WhileStaging()
    {
        var outbox = new InMemoryOutboxStore();
        var store = new InMemoryStateStore<TestState>(outbox);
        await store.SaveAsync("c1", new TestState { Value = "v1" });

        await store.TransitionAsync("c1", null, [new Core.Abstractions.OutboxMessage(
            Guid.NewGuid(), "out-topic", typeof(object).AssemblyQualifiedName!, "{}",
            new Core.Abstractions.MessageHeaders(), DateTimeOffset.UtcNow)]);

        Assert.Null(await store.GetAsync("c1"));
        Assert.Equal(1, outbox.Count);
    }

    [Fact]
    public async Task TransitionAsync_WithoutOutbox_Throws_WhenMessagesStaged()
    {
        var store = new InMemoryStateStore<TestState>();
        var entry = new Core.Abstractions.OutboxMessage(
            Guid.NewGuid(), "out-topic", typeof(object).AssemblyQualifiedName!, "{}",
            new Core.Abstractions.MessageHeaders(), DateTimeOffset.UtcNow);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => store.TransitionAsync("c1", new TestState(), [entry]));

        // Nothing was applied — the atomic unit failed as a whole.
        Assert.Null(await store.GetAsync("c1"));
    }

    [Fact]
    public async Task TransitionAsync_WithoutOutbox_AppliesState_WhenNoMessagesStaged()
    {
        var store = new InMemoryStateStore<TestState>();

        await store.TransitionAsync("c1", new TestState { Value = "v1" }, []);

        Assert.Equal("v1", (await store.GetAsync("c1"))!.Value);
    }
}
