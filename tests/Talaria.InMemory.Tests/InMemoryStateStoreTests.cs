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
}
