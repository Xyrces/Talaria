using Talaria.Transports.InMemory;

namespace Talaria.InMemory.Tests;

public class InMemoryTransactionalSessionTests
{
    private sealed record TestMessage(string Id);

    [Fact]
    public async Task Commit_MakesBufferedProducesVisible()
    {
        var transport = new InMemoryTransport();
        await using var session = await transport.BeginTransactionAsync();

        var producer = await session.GetProducerAsync<TestMessage>("txn-topic");
        await producer.ProduceAsync(new TestMessage("m1"));
        await producer.ProduceAsync(new TestMessage("m2"));

        // Nothing is visible before commit.
        Assert.Empty(await transport.ReadAllFromTopicAsync<TestMessage>("txn-topic"));

        await session.CommitAsync();

        var messages = await transport.ReadAllFromTopicAsync<TestMessage>("txn-topic");
        Assert.Equal(2, messages.Count);
    }

    [Fact]
    public async Task Abort_DiscardsBufferedProduces()
    {
        var transport = new InMemoryTransport();
        var session = await transport.BeginTransactionAsync();

        var producer = await session.GetProducerAsync<TestMessage>("txn-topic");
        await producer.ProduceAsync(new TestMessage("m1"));

        await session.AbortAsync();
        await session.DisposeAsync();

        Assert.Empty(await transport.ReadAllFromTopicAsync<TestMessage>("txn-topic"));
    }

    [Fact]
    public async Task Dispose_OpenSession_DiscardsBufferedProduces()
    {
        var transport = new InMemoryTransport();
        var session = await transport.BeginTransactionAsync();

        var producer = await session.GetProducerAsync<TestMessage>("txn-topic");
        await producer.ProduceAsync(new TestMessage("m1"));

        // Dispose without commit — implicit abort.
        await session.DisposeAsync();

        Assert.Empty(await transport.ReadAllFromTopicAsync<TestMessage>("txn-topic"));
    }

    [Fact]
    public async Task Produce_AfterCommit_Throws()
    {
        var transport = new InMemoryTransport();
        await using var session = await transport.BeginTransactionAsync();

        var producer = await session.GetProducerAsync<TestMessage>("txn-topic");
        await session.CommitAsync();

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => producer.ProduceAsync(new TestMessage("late")));
    }

    [Fact]
    public async Task Commit_Twice_Throws()
    {
        var transport = new InMemoryTransport();
        await using var session = await transport.BeginTransactionAsync();

        await session.CommitAsync();

        await Assert.ThrowsAsync<InvalidOperationException>(() => session.CommitAsync());
    }
}
