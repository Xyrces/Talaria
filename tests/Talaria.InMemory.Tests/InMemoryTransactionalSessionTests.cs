using Talaria.Transports.InMemory;

namespace Talaria.InMemory.Tests;

public class InMemoryTransactionalSessionTests
{
    [Fact]
    public async Task TransactionalSession_Methods_ShouldNotThrow()
    {
        // Act & Assert
        var session = new InMemoryTransactionalSession();
        
        await session.CommitAsync();
        await session.AbortAsync();
        await session.DisposeAsync();

        // If it doesn't throw, it succeeds since it's a no-op implementation.
        Assert.True(true);
    }
}
