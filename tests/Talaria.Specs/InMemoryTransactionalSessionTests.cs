using Talaria.Transports.InMemory;

namespace Talaria.Specs;

public class InMemoryTransactionalSessionTests
{
    [Fact]
    public async Task Session_Methods_CompleteSuccessfully()
    {
        var session = new InMemoryTransactionalSession();
        
        await session.CommitAsync();
        await session.AbortAsync();
        await session.DisposeAsync();
        
        // Just asserting we didn't throw
        Assert.True(true);
    }
}
