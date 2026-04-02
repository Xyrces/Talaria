using Talaria.Core.Abstractions;

namespace Talaria.Transports.InMemory;

/// <summary>
/// No-op transactional session for in-memory transport.
/// Always commits successfully — atomicity is inherent in single-process channels.
/// </summary>
internal sealed class InMemoryTransactionalSession : ITransactionalSession
{
    public Task CommitAsync(CancellationToken ct = default) => Task.CompletedTask;
    public Task AbortAsync(CancellationToken ct = default) => Task.CompletedTask;
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
