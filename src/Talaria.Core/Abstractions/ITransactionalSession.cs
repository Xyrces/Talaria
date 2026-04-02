namespace Talaria.Core.Abstractions;

/// <summary>
/// Represents a transactional session for atomic produce + commit operations.
/// </summary>
public interface ITransactionalSession : IAsyncDisposable
{
    /// <summary>
    /// Commits the transaction — all produces and offset commits within this session
    /// become durable atomically.
    /// </summary>
    Task CommitAsync(CancellationToken ct = default);

    /// <summary>
    /// Aborts the transaction — all produces and offset commits within this session
    /// are discarded.
    /// </summary>
    Task AbortAsync(CancellationToken ct = default);
}
