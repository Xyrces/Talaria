// SPDX-License-Identifier: Apache-2.0

using Microsoft.Extensions.DependencyInjection;
using Talaria.Core.Abstractions;

namespace Talaria.Core.Hosting;

/// <summary>Non-generic facade over <see cref="IStateStore{TState}"/> resolved per DI scope.</summary>
internal interface IStateStoreAccessor
{
    Task<SagaSnapshot<object>> ReadSnapshotAsync(IServiceProvider scope, string correlationId, string messageId, CancellationToken ct);
    Task<SagaCommitStatus> TryTransitionAsync(IServiceProvider scope, string correlationId, string messageId, long expectedVersion,
        object? state, IReadOnlyList<OutboxMessage> outbox, CancellationToken ct);
    Task<object?> GetAsync(IServiceProvider scope, string correlationId, CancellationToken ct);
    Task SaveAsync(IServiceProvider scope, string correlationId, object state, CancellationToken ct);
    Task DeleteAsync(IServiceProvider scope, string correlationId, CancellationToken ct);
    Task TransitionAsync(IServiceProvider scope, string correlationId, object? newState, IReadOnlyList<OutboxMessage> outbox, CancellationToken ct);
    object NewState();
}

internal sealed class StateStoreAccessor<TState> : IStateStoreAccessor where TState : class, new()
{
    public async Task<SagaSnapshot<object>> ReadSnapshotAsync(IServiceProvider scope, string id, string messageId, CancellationToken ct)
    {
        var snapshot = await (scope.GetService<IStateStore<TState>>() ?? scope.GetRequiredService<IStateStoreFactory>().GetStore<TState>()).ReadSnapshotAsync(id, messageId, ct);
        return new(snapshot.State, snapshot.Version, snapshot.IsCompleted, snapshot.MessageProcessed);
    }
    public Task<SagaCommitStatus> TryTransitionAsync(IServiceProvider scope, string id, string messageId, long expectedVersion,
        object? state, IReadOnlyList<OutboxMessage> outbox, CancellationToken ct)
        => (scope.GetService<IStateStore<TState>>() ?? scope.GetRequiredService<IStateStoreFactory>().GetStore<TState>()).TryTransitionAsync(id, messageId, expectedVersion, (TState?)state, outbox, ct);
    public async Task<object?> GetAsync(IServiceProvider scope, string correlationId, CancellationToken ct)
        => await (scope.GetService<IStateStore<TState>>() ?? scope.GetRequiredService<IStateStoreFactory>().GetStore<TState>()).GetAsync(correlationId, ct);

    public async Task SaveAsync(IServiceProvider scope, string correlationId, object state, CancellationToken ct)
        => await (scope.GetService<IStateStore<TState>>() ?? scope.GetRequiredService<IStateStoreFactory>().GetStore<TState>()).SaveAsync(correlationId, (TState)state, ct);

    public async Task DeleteAsync(IServiceProvider scope, string correlationId, CancellationToken ct)
        => await (scope.GetService<IStateStore<TState>>() ?? scope.GetRequiredService<IStateStoreFactory>().GetStore<TState>()).DeleteAsync(correlationId, ct);

    public async Task TransitionAsync(IServiceProvider scope, string correlationId, object? newState, IReadOnlyList<OutboxMessage> outbox, CancellationToken ct)
        => await (scope.GetService<IStateStore<TState>>() ?? scope.GetRequiredService<IStateStoreFactory>().GetStore<TState>()).TransitionAsync(correlationId, (TState?)newState, outbox, ct);

    public object NewState() => new TState();
}
