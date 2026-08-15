// SPDX-License-Identifier: Apache-2.0

using Microsoft.Extensions.DependencyInjection;
using Talaria.Core.Abstractions;

namespace Talaria.Core.Hosting;

/// <summary>Non-generic facade over <see cref="IStateStore{TState}"/> resolved per DI scope.</summary>
internal interface IStateStoreAccessor
{
    Task<object?> GetAsync(IServiceProvider scope, string correlationId, CancellationToken ct);
    Task SaveAsync(IServiceProvider scope, string correlationId, object state, CancellationToken ct);
    Task DeleteAsync(IServiceProvider scope, string correlationId, CancellationToken ct);
    Task TransitionAsync(IServiceProvider scope, string correlationId, object? newState, IReadOnlyList<OutboxMessage> outbox, CancellationToken ct);
    object NewState();
}

internal sealed class StateStoreAccessor<TState> : IStateStoreAccessor where TState : class, new()
{
    public async Task<object?> GetAsync(IServiceProvider scope, string correlationId, CancellationToken ct)
        => await scope.GetRequiredService<IStateStore<TState>>().GetAsync(correlationId, ct);

    public async Task SaveAsync(IServiceProvider scope, string correlationId, object state, CancellationToken ct)
        => await scope.GetRequiredService<IStateStore<TState>>().SaveAsync(correlationId, (TState)state, ct);

    public async Task DeleteAsync(IServiceProvider scope, string correlationId, CancellationToken ct)
        => await scope.GetRequiredService<IStateStore<TState>>().DeleteAsync(correlationId, ct);

    public async Task TransitionAsync(IServiceProvider scope, string correlationId, object? newState, IReadOnlyList<OutboxMessage> outbox, CancellationToken ct)
        => await scope.GetRequiredService<IStateStore<TState>>().TransitionAsync(correlationId, (TState?)newState, outbox, ct);

    public object NewState() => new TState();
}
