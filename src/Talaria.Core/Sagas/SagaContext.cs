// SPDX-License-Identifier: Apache-2.0

using System.Threading;

namespace Talaria.Core.Sagas;

internal sealed class SagaContext<TState> : ISagaContext<TState>
{
    private readonly List<object> _outboundMessages = new();

    /// <inheritdoc />
    public CancellationToken CancellationToken { get; internal set; }

    public ISagaContext<TState> Dispatch<TMessage>(TMessage message) where TMessage : class
    {
        _outboundMessages.Add(message);
        return this;
    }

    public SagaResult<TState> Complete()
    {
        // Snapshot: later Dispatch calls must not mutate an already-returned result.
        return SagaResult<TState>.Complete(_outboundMessages.ToArray());
    }

    public SagaResult<TState> Transition(TState newState)
    {
        return SagaResult<TState>.Transition(newState, _outboundMessages.ToArray());
    }

    public SagaResult<TState> Defer()
    {
        return SagaResult<TState>.Defer();
    }
}
