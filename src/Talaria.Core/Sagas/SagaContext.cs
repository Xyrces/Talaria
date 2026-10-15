// SPDX-License-Identifier: Apache-2.0

using System.Linq;
using System.Threading;

namespace Talaria.Core.Sagas;

internal sealed class SagaContext<TState> : ISagaContext<TState>
{
    private readonly TimeProvider _time;
    private readonly List<SagaScheduledDispatch> _dispatches = new();
    private readonly List<Type> _canceledTimeouts = new();
    private readonly List<string> _recordedSteps = new();

    public SagaContext(TimeProvider? timeProvider = null)
    {
        _time = timeProvider ?? TimeProvider.System;
    }

    /// <inheritdoc />
    public CancellationToken CancellationToken { get; internal set; }

    public ISagaContext<TState> Dispatch<TMessage>(TMessage message) where TMessage : class
    {
        ArgumentNullException.ThrowIfNull(message);
        _dispatches.Add(new SagaScheduledDispatch(message));
        return this;
    }

    public ISagaContext<TState> Dispatch<TMessage>(TMessage message, TimeSpan delay) where TMessage : class
    {
        ArgumentNullException.ThrowIfNull(message);
        if (delay < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(delay), delay, "Delay must be non-negative.");
        }

        return Dispatch(message, _time.GetUtcNow().Add(delay));
    }

    public ISagaContext<TState> Dispatch<TMessage>(TMessage message, DateTimeOffset dueAt) where TMessage : class
    {
        ArgumentNullException.ThrowIfNull(message);
        _dispatches.Add(new SagaScheduledDispatch(message, dueAt, IsTimeout: false));
        return this;
    }

    public ISagaContext<TState> RequestTimeout<TTimeout>(TTimeout timeout, TimeSpan delay) where TTimeout : class
    {
        ArgumentNullException.ThrowIfNull(timeout);
        if (delay < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(delay), delay, "Delay must be non-negative.");
        }

        return RequestTimeout(timeout, _time.GetUtcNow().Add(delay));
    }

    public ISagaContext<TState> RequestTimeout<TTimeout>(TTimeout timeout, DateTimeOffset dueAt) where TTimeout : class
    {
        ArgumentNullException.ThrowIfNull(timeout);
        var timeoutType = typeof(TTimeout);
        _dispatches.RemoveAll(d => d.IsTimeout && d.Message.GetType() == timeoutType);
        _canceledTimeouts.RemoveAll(t => t == timeoutType);
        _dispatches.Add(new SagaScheduledDispatch(timeout, dueAt, IsTimeout: true));
        _canceledTimeouts.Add(timeoutType);
        return this;
    }

    public ISagaContext<TState> CancelTimeout<TTimeout>() where TTimeout : class
    {
        var timeoutType = typeof(TTimeout);
        _dispatches.RemoveAll(d => d.IsTimeout && d.Message.GetType() == timeoutType);
        if (!_canceledTimeouts.Contains(timeoutType))
        {
            _canceledTimeouts.Add(timeoutType);
        }

        return this;
    }

    public ISagaContext<TState> RecordStep(string stepName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(stepName);
        _recordedSteps.Add(stepName.Trim());
        return this;
    }

    public SagaResult<TState> Complete()
    {
        // Snapshot: later Dispatch calls must not mutate an already-returned result.
        var dispatches = _dispatches.ToArray();
        var outbound = dispatches.Select(static d => d.Message).ToArray();
        var canceled = _canceledTimeouts.ToArray();
        return SagaResult<TState>.Complete(outbound, dispatches, canceled);
    }

    public SagaResult<TState> Transition(TState newState)
    {
        ArgumentNullException.ThrowIfNull(newState);
        var dispatches = _dispatches.ToArray();
        var outbound = dispatches.Select(static d => d.Message).ToArray();
        var canceled = _canceledTimeouts.ToArray();
        var recorded = _recordedSteps.ToArray();
        AppendRecordedSteps(newState, recorded);
        return SagaResult<TState>.Transition(newState, outbound, dispatches, canceled, recorded);
    }

    public SagaResult<TState> Compensate(TState newState)
    {
        ArgumentNullException.ThrowIfNull(newState);
        var dispatches = _dispatches.ToArray();
        var outbound = dispatches.Select(static d => d.Message).ToArray();
        var canceled = _canceledTimeouts.ToArray();
        var recorded = _recordedSteps.ToArray();
        AppendRecordedSteps(newState, recorded);
        return SagaResult<TState>.Compensate(newState, outbound, dispatches, canceled, recorded);
    }

    public SagaResult<TState> CompensateAndComplete(TState? stateSnapshot = default)
    {
        var dispatches = _dispatches.ToArray();
        var outbound = dispatches.Select(static d => d.Message).ToArray();
        var canceled = _canceledTimeouts.ToArray();
        var recorded = _recordedSteps.ToArray();
        AppendRecordedSteps(stateSnapshot, recorded);
        return SagaResult<TState>.CompensateAndComplete(stateSnapshot, outbound, dispatches, canceled, recorded);
    }

    public SagaResult<TState> Defer()
    {
        _dispatches.Clear();
        _canceledTimeouts.Clear();
        _recordedSteps.Clear();
        return SagaResult<TState>.Defer();
    }

    private static void AppendRecordedSteps(TState? state, IReadOnlyList<string> recorded)
    {
        if (recorded.Count > 0 && state is ICompensatableState compState)
        {
            compState.Compensation ??= new SagaCompensationState();
            foreach (var step in recorded)
            {
                compState.Compensation.RecordedSteps.Add(step);
            }
        }
    }
}
