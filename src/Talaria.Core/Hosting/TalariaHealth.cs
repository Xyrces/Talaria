// SPDX-License-Identifier: Apache-2.0
namespace Talaria.Core.Hosting;

public sealed record EndpointHealth(string Name, bool IsReady, string? Error);
public enum ListenerState { Created, Starting, Running, Draining, Stopped, Faulted }

/// <summary>Thread-safe listener readiness. Inspect from an application's health endpoint.</summary>
public sealed class TalariaHealth
{
    private readonly object _gate = new();
    private readonly Dictionary<string, EndpointHealth> _endpoints = new();
    private ListenerState _state;
    private TaskCompletionSource _changed = new(TaskCreationOptions.RunContinuationsAsynchronously);
    public ListenerState State { get { lock (_gate) return _state; } }
    public IReadOnlyList<EndpointHealth> Endpoints { get { lock (_gate) return _endpoints.Values.ToArray(); } }
    public bool IsReady { get { lock (_gate) return ReadyUnsafe; } }
    private bool ReadyUnsafe => _state == ListenerState.Running && _endpoints.Values.All(x => x.IsReady);

    public async Task WaitUntilReadyAsync(CancellationToken ct = default)
    {
        while (true)
        {
            Task changed;
            lock (_gate)
            {
                if (ReadyUnsafe) return;
                if (_state is ListenerState.Stopped or ListenerState.Faulted)
                    throw new InvalidOperationException($"Talaria listener is {_state}.");
                changed = _changed.Task;
            }
            await changed.WaitAsync(ct);
        }
    }

    internal void SetState(ListenerState state)
    {
        lock (_gate) { _state = state; Notify(); }
    }
    internal void SetEndpoint(string name, bool ready, string? error = null)
    {
        lock (_gate) { _endpoints[name] = new(name, ready, error); Notify(); }
    }
    private void Notify()
    {
        var previous = _changed;
        _changed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        previous.TrySetResult();
    }
}
