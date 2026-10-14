// SPDX-License-Identifier: Apache-2.0
using System.Collections.Concurrent;
using System.Threading.Channels;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Talaria.Core.Abstractions;
using Talaria.Core.Registration;
using Talaria.Transports.InMemory;

namespace Talaria.Testing;

/// <summary>A real Talaria host using the in-memory provider, with bounded handler assertions.</summary>
public sealed class TalariaTestHost : IAsyncDisposable
{
    private readonly IHost _host;
    private readonly ConcurrentDictionary<Type, ReceivedBuffer> _received = new();
    private bool _started;

    private sealed class ReceivedBuffer
    {
        private readonly object _gate = new();
        private readonly List<object> _items = [];
        private readonly List<Waiter> _waiters = [];

        private sealed class Waiter(Func<object, bool> predicate, TaskCompletionSource<object> completion)
        {
            public Func<object, bool> Predicate { get; } = predicate;
            public TaskCompletionSource<object> Completion { get; } = completion;
        }

        public void Add(object message)
        {
            lock (_gate)
            {
                for (var i = 0; i < _waiters.Count; i++)
                {
                    var waiter = _waiters[i];
                    bool matched;
                    try
                    {
                        matched = waiter.Predicate(message);
                    }
                    catch (Exception ex)
                    {
                        _waiters.RemoveAt(i);
                        waiter.Completion.TrySetException(ex);
                        i--;
                        continue;
                    }

                    if (matched)
                    {
                        _waiters.RemoveAt(i);
                        if (waiter.Completion.TrySetResult(message))
                            return;
                        i--;
                    }
                }
                _items.Add(message);
            }
        }

        public async Task<T> WaitAsync<T>(Func<T, bool> predicate, CancellationToken ct)
        {
            Waiter waiter;
            lock (_gate)
            {
                for (var i = 0; i < _items.Count; i++)
                {
                    var existing = (T)_items[i];
                    if (predicate(existing))
                    {
                        _items.RemoveAt(i);
                        return existing;
                    }
                }
                waiter = new Waiter(
                    obj => predicate((T)obj),
                    new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously));
                _waiters.Add(waiter);
            }

            using var registration = ct.Register(() =>
            {
                lock (_gate)
                {
                    _waiters.Remove(waiter);
                }
                waiter.Completion.TrySetCanceled(ct);
            });

            return (T)await waiter.Completion.Task.ConfigureAwait(false);
        }
    }

    private TalariaTestHost(IHost host) => _host = host;

    /// <summary>Creates a host and configures application services before any endpoints are mapped.</summary>
    public static TalariaTestHost Create(Action<IServiceCollection>? configureServices = null)
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Logging.ClearProviders();
        builder.Services.AddTalaria().UseInMemory();
        configureServices?.Invoke(builder.Services);
        return new TalariaTestHost(builder.Build());
    }

    /// <summary>Maps a command and records each handled message for assertions.</summary>
    public TalariaTestHost MapCommand<T>(Func<T, CancellationToken, Task>? handler = null)
    {
        EnsureNotStarted();
        Func<T, CancellationToken, Task> endpoint = async (T message, CancellationToken ct) =>
        {
            if (handler is not null) await handler(message, ct).ConfigureAwait(false);
            await RecordAsync(message, ct).ConfigureAwait(false);
        };
        _host.Services.MapCommand<T>(endpoint);
        return this;
    }

    /// <summary>Maps an event subscriber and records each handled message for assertions.</summary>
    public TalariaTestHost MapEvent<T>(Func<T, CancellationToken, Task>? handler = null, string? name = null)
    {
        EnsureNotStarted();
        Func<T, CancellationToken, Task> endpoint = async (T message, CancellationToken ct) =>
        {
            if (handler is not null) await handler(message, ct).ConfigureAwait(false);
            await RecordAsync(message, ct).ConfigureAwait(false);
        };
        var builder = _host.Services.MapEvent<T>(endpoint);
        if (!string.IsNullOrWhiteSpace(name)) builder.WithName(name);
        return this;
    }

    /// <summary>Starts the hosted consumer loops.</summary>
    public async Task StartAsync(CancellationToken ct = default)
    {
        EnsureNotStarted();
        _started = true;
        await _host.StartAsync(ct).ConfigureAwait(false);
    }

    public async Task SendAsync<T>(T command, CancellationToken ct = default)
    {
        await using var scope = _host.Services.CreateAsyncScope();
        await scope.ServiceProvider.GetRequiredService<IMessageBus>().SendAsync(command, ct);
    }
    public async Task PublishAsync<T>(T message, CancellationToken ct = default)
    {
        await using var scope = _host.Services.CreateAsyncScope();
        await scope.ServiceProvider.GetRequiredService<IMessageBus>().PublishAsync(message, ct);
    }

    /// <summary>Schedules a command to be sent to its mapped endpoint after the specified delay.</summary>
    public async Task ScheduleSendAsync<T>(T command, TimeSpan delay, CancellationToken ct = default)
    {
        await using var scope = _host.Services.CreateAsyncScope();
        await scope.ServiceProvider.GetRequiredService<IMessageBus>().ScheduleSendAsync(command, delay, ct);
    }

    /// <summary>Schedules a command to be sent to its mapped endpoint at or after the specified UTC due time.</summary>
    public async Task ScheduleSendAsync<T>(T command, DateTimeOffset dueAt, CancellationToken ct = default)
    {
        await using var scope = _host.Services.CreateAsyncScope();
        await scope.ServiceProvider.GetRequiredService<IMessageBus>().ScheduleSendAsync(command, dueAt, ct);
    }

    /// <summary>Schedules an event to be published to all subscribers after the specified delay.</summary>
    public async Task SchedulePublishAsync<T>(T message, TimeSpan delay, CancellationToken ct = default)
    {
        await using var scope = _host.Services.CreateAsyncScope();
        await scope.ServiceProvider.GetRequiredService<IMessageBus>().SchedulePublishAsync(message, delay, ct);
    }

    /// <summary>Schedules an event to be published to all subscribers at or after the specified UTC due time.</summary>
    public async Task SchedulePublishAsync<T>(T message, DateTimeOffset dueAt, CancellationToken ct = default)
    {
        await using var scope = _host.Services.CreateAsyncScope();
        await scope.ServiceProvider.GetRequiredService<IMessageBus>().SchedulePublishAsync(message, dueAt, ct);
    }

    /// <summary>Waits for a handled message of the requested type, failing within the supplied bound.</summary>
    public Task<T> ReceiveAsync<T>(TimeSpan timeout, CancellationToken ct = default)
    {
        if (timeout <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(timeout));
        return ReceiveCoreAsync<T>(_ => true, timeout, $"No handled message of type {typeof(T).Name} arrived within {timeout}.", ct);
    }

    /// <summary>Asserts that a matching handled message arrives before the bound expires.</summary>
    public Task<T> AssertReceivedAsync<T>(Func<T, bool> predicate, TimeSpan timeout, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(predicate);
        if (timeout <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(timeout));
        return ReceiveCoreAsync(predicate, timeout, $"No matching handled message of type {typeof(T).Name} arrived within {timeout}.", ct);
    }

    private async Task<T> ReceiveCoreAsync<T>(Func<T, bool> predicate, TimeSpan timeout, string timeoutMessage, CancellationToken ct)
    {
        var buffer = GetBuffer(typeof(T));
        using var timeoutSource = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeoutSource.CancelAfter(timeout);
        try
        {
            return await buffer.WaitAsync(predicate, timeoutSource.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (timeoutSource.IsCancellationRequested && !ct.IsCancellationRequested)
        {
            throw new TimeoutException(timeoutMessage);
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_started) await _host.StopAsync().ConfigureAwait(false);
        _host.Dispose();
    }

    private ReceivedBuffer GetBuffer(Type type) => _received.GetOrAdd(type, _ => new ReceivedBuffer());
    private ValueTask RecordAsync<T>(T message, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        GetBuffer(typeof(T)).Add(message!);
        return ValueTask.CompletedTask;
    }
    private void EnsureNotStarted()
    {
        if (_started) throw new InvalidOperationException("Map endpoints before starting the test host.");
    }
}

