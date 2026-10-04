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
    private readonly ConcurrentDictionary<Type, Channel<object>> _received = new();
    private bool _started;

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
    public TalariaTestHost MapEvent<T>(Func<T, CancellationToken, Task>? handler = null)
    {
        EnsureNotStarted();
        Func<T, CancellationToken, Task> endpoint = async (T message, CancellationToken ct) =>
        {
            if (handler is not null) await handler(message, ct).ConfigureAwait(false);
            await RecordAsync(message, ct).ConfigureAwait(false);
        };
        _host.Services.MapEvent<T>(endpoint);
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

    /// <summary>Waits for a handled message of the requested type, failing within the supplied bound.</summary>
    public async Task<T> ReceiveAsync<T>(TimeSpan timeout, CancellationToken ct = default)
    {
        if (timeout <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(timeout));
        var channel = GetChannel(typeof(T));
        using var timeoutSource = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeoutSource.CancelAfter(timeout);
        try
        {
            return (T)await channel.Reader.ReadAsync(timeoutSource.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            throw new TimeoutException($"No handled message of type {typeof(T).Name} arrived within {timeout}.");
        }
    }

    /// <summary>Asserts that a matching handled message arrives before the bound expires.</summary>
    public async Task<T> AssertReceivedAsync<T>(Func<T, bool> predicate, TimeSpan timeout, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(predicate);
        if (timeout <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(timeout));
        using var timeoutSource = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeoutSource.CancelAfter(timeout);
        var channel = GetChannel(typeof(T));
        try
        {
            while (true)
            {
                var item = (T)await channel.Reader.ReadAsync(timeoutSource.Token).ConfigureAwait(false);
                if (predicate(item)) return item;
            }
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            throw new TimeoutException($"No matching handled message of type {typeof(T).Name} arrived within {timeout}.");
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_started) await _host.StopAsync().ConfigureAwait(false);
        _host.Dispose();
    }

    private Channel<object> GetChannel(Type type) => _received.GetOrAdd(type, _ => Channel.CreateUnbounded<object>());
    private ValueTask RecordAsync<T>(T message, CancellationToken ct) => GetChannel(typeof(T)).Writer.WriteAsync(message!, ct);
    private void EnsureNotStarted()
    {
        if (_started) throw new InvalidOperationException("Map endpoints before starting the test host.");
    }
}
