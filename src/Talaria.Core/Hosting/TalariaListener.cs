// SPDX-License-Identifier: Apache-2.0

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Talaria.Core.Abstractions;
using Talaria.Core.Registration;
using Talaria.Core.Sagas;

namespace Talaria.Core.Hosting;

/// <summary>
/// Host-agnostic listener that orchestrates all Talaria consumption: topic handlers,
/// saga steps, deferral sweeper, and transactional outbox relay. It can be started
/// and stopped explicitly via <see cref="StartAsync(CancellationToken)"/> and
/// <see cref="StopAsync(CancellationToken)"/> without a Generic Host.
/// </summary>
/// <remarks>
/// The listener is single-cycle: <see cref="StartAsync(CancellationToken)"/> after
/// <see cref="StopAsync(CancellationToken)"/> throws <see cref="InvalidOperationException"/>.
/// Double-start and double-stop are idempotent no-ops. Disposing the listener stops
/// it if it is running but does not dispose caller-owned transports or stores.
/// </remarks>
public sealed class TalariaListener : IAsyncDisposable
{
    private readonly ITransport _transport;
    private readonly TopicRegistry _topicRegistry;
    private readonly SagaRegistry _sagaRegistry;
    private readonly TalariaOptions _options;
    private readonly ILogger<TalariaListener> _logger;
    private readonly IServiceProvider? _serviceProvider;
    private readonly TalariaListenerStores _stores;

    private readonly object _lifecycleLock = new();
    private CancellationTokenSource? _runCts;
    private Task? _runTask;
    private bool _stopped;
    private bool _disposed;

    /// <summary>
    /// Creates a new listener.
    /// </summary>
    /// <param name="transport">The transport that provides consumers and producers.</param>
    /// <param name="topicRegistry">Registry of topic handlers.</param>
    /// <param name="sagaRegistry">Registry of saga configurations.</param>
    /// <param name="options">Global Talaria options.</param>
    /// <param name="logger">Logger for listener diagnostics.</param>
    /// <param name="serviceProvider">
    /// Required when sagas are registered. Used to resolve state stores and create
    /// handler scopes. When supplied and <paramref name="stores"/> is omitted, the
    /// optional stores are resolved from this provider at construction time.
    /// </param>
    /// <param name="stores">
    /// Optional stores supplied directly. When omitted and a <paramref name="serviceProvider"/>
    /// is supplied, the stores are resolved from the provider.
    /// </param>
    public TalariaListener(
        ITransport transport,
        TopicRegistry topicRegistry,
        SagaRegistry sagaRegistry,
        TalariaOptions options,
        ILogger<TalariaListener> logger,
        IServiceProvider? serviceProvider = null,
        TalariaListenerStores? stores = null)
    {
        _transport = transport;
        _topicRegistry = topicRegistry;
        _sagaRegistry = sagaRegistry;
        _options = options;
        _logger = logger;
        _serviceProvider = serviceProvider;
        _stores = stores ?? new TalariaListenerStores(
            serviceProvider?.GetService<IIdempotencyStore>(),
            serviceProvider?.GetService<IDeferralStore>(),
            serviceProvider?.GetService<IOutboxStore>());
    }

    /// <summary>
    /// True while the listener is running (between a successful start and a completed stop).
    /// </summary>
    public bool IsRunning
    {
        get
        {
            lock (_lifecycleLock)
            {
                return _runTask is { IsCompleted: false } && !_stopped;
            }
        }
    }

    /// <summary>
    /// Seals the registries, snapshots the consumer plan, and starts all supervised loops.
    /// Idempotent while running. Throws <see cref="InvalidOperationException"/> if called
    /// after <see cref="StopAsync(CancellationToken)"/>.
    /// </summary>
    /// <remarks>
    /// The returned task completes once the start operation has completed; the listener
    /// continues running in the background until <see cref="StopAsync(CancellationToken)"/>
    /// is called.
    /// </remarks>
    public Task StartAsync(CancellationToken cancellationToken = default)
    {
        lock (_lifecycleLock)
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(nameof(TalariaListener));
            }

            if (_stopped)
            {
                throw new InvalidOperationException(
                    "TalariaListener has already been stopped. It is single-cycle; create a new listener to start again.");
            }

            if (_runTask is not null)
            {
                return Task.CompletedTask;
            }

            if (_sagaRegistry.Registrations.Count > 0 && _serviceProvider is null)
            {
                throw new InvalidOperationException(
                    "Sagas are registered but no IServiceProvider was supplied to TalariaListener. " +
                    "A service provider is required to resolve IStateStore<TState> instances and create handler scopes.");
            }

            _runCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            _runTask = RunAsync(_runCts.Token);
            return Task.CompletedTask;
        }
    }

    /// <summary>
    /// Cancels all loops, awaits their exit, and disposes listener-created consumers
    /// and producers. Idempotent after stop.
    /// </summary>
    public async Task StopAsync(CancellationToken cancellationToken = default)
    {
        Task? runTask;
        CancellationTokenSource? runCts;

        lock (_lifecycleLock)
        {
            if (_disposed || _stopped || _runTask is null)
            {
                return;
            }

            runTask = _runTask;
            runCts = _runCts;
            _stopped = true;
        }

        runCts?.Cancel();

        if (runTask is not null)
        {
            try
            {
                await runTask.WaitAsync(cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception)
            {
                // Loops are supervised; absorb any fault so stop is idempotent.
            }
        }

        runCts?.Dispose();
    }

    /// <summary>
    /// Stops the listener if it is running. Does not dispose caller-owned transports or stores.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        lock (_lifecycleLock)
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
        }

        await StopAsync();
        GC.SuppressFinalize(this);
    }

    private async Task RunAsync(CancellationToken ct)
    {
        _topicRegistry.Seal();
        _sagaRegistry.Seal();

        var topicRegistrations = _topicRegistry.Registrations;
        var sagaRegistrations = _sagaRegistry.Registrations;

        var pipeline = new MessageProcessingPipeline(_stores.IdempotencyStore, _options, _logger);

        TopicConsumerEngine? topicEngine = null;
        SagaConsumerEngine? sagaEngine = null;
        DeferralSweeperEngine? sweeperEngine = null;
        OutboxRelayEngine? relayEngine = null;

        try
        {
            var loopTasks = new List<Task>();

            if (topicRegistrations.Count > 0)
            {
                topicEngine = new TopicConsumerEngine(
                    _transport,
                    _topicRegistry,
                    _options,
                    _stores.DeferralStore,
                    pipeline,
                    _logger);
                loopTasks.Add(topicEngine.RunAsync(ct));
            }

            if (sagaRegistrations.Count > 0)
            {
                sagaEngine = new SagaConsumerEngine(
                    _transport,
                    _serviceProvider!,
                    _sagaRegistry,
                    _options,
                    _stores.IdempotencyStore,
                    _stores.DeferralStore,
                    _stores.OutboxStore,
                    pipeline,
                    _logger);
                loopTasks.Add(sagaEngine.RunAsync(ct));

                if (_stores.OutboxStore is not null && sagaEngine.DispatchRoutes.Count > 0)
                {
                    relayEngine = new OutboxRelayEngine(
                        _stores.OutboxStore,
                        _transport,
                        _options,
                        _logger);
                    loopTasks.Add(relayEngine.RunAsync(ct));
                }
            }

            if (_stores.DeferralStore is not null)
            {
                sweeperEngine = new DeferralSweeperEngine(
                    _stores.DeferralStore,
                    _transport,
                    _options,
                    _logger);
                loopTasks.Add(sweeperEngine.RunAsync(ct));
            }

            if (loopTasks.Count > 0)
            {
                try
                {
                    await Task.WhenAll(loopTasks);
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    // Expected during shutdown; the finally block disposes engine resources.
                }
            }
        }
        finally
        {
            if (relayEngine is not null)
            {
                await relayEngine.DisposeAsync();
            }

            if (sweeperEngine is not null)
            {
                await sweeperEngine.DisposeAsync();
            }

            if (sagaEngine is not null)
            {
                await sagaEngine.DisposeAsync();
            }
        }
    }
}
