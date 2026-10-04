// SPDX-License-Identifier: Apache-2.0

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Talaria.Core.Abstractions;
using Talaria.Core.Registration;
using Talaria.Core.Requesting;
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
    private readonly RequestClientFactory _requestClientFactory;

    private readonly object _lifecycleLock = new();
    private CancellationTokenSource? _runCts;
    private CancellationTokenSource? _intakeCts;
    private Task? _stopTask;
    private Task? _runTask;
    private bool _stopped;
    private bool _disposed;
    private readonly TaskCompletionSource _started = new(TaskCreationOptions.RunContinuationsAsynchronously);
    public Task Completion => _runTask ?? Task.CompletedTask;
    public TalariaHealth Health { get; } = new();

    /// <summary>
    /// Creates a new listener.
    /// </summary>
    /// <param name="transport">The transport that provides consumers and producers.</param>
    /// <param name="topicRegistry">Registry of topic handlers.</param>
    /// <param name="sagaRegistry">Registry of saga configurations.</param>
    /// <param name="options">Global Talaria options.</param>
    /// <param name="logger">Logger for listener diagnostics.</param>
    /// <param name="serviceProvider">
    /// Required when sagas or class-based topic consumers are registered. Used to resolve
    /// state stores, create consumer scopes, and resolve <see cref="ITopicConsumer{T}"/>
    /// instances by concrete type. When supplied and <paramref name="stores"/> is omitted,
    /// the optional stores are resolved from this provider at construction time.
    /// </param>
    /// <param name="stores">
    /// Optional stores supplied directly. When omitted and a <paramref name="serviceProvider"/>
    /// is supplied, the stores are resolved from the provider.
    /// </param>
    /// <param name="loggerFactory">
    /// Optional logger factory used by the internal <see cref="RequestClientFactory"/> created
    /// by <see cref="CreateRequestClient{TRequest}(string)"/>. When omitted, the listener's
    /// own logger is reused for all request/response diagnostics.
    /// </param>
    public TalariaListener(
        ITransport transport,
        TopicRegistry topicRegistry,
        SagaRegistry sagaRegistry,
        TalariaOptions options,
        ILogger<TalariaListener> logger,
        IServiceProvider? serviceProvider = null,
        TalariaListenerStores? stores = null,
        ILoggerFactory? loggerFactory = null)
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
        _requestClientFactory = new RequestClientFactory(
            transport,
            options,
            loggerFactory ?? new SingleLoggerFactory(logger),
            serviceProvider?.GetService<ITopologyProvisioner>());
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
                return _runTask is { IsCompleted: false };
            }
        }
    }

    /// <summary>
    /// Creates a typed request client bound to the supplied destination topic.
    /// </summary>
    /// <typeparam name="TRequest">The CLR request type.</typeparam>
    /// <param name="topic">The topic to which requests are published.</param>
    /// <returns>A request client sharing the listener's internal request client factory.</returns>
    /// <remarks>
    /// The factory and its inbox pump are owned by the listener and disposed with it.
    /// </remarks>
    public IRequestClient<TRequest> CreateRequestClient<TRequest>(string topic)
        where TRequest : class
    {
        return _requestClientFactory.CreateClient<TRequest>(topic);
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
                return _started.Task;
            }

            if (_sagaRegistry.Registrations.Count > 0 && _serviceProvider is null)
            {
                throw new InvalidOperationException(
                    "Sagas are registered but no IServiceProvider was supplied to TalariaListener. " +
                    "A service provider is required to resolve IStateStore<TState> instances and create handler scopes.");
            }

            if (_topicRegistry.Registrations.Any(r => r.ConsumerType is not null || r.ScopedHandler is not null) && _serviceProvider is null)
            {
                throw new InvalidOperationException(
                    "One or more topic registrations use class-based consumers but no IServiceProvider was supplied to TalariaListener. " +
                    "A service provider is required to resolve ITopicConsumer<T> instances and create per-message scopes.");
            }

            _runCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            _intakeCts = CancellationTokenSource.CreateLinkedTokenSource(_runCts.Token);
            Health.SetState(ListenerState.Starting);
            _runTask = RunAndSignalAsync(_runCts.Token);

            if (_runTask.IsCompleted)
            {
                if (_runTask.IsFaulted)
                {
                    _stopped = true;
                    _intakeCts.Dispose();
                    _runCts.Dispose();
                }

                return _runTask;
            }

            return _started.Task;
        }
    }

    /// <summary>
    /// Cancels all loops, awaits their exit, and disposes listener-created consumers
    /// and producers. Idempotent after stop.
    /// </summary>
    public Task StopAsync(CancellationToken cancellationToken = default)
    {
        lock (_lifecycleLock)
        {
            if (_stopTask is not null) return _stopTask.WaitAsync(cancellationToken);
            if (_disposed || _stopped || _runTask is null) return Task.CompletedTask;
            _stopped = true;
            _stopTask = StopCoreAsync(cancellationToken);
            return _stopTask;
        }
    }

    private async Task StopCoreAsync(CancellationToken cancellationToken)
    {
        Health.SetState(ListenerState.Draining);
        await _intakeCts!.CancelAsync();
        try
        {
            try { await _runTask!.WaitAsync(_options.ShutdownDrainTimeout, cancellationToken); }
            catch (TimeoutException)
            {
                _logger.LogWarning("Talaria drain timed out; canceling active handlers.");
                await _runCts!.CancelAsync();
                // Cancellation is cooperative; bound the second wait too.
                await _runTask!.WaitAsync(_options.ShutdownDrainTimeout, cancellationToken);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            await _runCts!.CancelAsync();
            throw;
        }
        catch (TimeoutException)
        {
            Health.SetState(ListenerState.Faulted);
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Talaria listener terminated during shutdown.");
        }
        finally
        {
            if (_runTask!.IsCompleted)
            {
                _intakeCts.Dispose();
                _runCts!.Dispose();
                Health.SetState(ListenerState.Stopped);
            }
        }
    }
    /// <summary>
    /// Stops the listener if it is running and disposes the internal request client factory.
    /// Does not dispose caller-owned transports or stores.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        lock (_lifecycleLock)
        {
            if (_disposed)
            {
                return;
            }
        }

        await StopAsync();
        await _requestClientFactory.DisposeAsync();

        lock (_lifecycleLock)
        {
            _disposed = true;
        }

        GC.SuppressFinalize(this);
    }

    /// <summary>Exports all mapped broker entities, including saga subscriptions, for external provisioning.</summary>
    public IReadOnlyList<TopologyDeclaration> GetTopology()
    {
        var declarations = _topicRegistry.GetTopology(_options).ToList();
        foreach (var saga in _sagaRegistry.Registrations)
        foreach (var step in saga.Steps)
        {
            declarations.Add(new(TopologyEntityKind.Topic, step.TopicName));
            declarations.Add(new(TopologyEntityKind.Subscription,
                SagaConsumerEngine.EndpointName(_options.ApplicationName, saga.StateType, step.TopicName), step.TopicName));
        }
        if (declarations.GroupBy(x => x.Name).Any(g => g.Any(x => x.Kind == TopologyEntityKind.Queue) && g.Any(x => x.Kind == TopologyEntityKind.Topic)))
            throw new InvalidOperationException("A destination cannot be both a command queue and an event topic.");
        return declarations.Distinct().ToArray();
    }
    private async Task RunAndSignalAsync(CancellationToken ct)
    {
        try { await RunAsync(ct); }
        catch (Exception ex)
        {
            Health.SetState(ListenerState.Faulted);
            _started.TrySetException(ex);
            throw;
        }
    }

    private async Task RunAsync(CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        var validation = new TalariaOptionsValidator().Validate(null, _options);
        if (validation.Failed) throw new InvalidOperationException(validation.FailureMessage);
        _topicRegistry.Seal();
        _sagaRegistry.Seal();

        var topicRegistrations = _topicRegistry.Registrations;
        var sagaRegistrations = _sagaRegistry.Registrations;

        if (_serviceProvider is not null && _serviceProvider.GetServices<ITransport>().Skip(1).Any())
            throw new InvalidOperationException("Select one Talaria transport per host.");
        if ((topicRegistrations.Any(r => RetryPolicy.IsEnabled(r.RetryPolicy ?? _options.DefaultRetryPolicy))
            || sagaRegistrations.Count > 0 && RetryPolicy.IsEnabled(_options.DefaultRetryPolicy)) && _stores.DeferralStore is null)
            throw new InvalidOperationException("Delayed retries require persistence. Configure UseInMemory(), UseRedisPersistence(), or UseSqlServerPersistence().");
        if (topicRegistrations.Any(r => r.Transactional) && _serviceProvider?.GetService<IServiceProviderIsService>()?.IsService(typeof(IMessageTransaction)) != true)
            throw new InvalidOperationException("Transactional endpoints require a registered IMessageTransaction provider.");
        if (sagaRegistrations.Any(r => r.DispatchTopics.Count > 0) && _stores.OutboxStore is null)
            throw new InvalidOperationException("Saga dispatch requires an outbox in the same persistence store. Configure UseInMemory(), UseRedisPersistence(), or UseSqlServerPersistence().");
        var declarations = GetTopology();
        if (_options.AutoProvisionTopology)
        {
            var provisioner = _serviceProvider?.GetService<ITopologyProvisioner>() ?? _transport as ITopologyProvisioner;
            if (declarations.Count > 0 && provisioner is not null)
                await provisioner.ProvisionAsync(declarations, ct);
        }
        var failures = _serviceProvider?.GetService<IFailedMessageStore>();
        var pipeline = new MessageProcessingPipeline(_stores.IdempotencyStore, _options, _logger, failures);

        TopicConsumerEngine? topicEngine = null;
        SagaConsumerEngine? sagaEngine = null;
        DeferralSweeperEngine? sweeperEngine = null;
        OutboxRelayEngine? relayEngine = null;
        var loopTasks = new List<Task>();

        try
        {

            if (topicRegistrations.Count > 0)
            {
                topicEngine = new TopicConsumerEngine(
                    _transport,
                    _topicRegistry,
                    _options,
                    _stores.DeferralStore,
                    pipeline,
                    _logger,
                    _serviceProvider);
                loopTasks.Add(topicEngine.RunAsync(ct, _intakeCts!.Token, Health));
            }

            if (sagaRegistrations.Count > 0)
            {
                sagaEngine = new SagaConsumerEngine(
                    _transport,
                    _serviceProvider!,
                    _sagaRegistry,
                    _options,
                    _stores.DeferralStore,
                    _stores.OutboxStore,
                    pipeline,
                    _logger);
                loopTasks.Add(sagaEngine.RunAsync(ct, _intakeCts!.Token, Health));

            }

            if (_stores.OutboxStore is not null)
            {
                relayEngine = new OutboxRelayEngine(_stores.OutboxStore, _transport, _options, _logger, failures, Health);
                loopTasks.Add(relayEngine.RunAsync(_intakeCts!.Token));
            }

            if (_stores.DeferralStore is not null)
            {
                sweeperEngine = new DeferralSweeperEngine(
                    _stores.DeferralStore,
                    _transport,
                    _options,
                    _logger, failures, Health);
                loopTasks.Add(sweeperEngine.RunAsync(_intakeCts!.Token));
            }

            Health.SetState(ListenerState.Running);
            _started.TrySetResult();
            if (loopTasks.Count > 0)
            {
                try
                {
                    var remaining = loopTasks.ToList();
                    while (remaining.Count > 0)
                    {
                        var completed = await Task.WhenAny(remaining);
                        try { await completed; } // Surface failures without waiting for unrelated endless loops.
                        catch (OperationCanceledException) when (ct.IsCancellationRequested || _intakeCts!.IsCancellationRequested)
                        {
                            // Relays stop immediately; active handlers still have their drain window.
                        }
                        remaining.Remove(completed);
                    }
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested || _intakeCts!.IsCancellationRequested)
                {
                    // Expected during shutdown; the finally block disposes engine resources.
                }
            }
        }
        finally
        {
            await _runCts!.CancelAsync();
            try { await Task.WhenAll(loopTasks); }
            catch (Exception) { /* Preserve the original engine error; all loops are now stopped. */ }
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

            if (topicEngine is not null)
            {
                await topicEngine.DisposeAsync();
            }
        }
    }

    /// <summary>
    /// Logger factory wrapper that returns the listener's logger for every category.
    /// Used by the internal <see cref="RequestClientFactory"/> when the listener is
    /// constructed without an explicit logger factory.
    /// </summary>
    private sealed class SingleLoggerFactory : ILoggerFactory
    {
        private readonly ILogger _logger;

        public SingleLoggerFactory(ILogger logger)
        {
            _logger = logger;
        }

        public ILogger CreateLogger(string categoryName) => _logger;

        public void AddProvider(ILoggerProvider provider) { }

        public void Dispose() { }
    }
}
