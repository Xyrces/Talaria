// SPDX-License-Identifier: Apache-2.0

using System.Collections.Concurrent;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using Talaria.Core.Abstractions;
using Talaria.Core.Hosting;

namespace Talaria.Core.Requesting;

/// <summary>
/// Factory that creates typed <see cref="IRequestClient{TRequest}"/> instances and manages
/// the shared per-factory inbox pump used to collect responses.
/// </summary>
/// <remarks>
/// Each factory owns a dedicated reply topic and consumer group, so multiple factories in
/// the same process receive isolated inboxes. Response delivery is at-least-once; the pump
/// completes each pending request on the first matching response and ignores duplicates.
/// </remarks>
public sealed class RequestClientFactory : IAsyncDisposable
{
    private readonly ITransport _transport;
    private readonly TalariaOptions _options;
    private readonly ILogger<RequestClientFactory> _logger;
    private readonly ITopologyProvisioner? _provisioner;
    private readonly ProducerCache _producerCache;
    private readonly ConcurrentDictionary<string, PendingRequest> _pending = new();
    private readonly SemaphoreSlim _initLock = new(1, 1);
    private readonly SemaphoreSlim _pumpStartLock = new(1, 1);

    private readonly string _inboxTopic;
    private readonly string _consumerGroup;

    private Task? _initializationTask;
    private Task? _pumpTask;
    private CancellationTokenSource? _pumpCts;
    private volatile bool _disposed;

    /// <summary>
    /// Creates a new request client factory.
    /// </summary>
    /// <param name="transport">The transport used to produce requests and consume responses.</param>
    /// <param name="options">Global Talaria options.</param>
    /// <param name="loggerFactory">Logger factory.</param>
    /// <param name="provisioner">Optional topology provisioner for transports that require explicit entity creation.</param>
    public RequestClientFactory(
        ITransport transport,
        TalariaOptions options,
        ILoggerFactory loggerFactory,
        ITopologyProvisioner? provisioner = null)
    {
        _transport = transport;
        _options = options;
        _logger = loggerFactory.CreateLogger<RequestClientFactory>();
        _provisioner = provisioner;
        _producerCache = new ProducerCache(transport);

        var suffix = Guid.NewGuid().ToString("N");
        _inboxTopic = $"{options.ApplicationName}-replies-{suffix}";
        _consumerGroup = $"{options.ApplicationName}-replies-{suffix}";
    }

    /// <summary>
    /// Creates a typed request client bound to the destination topic.
    /// </summary>
    /// <typeparam name="TRequest">The CLR request type.</typeparam>
    /// <param name="topic">The topic to which requests are published.</param>
    /// <returns>A request client.</returns>
    public IRequestClient<TRequest> CreateClient<TRequest>(string topic)
        where TRequest : class
    {
        return new RequestClient<TRequest>(this, topic);
    }

    internal string InboxTopic => _inboxTopic;

    internal async Task<TResponse> RequestAsync<TRequest, TResponse>(
        string topic,
        TRequest request,
        CancellationToken ct)
        where TRequest : class
        where TResponse : class
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await InitializeAsync(ct).ConfigureAwait(false);
        await EnsurePumpStartedAsync(ct).ConfigureAwait(false);

        var requestId = Guid.NewGuid().ToString("N");
        var tcs = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
        var pending = new PendingRequest(tcs, typeof(TResponse));
        _pending[requestId] = pending;

        // Disposal may have begun between the check above and this insert; the pump is
        // already cancelled and the dispose loop already ran, so fail fast instead of
        // hanging until the timeout.
        if (_disposed && _pending.TryRemove(requestId, out _))
        {
            throw new ObjectDisposedException(nameof(RequestClientFactory));
        }

        CancellationTokenSource? timeoutCts = null;
        try
        {
            // Use a standalone (non-linked) timeout source. Caller cancellation is observed
            // by WaitAsync(ct) and surfaces as OperationCanceledException. The timeout callback
            // alone completes the TCS with RequestTimeoutException.
            timeoutCts = new CancellationTokenSource(_options.DefaultRequestTimeout);
            pending.TimeoutRegistration = timeoutCts.Token.Register(() =>
            {
                if (tcs.TrySetException(new RequestTimeoutException(requestId)))
                {
                    _pending.TryRemove(requestId, out _);
                }
            });

            var invoker = await _producerCache.GetOrCreateAsync(topic, typeof(TRequest), ct).ConfigureAwait(false);
            var headers = new MessageHeaders
            {
                RequestId = requestId,
                ReplyTo = _inboxTopic,
            };
            await invoker.Produce(request, headers, null, ct).ConfigureAwait(false);

            return (TResponse)await tcs.Task.WaitAsync(ct).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (timeoutCts is not null && !timeoutCts.IsCancellationRequested)
        {
            _pending.TryRemove(requestId, out _);
            throw;
        }
        catch
        {
            _pending.TryRemove(requestId, out _);
            throw;
        }
        finally
        {
            pending.DisposeRegistrations();
            timeoutCts?.Dispose();
        }
    }

    private async Task InitializeAsync(CancellationToken ct)
    {
        if (_initializationTask is not null)
        {
            await _initializationTask.ConfigureAwait(false);
            return;
        }

        await _initLock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            if (_initializationTask is not null)
            {
                await _initializationTask.ConfigureAwait(false);
                return;
            }

            if (_provisioner is null)
            {
                _initializationTask = Task.CompletedTask;
                return;
            }

            _initializationTask = ProvisionInboxAsync(ct);
            await _initializationTask.ConfigureAwait(false);
        }
        finally
        {
            _initLock.Release();
        }
    }

    private async Task ProvisionInboxAsync(CancellationToken ct)
    {
        try
        {
            await _provisioner!.ProvisionAsync(
                new[] { new TopologyDeclaration(TopologyEntityKind.Queue, _inboxTopic) },
                ct).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "ITopologyProvisioner failed to provision inbox '{Inbox}'; the transport may auto-create entities or the requester may fail if auto-creation is disabled.",
                _inboxTopic);
        }
    }

    private async Task EnsurePumpStartedAsync(CancellationToken ct)
    {
        if (_pumpTask is not null)
        {
            return;
        }

        await _pumpStartLock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            if (_pumpTask is not null)
            {
                return;
            }

            _pumpCts = new CancellationTokenSource();
            // Supervised like the engine consumer loops: a faulting inbox pump is
            // restarted with backoff instead of silently hanging every pending and
            // future request. Pending requests survive restarts; uncommitted inbox
            // messages redeliver to the replacement consumer.
            _pumpTask = ConsumerSupervision.RunSupervisedAsync(
                $"request-inbox:{_inboxTopic}", RunPumpAsync, _logger, _pumpCts.Token);
        }
        finally
        {
            _pumpStartLock.Release();
        }
    }

    private async Task RunPumpAsync(CancellationToken ct)
    {
        var inboxConsumer = await _transport.CreateConsumerAsync<JsonElement>(
            _inboxTopic,
            new ConsumerOptions { ConsumerGroup = _consumerGroup },
            ct).ConfigureAwait(false);

        try
        {
            await foreach (var envelope in inboxConsumer.ConsumeAsync(ct).ConfigureAwait(false))
            {
                var requestId = envelope.Headers.RequestId;
                if (string.IsNullOrEmpty(requestId) || !_pending.TryGetValue(requestId, out var pending))
                {
                    await inboxConsumer.CommitAsync(envelope, ct).ConfigureAwait(false);
                    continue;
                }

                // The TCS arbitrates first-wins: a response that arrives while the timeout
                // fires is never dropped. Only the caller that successfully completes the TCS
                // removes the pending entry.
                bool completed = false;
                if (envelope.Headers.RequestFault)
                {
                    var exceptionType = envelope.Headers.TryGetValue(RequestClientFaultHeaders.ExceptionTypeKey, out var et) ? et : null;
                    var message = envelope.Headers.TryGetValue(RequestClientFaultHeaders.ExceptionMessageKey, out var em)
                        ? em
                        : "The responder faulted while processing the request. Enable IncludeExceptionDetailsInDlq on the responder for details.";
                    completed = pending.Tcs.TrySetException(new RequestFaultException(requestId, exceptionType, message));
                }
                else
                {
                    try
                    {
                        var response = JsonSerializer.Deserialize(envelope.Payload, pending.ResponseType);
                        if (response is null)
                        {
                            completed = pending.Tcs.TrySetException(new InvalidOperationException(
                                $"Response for request '{requestId}' deserialized to null."));
                        }
                        else
                        {
                            completed = pending.Tcs.TrySetResult(response);
                        }
                    }
                    catch (Exception ex)
                    {
                        completed = pending.Tcs.TrySetException(ex);
                    }
                }

                if (completed)
                {
                    _pending.TryRemove(requestId, out _);
                }

                pending.DisposeRegistrations();
                await inboxConsumer.CommitAsync(envelope, ct).ConfigureAwait(false);
            }
        }
        finally
        {
            await inboxConsumer.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Stops the shared inbox pump and disposes the inbox consumer. Disposing individual
    /// <see cref="IRequestClient{TRequest}"/> instances does not affect the shared pump.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        _pumpCts?.Cancel();

        if (_pumpTask is not null)
        {
            try
            {
                await _pumpTask.WaitAsync(TimeSpan.FromSeconds(10)).ConfigureAwait(false);
            }
            catch
            {
                // Best effort.
            }
        }

        _pumpCts?.Dispose();

        foreach (var kvp in _pending.ToArray())
        {
            if (_pending.TryRemove(kvp.Key, out var pending))
            {
                pending.Tcs.TrySetException(new ObjectDisposedException(nameof(RequestClientFactory)));
                pending.DisposeRegistrations();
            }
        }

        // The semaphores are deliberately not disposed: an in-flight RequestAsync may still
        // be waiting on them when disposal runs, and SemaphoreSlim holds no unmanaged
        // resources unless AvailableWaitHandle is accessed (it is not).

        await _producerCache.DisposeAsync().ConfigureAwait(false);
    }
}
