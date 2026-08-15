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
    private IConsumer<JsonElement>? _inboxConsumer;
    private Task? _pumpTask;
    private CancellationTokenSource? _pumpCts;
    private bool _disposed;

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
        EnsurePumpStarted();

        var requestId = Guid.NewGuid().ToString("N");
        var tcs = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);

        var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeoutCts.CancelAfter(_options.DefaultRequestTimeout);

        var registration = timeoutCts.Token.Register(() =>
        {
            if (_pending.TryRemove(requestId, out var pending))
            {
                pending.DisposeRegistrations();
                tcs.TrySetException(new RequestTimeoutException(requestId));
            }
        });

        var pending = new PendingRequest(tcs, typeof(TResponse), registration);
        _pending[requestId] = pending;

        try
        {
            var invoker = await _producerCache.GetOrCreateAsync(topic, typeof(TRequest), ct).ConfigureAwait(false);
            var headers = new MessageHeaders
            {
                RequestId = requestId,
                ReplyTo = _inboxTopic,
            };
            await invoker.Produce(request, headers, null, ct).ConfigureAwait(false);
        }
        catch (Exception)
        {
            _pending.TryRemove(requestId, out _);
            pending.DisposeRegistrations();
            timeoutCts.Dispose();
            throw;
        }

        try
        {
            var result = await tcs.Task.WaitAsync(ct).ConfigureAwait(false);
            return (TResponse)result;
        }
        catch (OperationCanceledException) when (timeoutCts.IsCancellationRequested && !ct.IsCancellationRequested)
        {
            throw new RequestTimeoutException(requestId);
        }
        finally
        {
            pending.DisposeRegistrations();
            timeoutCts.Dispose();
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
            _logger.LogDebug(
                ex,
                "ITopologyProvisioner failed to provision inbox '{Inbox}'; the transport may auto-create entities.",
                _inboxTopic);
        }
    }

    private void EnsurePumpStarted()
    {
        if (_pumpTask is not null)
        {
            return;
        }

        _pumpStartLock.Wait();
        try
        {
            if (_pumpTask is not null)
            {
                return;
            }

            _pumpCts = new CancellationTokenSource();
            _pumpTask = RunPumpAsync(_pumpCts.Token);
        }
        finally
        {
            _pumpStartLock.Release();
        }
    }

    private async Task RunPumpAsync(CancellationToken ct)
    {
        try
        {
            _inboxConsumer = await _transport.CreateConsumerAsync<JsonElement>(
                _inboxTopic,
                new ConsumerOptions { ConsumerGroup = _consumerGroup },
                ct).ConfigureAwait(false);

            await foreach (var envelope in _inboxConsumer.ConsumeAsync(ct).ConfigureAwait(false))
            {
                var requestId = envelope.Headers.RequestId;
                if (string.IsNullOrEmpty(requestId) || !_pending.TryRemove(requestId, out var pending))
                {
                    await _inboxConsumer.CommitAsync(envelope, ct).ConfigureAwait(false);
                    continue;
                }

                pending.DisposeRegistrations();

                if (envelope.Headers.RequestFault)
                {
                    var exceptionType = envelope.Headers.TryGetValue(RequestClientFaultHeaders.ExceptionTypeKey, out var et) ? et : null;
                    var message = _options.IncludeExceptionDetailsInDlq
                        && envelope.Headers.TryGetValue(RequestClientFaultHeaders.ExceptionMessageKey, out var em)
                        ? em
                        : "The responder faulted while processing the request. Enable IncludeExceptionDetailsInDlq for details.";
                    pending.Tcs.TrySetException(new RequestFaultException(requestId, exceptionType, message));
                }
                else
                {
                    try
                    {
                        var response = JsonSerializer.Deserialize(envelope.Payload, pending.ResponseType);
                        if (response is null)
                        {
                            pending.Tcs.TrySetException(new InvalidOperationException(
                                $"Response for request '{requestId}' deserialized to null."));
                        }
                        else
                        {
                            pending.Tcs.TrySetResult(response);
                        }
                    }
                    catch (Exception ex)
                    {
                        pending.Tcs.TrySetException(ex);
                    }
                }

                await _inboxConsumer.CommitAsync(envelope, ct).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // Expected on disposal.
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Request/response inbox pump for '{Inbox}' faulted.", _inboxTopic);
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
        _initLock.Dispose();
        _pumpStartLock.Dispose();

        if (_inboxConsumer is not null)
        {
            await _inboxConsumer.DisposeAsync().ConfigureAwait(false);
        }

        await _producerCache.DisposeAsync().ConfigureAwait(false);
    }
}
