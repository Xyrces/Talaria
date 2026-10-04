// SPDX-License-Identifier: Apache-2.0

using System.Collections.Concurrent;
using Azure;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using Microsoft.Extensions.Logging;
using Talaria.Core.Abstractions;

namespace Talaria.Transports.AzureServiceBus;

/// <summary>
/// Azure Service Bus transport entry point. Implements
/// <see cref="ITransport"/> by caching one <see cref="ServiceBusSender"/> per
/// topic (senders are connection-multiplexed and safe to share) and one
/// <see cref="ServiceBusProcessor"/> per (topic, consumer-group) pair.
/// <para>
/// Entity naming: the transport treats every Talaria "topic" as an ASB queue
/// or topic entity — the host picks one when it provisions entities. The DLQ
/// is the source entity's name with <see cref="AzureServiceBusTransportOptions.DlqSuffix"/>
/// appended; both source and DLQ entities must already exist on the namespace
/// (topology provisioning is a separate concern tracked under
/// <see cref="Talaria.Core.Abstractions.ITopologyProvisioner"/>).
/// </para>
/// <para>
/// Lifecycle: the transport owns its <see cref="ServiceBusClient"/>, the
/// sender cache, and every processor it created. Disposing the transport
/// stops processors, closes senders, and disposes the client in a single
/// best-effort pass.
/// </para>
/// </summary>
/// <since>1.0.0</since>
public sealed class AzureServiceBusTransport : ITransport, ITopologyProvisioner, IAsyncDisposable
{
    private readonly AzureServiceBusTransportOptions _options;
    private readonly ILoggerFactory? _loggerFactory;
    private readonly ILogger? _logger;
    private readonly bool _includeDlqExceptionDetails;

    private readonly ServiceBusClient _client;
    private readonly ConcurrentDictionary<string, ServiceBusSender> _senders = new(StringComparer.Ordinal);

    // (entity, consumer-group) → processor. The transport is the sole owner
    // and disposes every processor it created on shutdown.
    private readonly ConcurrentDictionary<Guid, ServiceBusProcessor> _processors = new();
    private readonly ConcurrentDictionary<string, TopologyEntityKind> _entityKinds = new();

    // Disposal guard.
    private int _disposed;

    /// <summary>
    /// Creates the transport from the supplied options. The
    /// <see cref="ServiceBusClient"/> is created here and owned by the
    /// transport for its lifetime.
    /// </summary>
    /// <param name="options">Tuning knobs (connection string, DLQ suffix, etc.).</param>
    /// <param name="loggerFactory">Optional logger factory; used by senders/processors and this transport.</param>
    /// <param name="includeDlqExceptionDetails">
    /// When true, raw exception messages are written to DLQ headers. Mirrors
    /// <c>TalariaOptions.IncludeExceptionDetailsInDlq</c> — keep disabled in
    /// production.
    /// </param>
    /// <exception cref="ArgumentException">
    /// Thrown when neither <see cref="AzureServiceBusTransportOptions.ConnectionString"/>
    /// nor <see cref="AzureServiceBusTransportOptions.FullyQualifiedNamespace"/>
    /// is supplied.
    /// </exception>
    public AzureServiceBusTransport(
        AzureServiceBusTransportOptions options,
        ILoggerFactory? loggerFactory = null,
        bool includeDlqExceptionDetails = false)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _loggerFactory = loggerFactory;
        _logger = loggerFactory?.CreateLogger<AzureServiceBusTransport>();
        _includeDlqExceptionDetails = includeDlqExceptionDetails;

        if (string.IsNullOrWhiteSpace(_options.ConnectionString)
            && string.IsNullOrWhiteSpace(_options.FullyQualifiedNamespace))
        {
            throw new ArgumentException(
                $"{nameof(AzureServiceBusTransportOptions.ConnectionString)} or {nameof(AzureServiceBusTransportOptions.FullyQualifiedNamespace)} is required.",
                nameof(options));
        }

        _client = string.IsNullOrWhiteSpace(_options.ConnectionString)
            ? new ServiceBusClient(_options.FullyQualifiedNamespace, _options.Credential
                ?? throw new ArgumentException("Credential is required with FullyQualifiedNamespace.", nameof(options)))
            : new ServiceBusClient(_options.ConnectionString);
    }

    /// <summary>
    /// The transport's human-readable name, surfaced in logs and metrics.
    /// </summary>
    public string Name => "AzureServiceBus";

    /// <summary>
    /// The raw <see cref="ServiceBusClient"/> owned by this transport. Exposed
    /// so sibling extensions (e.g. <c>UseAzureServiceBusDeferral</c>) can
    /// share the same AMQP connection.
    /// </summary>
    public ServiceBusClient Client => _client;

    /// <inheritdoc />
    public Task<IConsumer<T>> CreateConsumerAsync<T>(
        string topic,
        ConsumerOptions options,
        CancellationToken ct = default)
    {
        var consumerGroup = string.IsNullOrEmpty(options.ConsumerGroup)
            ? "talaria-default"
            : options.ConsumerGroup;

        ObjectDisposedException.ThrowIf(_disposed != 0, this);
        var processorOptions = new ServiceBusProcessorOptions
        {
            MaxConcurrentCalls = 1,
            PrefetchCount = _options.PrefetchCount,
            AutoCompleteMessages = false,
            ReceiveMode = ServiceBusReceiveMode.PeekLock,
            MaxAutoLockRenewalDuration = _options.MaxAutoLockRenewalDuration,
        };
        var kind = _entityKinds.TryGetValue(topic, out var known) ? known : options.EntityKind;
        var processor = kind == TopologyEntityKind.Queue
            ? _client.CreateProcessor(topic, processorOptions)
            : _client.CreateProcessor(topic, consumerGroup, processorOptions);
        var processorId = Guid.NewGuid();
        _processors[processorId] = processor;
        var dlqSender = GetOrCreateSender(topic + _options.DlqSuffix);

        IConsumer<T> consumer = new AzureServiceBusConsumer<T>(
            processor,
            dlqSender,
            topic,
            topic + _options.DlqSuffix,
            options.BufferCapacity > 0 ? options.BufferCapacity : _options.BufferCapacity,
            _includeDlqExceptionDetails,
            _loggerFactory?.CreateLogger<AzureServiceBusConsumer<T>>(),
            () => _processors.TryRemove(processorId, out _));

        return Task.FromResult(consumer);
    }

    /// <inheritdoc />
    public Task<IProducer<T>> CreateProducerAsync<T>(
        string topic,
        ProducerOptions options,
        CancellationToken ct = default)
    {
        var sender = GetOrCreateSender(topic);
        IProducer<T> producer = new AzureServiceBusProducer<T>(sender, topic);
        return Task.FromResult(producer);
    }

    /// <inheritdoc />
    public Task<ITransactionalSession> BeginTransactionAsync(
        string? consumerGroup = null,
        TransactionOffsetSource? offsetSource = null,
        CancellationToken ct = default)
        => throw new NotSupportedException(
            "Azure Service Bus transport does not provide atomic Talaria transactions. Use a persistence outbox for reliable state changes and outbound messages.");

    /// <summary>
    /// Returns a cached <see cref="ServiceBusSender"/> for the given topic or
    /// entity name. Senders are thread-safe and connection-bound, so a single
    /// sender per topic is the canonical pattern.
    /// </summary>
    internal ServiceBusSender CheckoutSender(string topic)
        => GetOrCreateSender(topic);

    private ServiceBusSender GetOrCreateSender(string topic)
    {
        return _senders.GetOrAdd(topic, name =>
        {
            try
            {
                return _client.CreateSender(name);
            }
            catch
            {
                _senders.TryRemove(name, out _);
                throw;
            }
        });
    }

    /// <summary>
    /// Idempotently ensure that a queue or topic exists on the namespace.
    /// Best-effort: callers that lack management permissions get an
    /// <see cref="AggregateException"/> wrapping the original
    /// <see cref="RequestFailedException"/>, which the host can log and
    /// ignore. The transport itself does not auto-provision; this helper
    /// exists for the saga sample's `UseAzureServiceBusTransport` flow when
    /// the host wants the transport to declare entities on startup.
    /// </summary>
    public async Task EnsureEntityAsync(
        string entityName,
        TopologyEntityKind kind,
        CancellationToken ct = default)
    {
        var admin = CreateAdministrationClient();

        _entityKinds[entityName] = kind;
        if (kind == TopologyEntityKind.Queue)
        {
            if (!await admin.QueueExistsAsync(entityName, ct).ConfigureAwait(false))
            {
                var opts = new CreateQueueOptions(entityName)
                {
                    LockDuration = _options.LockDuration,
                    MaxDeliveryCount = _options.MaxRetries + 1,
                    DeadLetteringOnMessageExpiration = true,
                };
                await admin.CreateQueueAsync(opts, ct).ConfigureAwait(false);
            }

            var dlq = entityName + _options.DlqSuffix;
            if (!await admin.QueueExistsAsync(dlq, ct).ConfigureAwait(false))
            {
                var dlqOpts = new CreateQueueOptions(dlq)
                {
                    LockDuration = _options.LockDuration,
                    MaxDeliveryCount = _options.MaxRetries + 1,
                };
                await admin.CreateQueueAsync(dlqOpts, ct).ConfigureAwait(false);
            }
        }
        else if (kind == TopologyEntityKind.Topic)
        {
            if (!await admin.TopicExistsAsync(entityName, ct)) await admin.CreateTopicAsync(entityName, ct);
            if (!await admin.QueueExistsAsync(entityName + _options.DlqSuffix, ct))
                await admin.CreateQueueAsync(entityName + _options.DlqSuffix, ct);
        }
        else throw new ArgumentException("Subscriptions require a parent topic; use ProvisionAsync.", nameof(kind));
    }

    private ServiceBusAdministrationClient CreateAdministrationClient() => string.IsNullOrWhiteSpace(_options.ConnectionString)
        ? new ServiceBusAdministrationClient(_options.FullyQualifiedNamespace, _options.Credential!)
        : new ServiceBusAdministrationClient(_options.ConnectionString);

    public async Task ProvisionAsync(IEnumerable<TopologyDeclaration> declarations, CancellationToken ct = default)
    {
        var admin = CreateAdministrationClient();
        foreach (var declaration in declarations.OrderBy(x => x.Kind == TopologyEntityKind.Subscription ? 1 : 0))
        {
            if (declaration.Kind != TopologyEntityKind.Subscription)
                await EnsureEntityAsync(declaration.Name, declaration.Kind, ct);
            else
            {
                if (string.IsNullOrWhiteSpace(declaration.ParentName)) throw new ArgumentException("A subscription requires a parent topic.");
                if (!await admin.SubscriptionExistsAsync(declaration.ParentName, declaration.Name, ct))
                    await admin.CreateSubscriptionAsync(new CreateSubscriptionOptions(declaration.ParentName, declaration.Name)
                    {
                        LockDuration = declaration.LockDuration ?? _options.LockDuration,
                        MaxDeliveryCount = declaration.MaxDeliveryCount ?? _options.MaxRetries + 1,
                    }, ct);
            }
        }
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        foreach (var processor in _processors.Values)
        {
            try
            {
                await processor.StopProcessingAsync().ConfigureAwait(false);
            }
            catch
            {
                // Best effort — host is shutting down.
            }

            try
            {
                await processor.DisposeAsync().ConfigureAwait(false);
            }
            catch
            {
                // Best effort.
            }
        }
        _processors.Clear();

        foreach (var sender in _senders.Values)
        {
            try
            {
                await sender.CloseAsync().ConfigureAwait(false);
            }
            catch
            {
                // Best effort.
            }
        }
        _senders.Clear();

        try
        {
            await _client.DisposeAsync().ConfigureAwait(false);
        }
        catch
        {
            // Best effort.
        }
    }
}
