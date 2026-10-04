// SPDX-License-Identifier: Apache-2.0
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Talaria.Core.Abstractions;

namespace Talaria.Transports.RabbitMq;

public sealed class RabbitMqOptions
{
    public string ConnectionString { get; set; } = "amqp://guest:guest@localhost:5672/";
    public ushort PrefetchCount { get; set; } = 16;
    public string DlqSuffix { get; set; } = ".dlq";
}

/// <summary>Durable RabbitMQ messaging with manual settlement and publisher confirms.</summary>
public sealed class RabbitMqTransport(RabbitMqOptions options) : ITransport, ITopologyProvisioner, IAsyncDisposable
{
    private readonly SemaphoreSlim _connectionGate = new(1, 1);
    private IConnection? _connection;
    private bool _disposed;
    public string Name => "RabbitMQ";

    private async Task<IChannel> ChannelAsync(bool confirms, CancellationToken ct)
    {
        await _connectionGate.WaitAsync(ct);
        try
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            _connection ??= await new ConnectionFactory
            {
                Uri = new Uri(options.ConnectionString), AutomaticRecoveryEnabled = true,
                TopologyRecoveryEnabled = true,
            }.CreateConnectionAsync(ct);
            return await _connection.CreateChannelAsync(new CreateChannelOptions(confirms, confirms), ct);
        }
        finally { _connectionGate.Release(); }
    }

    public async Task ProvisionAsync(IEnumerable<TopologyDeclaration> declarations, CancellationToken ct = default)
    {
        await using var channel = await ChannelAsync(false, ct);
        foreach (var item in declarations.OrderBy(x => x.Kind == TopologyEntityKind.Subscription ? 1 : 0))
        {
            var exchange = item.ParentName ?? item.Name;
            await channel.ExchangeDeclareAsync(exchange, ExchangeType.Fanout, durable: true, autoDelete: false, cancellationToken: ct);
            if (item.Kind is TopologyEntityKind.Queue or TopologyEntityKind.Subscription)
            {
                await channel.QueueDeclareAsync(item.Name, durable: true, exclusive: false, autoDelete: false, cancellationToken: ct);
                await channel.QueueBindAsync(item.Name, exchange, "", cancellationToken: ct);
            }
            var dlq = exchange + options.DlqSuffix;
            await channel.ExchangeDeclareAsync(dlq, ExchangeType.Fanout, durable: true, autoDelete: false, cancellationToken: ct);
            await channel.QueueDeclareAsync(dlq, durable: true, exclusive: false, autoDelete: false, cancellationToken: ct);
            await channel.QueueBindAsync(dlq, dlq, "", cancellationToken: ct);
        }
    }

    public async Task<IProducer<T>> CreateProducerAsync<T>(string topic, ProducerOptions producerOptions, CancellationToken ct = default)
        => new Producer<T>(await ChannelAsync(true, ct), topic);

    public async Task<IConsumer<T>> CreateConsumerAsync<T>(string topic, ConsumerOptions consumerOptions, CancellationToken ct = default)
        => new Consumer<T>(await ChannelAsync(false, ct), await ChannelAsync(true, ct), topic,
            consumerOptions.EntityKind == TopologyEntityKind.Queue ? topic : consumerOptions.ConsumerGroup
                ?? throw new ArgumentException("An event subscription requires a consumer group."), options);

    public Task<ITransactionalSession> BeginTransactionAsync(string? consumerGroup = null, TransactionOffsetSource? offsetSource = null, CancellationToken ct = default)
        => throw new NotSupportedException("Use a persistence outbox for atomic Talaria dispatch; RabbitMQ publisher confirms do not provide a database transaction.");

    public async ValueTask DisposeAsync()
    {
        await _connectionGate.WaitAsync();
        try
        {
            if (_disposed) return;
            _disposed = true;
            if (_connection is not null) await _connection.DisposeAsync();
        }
        finally { _connectionGate.Release(); }
    }

    private sealed class Producer<T>(IChannel channel, string destination) : IProducer<T>
    {
        private readonly SemaphoreSlim _gate = new(1, 1);
        public async Task ProduceAsync(T message, MessageHeaders? headers = null, string? partitionKey = null, CancellationToken ct = default)
        {
            var copy = headers is null ? new MessageHeaders() : new MessageHeaders(headers);
            copy.MessageId ??= Guid.NewGuid().ToString("N");
            if (typeof(T) != typeof(JsonElement) || !copy.ContainsKey(MessageHeaders.MessageTypeKey))
                copy[MessageHeaders.MessageTypeKey] = typeof(T).FullName ?? typeof(T).Name;
            if (copy.ContainsKey(MessageHeaders.HopCountKey)) copy.HopCount++;
            if (System.Diagnostics.Activity.Current is { } activity)
            {
                copy.TraceParent ??= activity.Id;
                copy.TraceState ??= activity.TraceStateString;
            }
            if (partitionKey is not null) copy["talaria.partition_key"] = partitionKey;
            await _gate.WaitAsync(ct);
            try
            {
                await channel.BasicPublishAsync(destination, "", mandatory: !copy.TryGetValue("talaria.intent", out var intent) || intent != "event",
                    basicProperties: new BasicProperties
                    {
                        Persistent = true, ContentType = "application/json", MessageId = copy.MessageId,
                        Headers = copy.ToDictionary(x => x.Key, x => (object?)x.Value),
                    }, body: JsonSerializer.SerializeToUtf8Bytes(message), cancellationToken: ct);
            }
            finally { _gate.Release(); }
        }
        public ValueTask DisposeAsync() => channel.DisposeAsync();
    }

    private sealed class Consumer<T>(IChannel channel, IChannel publisher, string topic, string queue, RabbitMqOptions options) : IConsumer<T>, IConsumerReadiness
    {
        private int _started;
        private readonly CancellationTokenSource _stop = new();
        private readonly System.Threading.Channels.Channel<MessageEnvelope<T>> _messages =
            System.Threading.Channels.Channel.CreateBounded<MessageEnvelope<T>>(Math.Max(1, (int)options.PrefetchCount));
        private readonly SemaphoreSlim _settlement = new(1, 1);
        private readonly TaskCompletionSource _ready = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public Task Ready => _ready.Task;

        public IAsyncEnumerable<MessageEnvelope<T>> ConsumeAsync(CancellationToken ct = default)
        {
            SingleEnumerationGuard.ThrowIfAlreadyStarted(ref _started);
            return Read(ct);
        }

        private int _enumerated;
        private async IAsyncEnumerable<MessageEnvelope<T>> Read([EnumeratorCancellation] CancellationToken ct)
        {
            SingleEnumerationGuard.ThrowIfAlreadyStarted(ref _enumerated);
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, _stop.Token);
            var consumer = new AsyncEventingBasicConsumer(channel);
            consumer.ReceivedAsync += async (_, args) =>
            {
                try
                {
                    var headers = new MessageHeaders();
                    if (args.BasicProperties.Headers is { } values)
                        foreach (var (key, value) in values)
                            if (value is not null) headers[key] = value is byte[] bytes ? Encoding.UTF8.GetString(bytes) : value.ToString()!;
                    headers.MessageId = args.BasicProperties.MessageId ?? headers.MessageId;
                    T? payload;
                    try { payload = JsonSerializer.Deserialize<T>(args.Body.Span); }
                    catch (JsonException)
                    {
                        await DeadLetterRaw(args.Body.ToArray(), headers, args.DeliveryTag, linked.Token);
                        return;
                    }
                    if (payload is null)
                    {
                        await DeadLetterRaw(args.Body.ToArray(), headers, args.DeliveryTag, linked.Token);
                        return;
                    }
                    await _messages.Writer.WriteAsync(new MessageEnvelope<T>
                    {
                        Payload = payload, Headers = headers, SourceTopic = topic, Offset = checked((long)args.DeliveryTag),
                        PartitionKey = headers.TryGetValue("talaria.partition_key", out var partition) ? partition : null,
                        CorrelationId = headers.TryGetValue(MessageHeaders.CorrelationIdKey, out var correlation) ? correlation : null,
                        Timestamp = DateTimeOffset.UtcNow,
                    }, linked.Token);
                }
                catch (Exception ex) { _messages.Writer.TryComplete(ex); }
            };
            consumer.ShutdownAsync += (_, args) =>
            {
                var error = new IOException($"RabbitMQ consumer stopped: {args.ReplyText}");
                _ready.TrySetException(error);
                _messages.Writer.TryComplete(error);
                return Task.CompletedTask;
            };
            await channel.BasicQosAsync(0, options.PrefetchCount, false, linked.Token);
            try
            {
                await channel.BasicConsumeAsync(queue, false, consumer, linked.Token);
                _ready.TrySetResult();
            }
            catch (Exception ex)
            {
                _ready.TrySetException(ex);
                throw;
            }
            await foreach (var envelope in _messages.Reader.ReadAllAsync(linked.Token)) yield return envelope;
        }

        private async Task DeadLetterRaw(byte[] body, MessageHeaders headers, ulong tag, CancellationToken ct)
        {
            headers.DlqReason = "deserialization_failed";
            await _settlement.WaitAsync(ct);
            try
            {
                await publisher.BasicPublishAsync(topic + options.DlqSuffix, "", true,
                    new BasicProperties { Persistent = true, MessageId = headers.MessageId,
                        Headers = headers.ToDictionary(x => x.Key, x => (object?)x.Value) }, body, ct);
                await channel.BasicAckAsync(tag, false, ct);
            }
            finally { _settlement.Release(); }
        }

        public async Task CommitAsync(MessageEnvelope<T> message, CancellationToken ct = default)
        {
            await _settlement.WaitAsync(ct);
            try { await channel.BasicAckAsync(checked((ulong)message.Offset), false, ct); }
            finally { _settlement.Release(); }
        }

        public async Task NackAsync(MessageEnvelope<T> message, CancellationToken ct = default)
        {
            await _settlement.WaitAsync(ct);
            try
            {
                await publisher.BasicPublishAsync(topic + options.DlqSuffix, "", true,
                    new BasicProperties { Persistent = true, MessageId = message.Headers.MessageId,
                        Headers = message.Headers.ToDictionary(x => x.Key, x => (object?)x.Value) },
                    JsonSerializer.SerializeToUtf8Bytes(message.Payload), ct);
                await channel.BasicAckAsync(checked((ulong)message.Offset), false, ct);
            }
            finally { _settlement.Release(); }
        }

        public async ValueTask DisposeAsync()
        {
            await _stop.CancelAsync();
            await channel.DisposeAsync();
            await publisher.DisposeAsync();
            _messages.Writer.TryComplete();
        }
    }
}
