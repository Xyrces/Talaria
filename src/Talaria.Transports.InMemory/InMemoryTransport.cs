using System.Collections.Concurrent;
using System.Text.Json;
using System.Threading.Channels;
using Talaria.Core.Abstractions;

namespace Talaria.Transports.InMemory;

/// <summary>
/// In-memory transport backed by System.Threading.Channels.
/// Provides fully deterministic, container-free messaging for testing and development.
/// </summary>
public sealed class InMemoryTransport : ITransport
{
    public string Name => "InMemory";

    private readonly ConcurrentDictionary<string, object> _channels = new();
    private readonly InMemoryTransportOptions _options;
    internal InMemoryTransportOptions Options => _options;

    public InMemoryTransport() : this(new InMemoryTransportOptions()) { }

    public InMemoryTransport(InMemoryTransportOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        if (options.ChannelCapacity <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options), $"{nameof(InMemoryTransportOptions.ChannelCapacity)} must be greater than zero.");
        }

        if (options.SimulatedLatency < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options), $"{nameof(InMemoryTransportOptions.SimulatedLatency)} must not be negative.");
        }

        _options = options;
    }

    /// <summary>
    /// Gets or creates a channel for the given topic.
    /// </summary>
    internal Channel<InMemoryMessage> GetOrCreateChannel(string topic)
    {
        return (Channel<InMemoryMessage>)_channels.GetOrAdd(topic, _ =>
            Channel.CreateBounded<InMemoryMessage>(
                new BoundedChannelOptions(_options.ChannelCapacity)
                {
                    FullMode = BoundedChannelFullMode.Wait,
                    SingleReader = false,
                    SingleWriter = false,
                }));
    }

    public Task<IConsumer<T>> CreateConsumerAsync<T>(
        string topic,
        ConsumerOptions options,
        CancellationToken ct = default)
    {
        var channel = GetOrCreateChannel(topic);
        var dlqChannel = GetOrCreateChannel(topic + _options.DlqSuffix);
        var appDlqChannel = GetOrCreateChannel("__app.dlq");
        IConsumer<T> consumer = new InMemoryConsumer<T>(topic, channel, dlqChannel, appDlqChannel, _options);
        return Task.FromResult(consumer);
    }

    public Task<IProducer<T>> CreateProducerAsync<T>(
        string topic,
        ProducerOptions options,
        CancellationToken ct = default)
    {
        var channel = GetOrCreateChannel(topic);
        IProducer<T> producer = new InMemoryProducer<T>(channel, topic, _options);
        return Task.FromResult(producer);
    }

    public Task<ITransactionalSession> BeginTransactionAsync(
        string? consumerGroup = null,
        TransactionOffsetSource? offsetSource = null,
        CancellationToken ct = default)
    {
        // Offsets are not transactional in the in-memory transport; the session
        // buffers produces so commit/abort visibility is testable.
        return Task.FromResult<ITransactionalSession>(new InMemoryTransactionalSession(this));
    }

    /// <summary>
    /// Reads all messages currently pending on a topic. Useful for test assertions.
    /// </summary>
    public async Task<List<MessageEnvelope<T>>> ReadAllFromTopicAsync<T>(
        string topic,
        CancellationToken ct = default)
    {
        var results = new List<MessageEnvelope<T>>();
        var channel = GetOrCreateChannel(topic);

        while (channel.Reader.TryRead(out var raw))
        {
            var payload = JsonSerializer.Deserialize<T>(raw.PayloadJson)!;
            results.Add(new MessageEnvelope<T>
            {
                Payload = payload,
                Headers = raw.Headers,
                SourceTopic = topic,
                Offset = raw.Offset,
                Timestamp = raw.Timestamp,
            });
        }

        return results;
    }
}
