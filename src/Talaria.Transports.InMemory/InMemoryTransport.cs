// SPDX-License-Identifier: Apache-2.0

using System.Collections.Concurrent;
using System.Text.Json;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Talaria.Core.Abstractions;

namespace Talaria.Transports.InMemory;

/// <summary>
/// In-memory transport backed by System.Threading.Channels. A fully functional
/// lightweight transport for single-process deployments, prototyping, and tests —
/// no backing message bus required.
/// <para>
/// Kafka-parity semantics: every (topic, consumer-group) pair gets its own channel, so
/// consumer groups are independent (a late-joining group replays the retained backlog);
/// offsets are assigned per topic by the transport; malformed payloads are routed to the
/// DLQ instead of killing the consumer loop; unsettled (uncommitted) messages are
/// requeued when a consumer is disposed, mirroring Kafka redelivery. DLQ topics are
/// unbounded.
/// </para>
/// <para>
/// Remaining divergences from Kafka: the retained backlog is capped at ChannelCapacity
/// (oldest dropped, and requeue-on-dispose drops overflow on a full channel), there is
/// no partition key ordering, offsets do not join transactional sessions (produces
/// are buffered until commit; the offset commit itself is per-consumer), and the
/// partition key is carried on messages/envelopes but is not used for routing or
/// ordering.
/// </para>
/// </summary>
public sealed class InMemoryTransport : ITransport
{
    public string Name => "InMemory";

    private readonly ConcurrentDictionary<string, TopicBus> _topicBuses = new();
    private readonly ConcurrentDictionary<string, TopicBus> _dlqBuses = new();
    private readonly InMemoryTransportOptions _options;
    private readonly bool _includeDlqExceptionDetails;
    private readonly ILogger? _logger;
    internal InMemoryTransportOptions Options => _options;

    public InMemoryTransport() : this(new InMemoryTransportOptions()) { }

    public InMemoryTransport(InMemoryTransportOptions options, bool includeDlqExceptionDetails = false, ILogger? logger = null)
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
        _includeDlqExceptionDetails = includeDlqExceptionDetails;
        _logger = logger;
    }

    /// <summary>
    /// Gets or creates the bus for a regular topic (bounded by ChannelCapacity).
    /// </summary>
    internal TopicBus GetOrCreateBus(string topic)
        => _topicBuses.GetOrAdd(topic, _ => new TopicBus(_options.ChannelCapacity, unbounded: false));

    /// <summary>
    /// Gets or creates the bus for a DLQ topic (unbounded — dead letters must never be dropped
    /// or block the consumer loop).
    /// </summary>
    internal TopicBus GetOrCreateDlqBus(string dlqTopic)
        => _dlqBuses.GetOrAdd(dlqTopic, _ => new TopicBus(unbounded: true));

    public Task<IConsumer<T>> CreateConsumerAsync<T>(
        string topic,
        ConsumerOptions options,
        CancellationToken ct = default)
    {
        var bus = GetOrCreateBus(topic);
        var groupChannel = bus.GetOrCreateGroupChannel(options.ConsumerGroup ?? "default");
        var dlqBus = GetOrCreateDlqBus(topic + _options.DlqSuffix);
        var appDlqBus = GetOrCreateDlqBus("__app.dlq");
        IConsumer<T> consumer = new InMemoryConsumer<T>(
            topic, groupChannel, dlqBus, appDlqBus, _options, _includeDlqExceptionDetails, _logger);
        return Task.FromResult(consumer);
    }

    public Task<IProducer<T>> CreateProducerAsync<T>(
        string topic,
        ProducerOptions options,
        CancellationToken ct = default)
    {
        IProducer<T> producer = new InMemoryProducer<T>(GetOrCreateBus(topic), topic, _options);
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
    /// Reads all messages currently pending on a topic from a dedicated reader group.
    /// Useful for test assertions. The first call replays the retained backlog.
    /// </summary>
    public async Task<List<MessageEnvelope<T>>> ReadAllFromTopicAsync<T>(
        string topic,
        CancellationToken ct = default)
    {
        var results = new List<MessageEnvelope<T>>();
        // DLQ topics live in the DLQ bus registry; everything else in the topic registry.
        var bus = _dlqBuses.TryGetValue(topic, out var dlqBus) ? dlqBus : GetOrCreateBus(topic);
        var channel = bus.GetOrCreateGroupChannel("__talaria.test-reader");

        while (channel.Reader.TryRead(out var raw))
        {
            var payload = JsonSerializer.Deserialize<T>(raw.PayloadJson)!;
            results.Add(new MessageEnvelope<T>
            {
                Payload = payload,
                Headers = raw.Headers,
                SourceTopic = topic,
                PartitionKey = raw.PartitionKey,
                Offset = raw.Offset,
                Timestamp = raw.Timestamp,
            });
        }

        return results;
    }

    /// <summary>
    /// A topic's message bus: retains a capped backlog and fans each published message
    /// out to every consumer-group channel (Kafka group semantics). Assigns per-topic offsets.
    /// </summary>
    internal sealed class TopicBus
    {
        private readonly object _gate = new();
        private readonly List<InMemoryMessage> _backlog = new();
        private readonly Dictionary<string, Channel<InMemoryMessage>> _groups = new();
        private readonly int _backlogCapacity;
        private readonly bool _unbounded;
        private long _offset;

        public TopicBus(int backlogCapacity, bool unbounded)
        {
            _backlogCapacity = backlogCapacity;
            _unbounded = unbounded;
        }

        /// <summary>
        /// Creates an unbounded DLQ bus. Dead letters must never be dropped, so capacity
        /// is irrelevant and the retained backlog is not capped.
        /// </summary>
        public TopicBus(bool unbounded)
        {
            _unbounded = unbounded;
            _backlogCapacity = int.MaxValue;
        }

        /// <summary>
        /// Returns the channel for a consumer group, creating it (and replaying the retained
        /// backlog) on first use.
        /// </summary>
        public Channel<InMemoryMessage> GetOrCreateGroupChannel(string group)
        {
            lock (_gate)
            {
                if (_groups.TryGetValue(group, out var existing))
                {
                    return existing;
                }

                var channel = _unbounded
                    ? Channel.CreateUnbounded<InMemoryMessage>(new UnboundedChannelOptions { SingleReader = false, SingleWriter = false })
                    : Channel.CreateBounded<InMemoryMessage>(new BoundedChannelOptions(_backlogCapacity)
                    {
                        FullMode = BoundedChannelFullMode.Wait,
                        SingleReader = false,
                        SingleWriter = false,
                    });

                foreach (var message in _backlog)
                {
                    channel.Writer.TryWrite(CloneForGroup(message));
                }

                _groups[group] = channel;
                return channel;
            }
        }

        /// <summary>
        /// Assigns the next offset, retains the message, and writes a per-group clone to
        /// every subscribed group channel.
        /// </summary>
        public async Task PublishAsync(InMemoryMessage message, CancellationToken ct)
        {
            List<Channel<InMemoryMessage>> targets;
            lock (_gate)
            {
                message.Offset = Interlocked.Increment(ref _offset);
                _backlog.Add(message);
                if (_backlog.Count > _backlogCapacity)
                {
                    _backlog.RemoveAt(0);
                }

                targets = _groups.Values.ToList();
            }

            foreach (var channel in targets)
            {
                await channel.Writer.WriteAsync(CloneForGroup(message), ct);
            }
        }

        // Each group gets its own headers copy so engine mutations (DLQ reason, hop count)
        // on one group's envelope never leak into another group's view of the same message.
        private static InMemoryMessage CloneForGroup(InMemoryMessage message)
            => message with { Headers = new MessageHeaders(message.Headers) };
    }
}
