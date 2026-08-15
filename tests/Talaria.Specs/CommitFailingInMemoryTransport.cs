// SPDX-License-Identifier: Apache-2.0

using Talaria.Core.Abstractions;
using Talaria.Transports.InMemory;

namespace Talaria.Specs.Tests;

/// <summary>
/// Test helper that wraps <see cref="InMemoryTransport"/> and lets individual message ids
/// fail a configurable number of <see cref="IConsumer{T}.CommitAsync"/> calls before
/// succeeding. This makes the engine's commit-before-release paths observable and
/// redelivery deterministic in tests.
/// </summary>
internal sealed class CommitFailingInMemoryTransport : ITransport
{
    private readonly InMemoryTransport _inner;
    private readonly Dictionary<string, int> _remainingFailures = new();
    private readonly object _gate = new();

    public CommitFailingInMemoryTransport(InMemoryTransport inner)
    {
        _inner = inner;
    }

    public string Name => _inner.Name;

    public InMemoryTransport InnerTransport => _inner;

    /// <summary>
    /// Configures <paramref name="messageId"/> to throw on its next
    /// <paramref name="count"/> CommitAsync attempts.
    /// </summary>
    public void SetCommitFailures(string messageId, int count)
    {
        lock (_gate)
        {
            _remainingFailures[messageId] = count;
        }
    }

    public Task<IConsumer<T>> CreateConsumerAsync<T>(
        string topic,
        ConsumerOptions options,
        CancellationToken ct = default)
    {
        var innerConsumerTask = _inner.CreateConsumerAsync<T>(topic, options, ct);
        return WrapConsumerAsync(innerConsumerTask, ct);
    }

    private async Task<IConsumer<T>> WrapConsumerAsync<T>(Task<IConsumer<T>> innerTask, CancellationToken ct)
    {
        var inner = await innerTask;
        return new CommitFailingConsumer<T>(this, inner);
    }

    public Task<IProducer<T>> CreateProducerAsync<T>(
        string topic,
        ProducerOptions options,
        CancellationToken ct = default)
    {
        return _inner.CreateProducerAsync<T>(topic, options, ct);
    }

    public Task<ITransactionalSession> BeginTransactionAsync(
        string? consumerGroup = null,
        TransactionOffsetSource? offsetSource = null,
        CancellationToken ct = default)
    {
        return _inner.BeginTransactionAsync(consumerGroup, offsetSource, ct);
    }

    internal Task CommitAsync<T>(IConsumer<T> innerConsumer, MessageEnvelope<T> message, CancellationToken ct)
    {
        var messageId = message.Headers.MessageId;
        if (!string.IsNullOrEmpty(messageId))
        {
            lock (_gate)
            {
                if (_remainingFailures.TryGetValue(messageId, out var remaining) && remaining > 0)
                {
                    _remainingFailures[messageId] = remaining - 1;
                    throw new InvalidOperationException($"Simulated commit failure for message {messageId}.");
                }
            }
        }

        return innerConsumer.CommitAsync(message, ct);
    }

    private sealed class CommitFailingConsumer<T> : IConsumer<T>
    {
        private readonly CommitFailingInMemoryTransport _transport;
        private readonly IConsumer<T> _inner;

        public CommitFailingConsumer(CommitFailingInMemoryTransport transport, IConsumer<T> inner)
        {
            _transport = transport;
            _inner = inner;
        }

        public IAsyncEnumerable<MessageEnvelope<T>> ConsumeAsync(CancellationToken ct = default)
            => _inner.ConsumeAsync(ct);

        public Task CommitAsync(MessageEnvelope<T> message, CancellationToken ct = default)
            => _transport.CommitAsync(_inner, message, ct);

        public Task NackAsync(MessageEnvelope<T> message, CancellationToken ct = default)
            => _inner.NackAsync(message, ct);

        public ValueTask DisposeAsync()
            => _inner.DisposeAsync();
    }
}
