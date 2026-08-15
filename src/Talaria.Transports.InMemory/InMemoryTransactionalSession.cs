// SPDX-License-Identifier: Apache-2.0

using Talaria.Core.Abstractions;

namespace Talaria.Transports.InMemory;

/// <summary>
/// Transactional session for the in-memory transport. Produces are buffered and only
/// become visible to consumers on <see cref="CommitAsync"/>; <see cref="AbortAsync"/>
/// (or disposing an open session) discards them — making the abort path observable in tests.
/// Offsets are assigned by the topic bus in commit order.
/// </summary>
internal sealed class InMemoryTransactionalSession : ITransactionalSession
{
    private readonly List<(InMemoryTransport.TopicBus Bus, InMemoryMessage Message)> _buffer = new();
    private readonly object _gate = new();
    private readonly InMemoryTransport _transport;
    private bool _completed;

    public InMemoryTransactionalSession(InMemoryTransport transport)
    {
        _transport = transport;
    }

    public Task<IProducer<T>> GetProducerAsync<T>(string topic, CancellationToken ct = default)
    {
        ThrowIfCompleted();
        return Task.FromResult<IProducer<T>>(
            new InMemoryBufferedProducer<T>(this, _transport.GetOrCreateBus(topic), topic));
    }

    public async Task CommitAsync(CancellationToken ct = default)
    {
        ThrowIfCompleted();

        List<(InMemoryTransport.TopicBus Bus, InMemoryMessage Message)> pending;
        lock (_gate)
        {
            pending = new List<(InMemoryTransport.TopicBus, InMemoryMessage)>(_buffer);
        }

        foreach (var (bus, message) in pending)
        {
            await bus.PublishAsync(message, ct);
        }

        lock (_gate)
        {
            _buffer.Clear();
            _completed = true;
        }
    }

    public Task AbortAsync(CancellationToken ct = default)
    {
        ThrowIfCompleted();
        lock (_gate)
        {
            _buffer.Clear();
            _completed = true;
        }

        return Task.CompletedTask;
    }

    public ValueTask DisposeAsync()
    {
        lock (_gate)
        {
            _buffer.Clear();
            _completed = true;
        }

        return ValueTask.CompletedTask;
    }

    internal void Buffer(InMemoryTransport.TopicBus bus, InMemoryMessage message)
    {
        lock (_gate)
        {
            ThrowIfCompleted();
            _buffer.Add((bus, message));
        }
    }

    private void ThrowIfCompleted()
    {
        if (_completed)
        {
            throw new InvalidOperationException("The transaction has already been committed or aborted.");
        }
    }
}

/// <summary>
/// Producer whose writes are buffered inside an <see cref="InMemoryTransactionalSession"/>
/// until the session commits.
/// </summary>
internal sealed class InMemoryBufferedProducer<T> : IProducer<T>
{
    private readonly InMemoryTransactionalSession _session;
    private readonly InMemoryTransport.TopicBus _bus;
    private readonly string _topic;

    public InMemoryBufferedProducer(
        InMemoryTransactionalSession session,
        InMemoryTransport.TopicBus bus,
        string topic)
    {
        _session = session;
        _bus = bus;
        _topic = topic;
    }

    public Task ProduceAsync(
        T message,
        MessageHeaders? headers = null,
        string? partitionKey = null,
        CancellationToken ct = default)
    {
        var msg = InMemoryProducer<T>.CreateMessage(message, headers);

        if (System.Diagnostics.Activity.Current != null)
        {
            System.Diagnostics.Activity.Current.SetTag("messaging.destination.name", _topic);
            System.Diagnostics.Activity.Current.SetTag("messaging.system", "talaria");
        }

        _session.Buffer(_bus, msg);
        return Task.CompletedTask;
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
