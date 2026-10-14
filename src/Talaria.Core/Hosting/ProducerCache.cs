// SPDX-License-Identifier: Apache-2.0

using Talaria.Core.Abstractions;

namespace Talaria.Core.Hosting;

internal sealed class ProducerCache : IAsyncDisposable
{
    private readonly Dictionary<(string Topic, Type MessageType), ProducerInvoker> _producers = new();
    private readonly ITransport _transport;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private bool _disposed;

    public ProducerCache(ITransport transport) => _transport = transport;

    public async Task<ProducerInvoker> GetOrCreateAsync(string topic, Type messageType, CancellationToken ct)
    {
        await _gate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            var key = (topic, messageType);
            if (_producers.TryGetValue(key, out var existing)) return existing;

            var method = typeof(ProducerCache)
                .GetMethod(nameof(CreateProducerInvokerTypedAsync), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!
                .MakeGenericMethod(messageType);
            // Only successful creation is cached. Serializing creation also prevents
            // competing callers from leaking producers or racing with disposal.
            var invoker = await ((Task<ProducerInvoker>)method.Invoke(null, [_transport, topic, ct])!).ConfigureAwait(false);
            _producers.Add(key, invoker);
            return invoker;
        }
        finally { _gate.Release(); }
    }

    private static async Task<ProducerInvoker> CreateProducerInvokerTypedAsync<T>(ITransport transport, string topic, CancellationToken ct)
    {
        var producer = await transport.CreateProducerAsync<T>(topic, new ProducerOptions(), ct).ConfigureAwait(false);
        return new ProducerInvoker(
            async (msg, headers, partitionKey, token) => await producer.ProduceAsync((T)msg, headers, partitionKey, token).ConfigureAwait(false),
            producer);
    }

    public async ValueTask DisposeAsync()
    {
        await _gate.WaitAsync().ConfigureAwait(false);
        try
        {
            if (_disposed) return;
            _disposed = true;
            foreach (var invoker in _producers.Values)
            {
                try { await invoker.Producer.DisposeAsync().ConfigureAwait(false); }
                catch { /* Best effort: dispose the remaining producers too. */ }
            }
            _producers.Clear();
        }
        finally { _gate.Release(); }
    }
}

internal sealed record ProducerInvoker(
    Func<object, MessageHeaders?, string?, CancellationToken, Task> Produce,
    IAsyncDisposable Producer);
