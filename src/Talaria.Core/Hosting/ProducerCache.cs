// SPDX-License-Identifier: Apache-2.0

using System.Collections.Concurrent;
using Talaria.Core.Abstractions;

namespace Talaria.Core.Hosting;

internal sealed class ProducerCache : IAsyncDisposable
{
    private readonly ConcurrentDictionary<(string Topic, Type MessageType), ProducerInvoker> _producers = new();
    private readonly ITransport _transport;

    public ProducerCache(ITransport transport)
    {
        _transport = transport;
    }

    public async Task<ProducerInvoker> GetOrCreateAsync(
        string topic,
        Type messageType,
        CancellationToken ct)
    {
        if (_producers.TryGetValue((topic, messageType), out var existing))
        {
            return existing;
        }

        var method = typeof(ProducerCache)
            .GetMethod(nameof(CreateProducerInvokerAsync), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!
            .MakeGenericMethod(messageType);

        var invoker = await (Task<ProducerInvoker>)method.Invoke(null, [_transport, topic, ct])!;
        return _producers.GetOrAdd((topic, messageType), invoker);
    }

    private static async Task<ProducerInvoker> CreateProducerInvokerAsync<T>(ITransport transport, string topic, CancellationToken ct)
        where T : class
    {
        var producer = await transport.CreateProducerAsync<T>(topic, new ProducerOptions(), ct);
        return new ProducerInvoker(
            async (msg, headers, partitionKey, token) => await producer.ProduceAsync((T)msg, headers, partitionKey, token),
            producer);
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var invoker in _producers.Values)
        {
            await invoker.Producer.DisposeAsync();
        }

        _producers.Clear();
    }
}

internal sealed record ProducerInvoker(
    Func<object, MessageHeaders?, string?, CancellationToken, Task> Produce,
    IAsyncDisposable Producer);
