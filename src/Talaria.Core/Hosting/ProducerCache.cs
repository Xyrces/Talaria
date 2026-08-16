// SPDX-License-Identifier: Apache-2.0

using System.Collections.Concurrent;
using Talaria.Core.Abstractions;

namespace Talaria.Core.Hosting;

internal sealed class ProducerCache : IAsyncDisposable
{
    private readonly ConcurrentDictionary<(string Topic, Type MessageType), Lazy<Task<ProducerInvoker>>> _producers = new();
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
        var key = (Topic: topic, MessageType: messageType);
        var lazy = _producers.GetOrAdd(
            key,
            _ => new Lazy<Task<ProducerInvoker>>(
                () => CreateProducerInvokerAsync(key.Topic, key.MessageType, ct),
                LazyThreadSafetyMode.ExecutionAndPublication));

        try
        {
            return await lazy.Value.ConfigureAwait(false);
        }
        catch
        {
            // Do not cache a failed creation attempt: transient transport errors should be
            // retried on the next call rather than poisoning the cache forever.
            _producers.TryRemove(key, out _);
            throw;
        }
    }

    private async Task<ProducerInvoker> CreateProducerInvokerAsync(string topic, Type messageType, CancellationToken ct)
    {
        var method = typeof(ProducerCache)
            .GetMethod(nameof(CreateProducerInvokerTypedAsync), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!
            .MakeGenericMethod(messageType);

        return await ((Task<ProducerInvoker>)method.Invoke(null, [_transport, topic, ct])!).ConfigureAwait(false);
    }

    private static async Task<ProducerInvoker> CreateProducerInvokerTypedAsync<T>(ITransport transport, string topic, CancellationToken ct)
        where T : class
    {
        var producer = await transport.CreateProducerAsync<T>(topic, new ProducerOptions(), ct).ConfigureAwait(false);
        return new ProducerInvoker(
            async (msg, headers, partitionKey, token) => await producer.ProduceAsync((T)msg, headers, partitionKey, token).ConfigureAwait(false),
            producer);
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var lazy in _producers.Values)
        {
            if (!lazy.IsValueCreated)
            {
                continue;
            }

            try
            {
                var invoker = await lazy.Value.ConfigureAwait(false);
                await invoker.Producer.DisposeAsync().ConfigureAwait(false);
            }
            catch
            {
                // Best effort: a faulted producer does not prevent disposing the rest.
            }
        }

        _producers.Clear();
    }
}

internal sealed record ProducerInvoker(
    Func<object, MessageHeaders?, string?, CancellationToken, Task> Produce,
    IAsyncDisposable Producer);
