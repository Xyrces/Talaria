// SPDX-License-Identifier: Apache-2.0
using System.Runtime.CompilerServices;
using Talaria.Core.Abstractions;

namespace Talaria.Core.Hosting;

internal static class ConsumerReader
{
    // Intake cancellation is observed between deliveries. While a handler owns a
    // yielded envelope, the transport keeps polling/renewing so it can still settle.
    internal static async IAsyncEnumerable<MessageEnvelope<T>> ReadAsync<T>(IConsumer<T> consumer,
        TalariaHealth? health, string endpoint, CancellationToken intake, [EnumeratorCancellation] CancellationToken processing)
    {
        using var reads = CancellationTokenSource.CreateLinkedTokenSource(processing);
        await using var enumerator = consumer.ConsumeAsync(reads.Token).GetAsyncEnumerator();
        Task<bool>? pending = null;
        try
        {
            var ready = consumer is IConsumerReadiness readiness ? readiness.Ready : Task.CompletedTask;
            var first = true;
            while (!intake.IsCancellationRequested)
            {
                var available = false;
                pending = enumerator.MoveNextAsync().AsTask();
                try
                {
                    if (first)
                    {
                        var firstSignal = await Task.WhenAny(ready, pending).WaitAsync(intake);
                        if (firstSignal == pending)
                        {
                            available = await pending;
                            pending = null;
                            if (!available) break;
                        }
                        else if (pending.IsFaulted || pending.IsCanceled)
                        {
                            await pending;
                        }
                        await ready.WaitAsync(intake);
                        health?.SetEndpoint(endpoint, true);
                        first = false;
                    }
                    if (pending is not null)
                        available = await pending.WaitAsync(intake);
                }
                catch (OperationCanceledException) when (intake.IsCancellationRequested) { break; }
                pending = null;
                if (!available) break;
                yield return enumerator.Current;
            }
        }
        finally
        {
            await reads.CancelAsync();
            if (pending is not null)
            {
                try { await pending; }
                catch (Exception) { /* The original read failure or shutdown is already observed. */ }
            }
            health?.SetEndpoint(endpoint, false);
        }
    }
}
