using System.Runtime.CompilerServices;
using Talaria.Core.Abstractions;
using Talaria.Core.Hosting;

namespace Talaria.Core.Tests;

public sealed class ConsumerReaderTests
{
    private sealed class EarlyEndConsumer : IConsumer<string>, IConsumerReadiness
    {
        private readonly TaskCompletionSource _ready = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public Task Ready => _ready.Task;
        public int EnumeratorDisposed;

        public async IAsyncEnumerable<MessageEnvelope<string>> ConsumeAsync([EnumeratorCancellation] CancellationToken ct = default)
        {
            try
            {
                await Task.Yield();
                yield break;
            }
            finally
            {
                Interlocked.Increment(ref EnumeratorDisposed);
            }
        }

        public Task CommitAsync(MessageEnvelope<string> message, CancellationToken ct = default) => Task.CompletedTask;
        public Task NackAsync(MessageEnvelope<string> message, CancellationToken ct = default) => Task.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    [Fact]
    public async Task CleanReaderEndBeforeReadyDoesNotHangAndDisposesEnumeration()
    {
        var consumer = new EarlyEndConsumer();
        using var stop = new CancellationTokenSource();
        var reader = ConsumerReader.ReadAsync(consumer, new TalariaHealth(), "endpoint", stop.Token, stop.Token);
        await using var enumerator = reader.GetAsyncEnumerator();

        Assert.False(await enumerator.MoveNextAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(2)));
        await enumerator.DisposeAsync();
        Assert.Equal(1, consumer.EnumeratorDisposed);
    }
}
