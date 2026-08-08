using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Talaria.Core;
using Talaria.Core.Abstractions;
using Talaria.Core.Hosting;
using Talaria.Core.Registration;
using Talaria.Transports.InMemory;
using Xunit;

namespace Talaria.Specs.Tests;

public class EngineIdempotencyTests
{
    private const string AppName = "idem-test-app";

    private class DummyMessage { }

    private sealed class ThrowingIdempotencyStore : IIdempotencyStore
    {
        public Task<IdempotencyLock?> TryAcquireLockAsync(
            string messageId, string consumerQueue, TimeSpan expiration, CancellationToken ct = default)
            => throw new InvalidOperationException("Simulated idempotency store outage.");

        public Task MarkCompleteAsync(IdempotencyLock @lock, CancellationToken ct = default)
            => throw new InvalidOperationException("Simulated idempotency store outage.");

        public Task ReleaseLockAsync(IdempotencyLock @lock, CancellationToken ct = default)
            => throw new InvalidOperationException("Simulated idempotency store outage.");
    }

    private static TalariaHostedService BuildService(
        InMemoryTransport transport,
        TopicRegistry topicReg,
        IIdempotencyStore store,
        out ServiceProvider services)
    {
        services = new ServiceCollection()
            .AddSingleton(store)
            .BuildServiceProvider();

        return new TalariaHostedService(
            transport,
            topicReg,
            Options.Create(new TalariaOptions { ApplicationName = AppName }),
            NullLogger<TalariaHostedService>.Instance,
            services);
    }

    [Fact]
    public async Task Duplicate_MessageId_Is_Processed_Exactly_Once()
    {
        var transport = new InMemoryTransport();
        var store = new InMemoryIdempotencyStore();
        var handlerCalls = 0;

        var topicReg = new TopicRegistry();
        topicReg.Add(new TopicRegistration
        {
            TopicName = "dup-topic",
            MessageType = typeof(DummyMessage),
            Handler = (msg, headers, ct) =>
            {
                Interlocked.Increment(ref handlerCalls);
                return Task.CompletedTask;
            }
        });

        var hostedService = BuildService(transport, topicReg, store, out var services);
        using var cts = new CancellationTokenSource();
        await hostedService.StartAsync(cts.Token);

        var producer = await transport.CreateProducerAsync<DummyMessage>("dup-topic", new ProducerOptions());
        var headers = new MessageHeaders { MessageId = "dup-1" };
        await producer.ProduceAsync(new DummyMessage(), headers);
        await producer.ProduceAsync(new DummyMessage(), new MessageHeaders { MessageId = "dup-1" });

        // Wait for the first delivery to be handled...
        await WaitUntilAsync(() => Volatile.Read(ref handlerCalls) == 1);

        // ...then give the second delivery a window to be consumed and skipped as a duplicate.
        // The handler must never run a second time.
        var stableDeadline = DateTime.UtcNow.AddMilliseconds(500);
        while (DateTime.UtcNow < stableDeadline)
        {
            await Task.Delay(50);
            Assert.Equal(1, Volatile.Read(ref handlerCalls));
        }

        Assert.Equal(1, store.Count); // one COMPLETED marker

        await hostedService.StopAsync(cts.Token);
    }

    [Fact]
    public async Task Handler_Failure_Releases_Lock_And_Routes_To_Dlq_So_Same_MessageId_Can_Be_Retried()
    {
        var transport = new InMemoryTransport();
        var store = new InMemoryIdempotencyStore();
        var handlerCalls = 0;

        var topicReg = new TopicRegistry();
        topicReg.Add(new TopicRegistration
        {
            TopicName = "fail-topic",
            MessageType = typeof(DummyMessage),
            Handler = (msg, headers, ct) =>
            {
                // First invocation throws; the retry (same MessageId) succeeds.
                if (Interlocked.Increment(ref handlerCalls) == 1)
                {
                    throw new InvalidOperationException("boom");
                }

                return Task.CompletedTask;
            }
        });

        var hostedService = BuildService(transport, topicReg, store, out var services);
        using var cts = new CancellationTokenSource();
        await hostedService.StartAsync(cts.Token);

        var producer = await transport.CreateProducerAsync<DummyMessage>("fail-topic", new ProducerOptions());
        await producer.ProduceAsync(new DummyMessage(), new MessageHeaders { MessageId = "retry-1" });

        // First delivery fails → DLQ'd, lock released.
        var dlq = await ReadUntilAsync<DummyMessage>(transport, "fail-topic.dlq", 1);
        Assert.Single(dlq);

        // Retry the same MessageId: the released lock must be re-acquirable.
        await producer.ProduceAsync(new DummyMessage(), new MessageHeaders { MessageId = "retry-1" });

        await WaitUntilAsync(() => Volatile.Read(ref handlerCalls) == 2);
        Assert.Equal(2, Volatile.Read(ref handlerCalls));

        await hostedService.StopAsync(cts.Token);
    }

    [Fact]
    public async Task Successful_Processing_Marks_Lock_Completed()
    {
        var transport = new InMemoryTransport();
        var store = new InMemoryIdempotencyStore();
        var handlerCalls = 0;

        var topicReg = new TopicRegistry();
        topicReg.Add(new TopicRegistration
        {
            TopicName = "complete-topic",
            MessageType = typeof(DummyMessage),
            Handler = (msg, headers, ct) =>
            {
                Interlocked.Increment(ref handlerCalls);
                return Task.CompletedTask;
            }
        });

        var hostedService = BuildService(transport, topicReg, store, out var services);
        using var cts = new CancellationTokenSource();
        await hostedService.StartAsync(cts.Token);

        var producer = await transport.CreateProducerAsync<DummyMessage>("complete-topic", new ProducerOptions());
        await producer.ProduceAsync(new DummyMessage(), new MessageHeaders { MessageId = "done-1" });

        // Wait until the handler ran — probing the store before that would acquire the
        // lock ourselves and suppress the engine's own delivery.
        await WaitUntilAsync(() => Volatile.Read(ref handlerCalls) == 1);

        // The engine marks the key COMPLETED after the handler returns; poll until the
        // marker is visible. A COMPLETED key must reject any further acquisition.
        var consumerGroup = $"{AppName}.complete-topic";
        await WaitUntilAsync(async () =>
            await store.TryAcquireLockAsync("done-1", consumerGroup, TimeSpan.FromMinutes(1)) is null);

        await hostedService.StopAsync(cts.Token);
    }

    [Fact]
    public async Task Idempotency_Store_Failure_Leaves_Message_Undeadlettered_And_Handler_Not_Invoked()
    {
        var transport = new InMemoryTransport();
        var handlerCalls = 0;

        var topicReg = new TopicRegistry();
        topicReg.Add(new TopicRegistration
        {
            TopicName = "outage-topic",
            MessageType = typeof(DummyMessage),
            Handler = (msg, headers, ct) =>
            {
                Interlocked.Increment(ref handlerCalls);
                return Task.CompletedTask;
            }
        });

        var hostedService = BuildService(transport, topicReg, new ThrowingIdempotencyStore(), out var services);
        using var cts = new CancellationTokenSource();
        await hostedService.StartAsync(cts.Token);

        var producer = await transport.CreateProducerAsync<DummyMessage>("outage-topic", new ProducerOptions());
        await producer.ProduceAsync(new DummyMessage(), new MessageHeaders { MessageId = "outage-1" });

        // An infrastructure (store) failure must never dead-letter a healthy message.
        // The supervised loop may fault and restart; give it a window and assert the
        // DLQ stays empty and the handler never ran.
        var deadline = DateTime.UtcNow.AddSeconds(3);
        while (DateTime.UtcNow < deadline)
        {
            await Task.Delay(100);
            var dlq = await transport.ReadAllFromTopicAsync<DummyMessage>("outage-topic.dlq");
            Assert.Empty(dlq);
            Assert.Equal(0, Volatile.Read(ref handlerCalls));
        }

        await hostedService.StopAsync(cts.Token);
    }

    [Fact]
    public async Task InMemoryIdempotencyStore_Concurrent_Acquires_Grant_Exactly_One_Lock()
    {
        var store = new InMemoryIdempotencyStore();

        var results = await Task.WhenAll(Enumerable.Range(0, 100)
            .Select(_ => Task.Run(() => store.TryAcquireLockAsync("race-1", "race-group", TimeSpan.FromMinutes(1)))));

        var granted = results.Where(l => l is not null).ToList();
        Assert.Single(granted);
        Assert.Equal(1, store.Count);
    }

    [Fact]
    public async Task InMemoryIdempotencyStore_Release_With_Stale_Fencing_Token_Does_Not_Remove_Lock()
    {
        var store = new InMemoryIdempotencyStore();

        var lockA = await store.TryAcquireLockAsync("fence-1", "fence-group", TimeSpan.FromMinutes(1));
        Assert.NotNull(lockA);

        // A stale holder (old fencing token) must not be able to remove A's lock.
        var stale = new IdempotencyLock("fence-1", "fence-group", "stale-token");
        await store.ReleaseLockAsync(stale);

        // A's lock is still active: re-acquisition must be denied.
        Assert.Null(await store.TryAcquireLockAsync("fence-1", "fence-group", TimeSpan.FromMinutes(1)));

        // The real owner can release; afterwards the key is re-acquirable.
        await store.ReleaseLockAsync(lockA);
        var lockB = await store.TryAcquireLockAsync("fence-1", "fence-group", TimeSpan.FromMinutes(1));
        Assert.NotNull(lockB);

        await store.ReleaseLockAsync(lockB);
    }

    private static async Task<List<MessageEnvelope<T>>> ReadUntilAsync<T>(
        InMemoryTransport transport, string topic, int expectedCount)
    {
        var deadline = DateTime.UtcNow.AddSeconds(10);
        List<MessageEnvelope<T>> messages;
        do
        {
            messages = await transport.ReadAllFromTopicAsync<T>(topic);
            if (messages.Count >= expectedCount)
            {
                break;
            }

            await Task.Delay(50);
        }
        while (DateTime.UtcNow < deadline);

        return messages;
    }

    private static Task WaitUntilAsync(Func<bool> condition)
        => WaitUntilAsync(() => Task.FromResult(condition()));

    private static async Task WaitUntilAsync(Func<Task<bool>> condition)
    {
        var deadline = DateTime.UtcNow.AddSeconds(10);
        while (DateTime.UtcNow < deadline)
        {
            if (await condition())
            {
                return;
            }

            await Task.Delay(25);
        }

        Assert.True(await condition(), "Condition was not met within the timeout.");
    }
}
