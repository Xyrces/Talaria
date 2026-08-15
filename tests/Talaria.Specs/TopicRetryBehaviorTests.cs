using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Talaria.Core;
using Talaria.Core.Abstractions;
using Talaria.Core.Registration;
using Talaria.Transports.InMemory;
using Xunit;

namespace Talaria.Specs.Tests;

public class TopicRetryBehaviorTests
{
    private class RetryMessage
    {
        public string Id { get; set; } = "";
    }

    [Fact]
    public async Task FailTwiceThenSucceed_HandlerRunsThreeTimes_AndDLQIsEmpty()
    {
        var transport = new InMemoryTransport();
        var attempts = 0;
        var received = new List<string>();

        var builder = Host.CreateDefaultBuilder();
        builder.ConfigureServices(services =>
        {
            services.AddTalaria(opts =>
            {
                opts.ApplicationName = "test-app";
                opts.MinRetryDelay = TimeSpan.FromMilliseconds(50);
            })
            .UseInMemoryTransport(transport)
            .UseInMemoryDeferralStore();
        });

        var host = builder.Build();

        host.Services.MapTopic<RetryMessage>("retry.topic",
            (msg, ct) =>
            {
                var attempt = Interlocked.Increment(ref attempts);
                received.Add(msg.Id);
                if (attempt < 3)
                {
                    throw new InvalidOperationException("boom");
                }

                return Task.CompletedTask;
            },
            new RetryPolicy
            {
                MaxRetryAttempts = 3,
                RetryInterval = TimeSpan.FromMilliseconds(50),
                BackoffType = RetryBackoffType.Fixed,
            });

        // Use envelope-aware registration for header assertions while reusing the same topic
        // would conflict, so we inspect the deferred copies directly via a capturing store.
        var producer = await transport.CreateProducerAsync<RetryMessage>("retry.topic", new ProducerOptions());
        await producer.ProduceAsync(new RetryMessage { Id = "MSG-RETRY" }, new MessageHeaders { MessageId = "root-1" });

        await host.StartAsync();

        var succeeded = await PollUntilAsync(
            () => Task.FromResult(Volatile.Read(ref attempts) >= 3), TimeSpan.FromSeconds(10));
        Assert.True(succeeded, $"Handler did not run 3 times (ran {Volatile.Read(ref attempts)}).");

        // Stability window to ensure nothing DLQ'd.
        await Task.Delay(500);

        Assert.Equal(3, Volatile.Read(ref attempts));
        Assert.Equal(["MSG-RETRY", "MSG-RETRY", "MSG-RETRY"], received);
        Assert.Empty(await transport.ReadAllFromTopicAsync<RetryMessage>("retry.topic.dlq"));

        await host.StopAsync();
        host.Dispose();
    }

    [Fact]
    public async Task RetryCopy_MintsStableRootMessageId_AcrossAttempts()
    {
        var transport = new InMemoryTransport();
        var innerStore = new InMemoryDeferralStore();
        var captured = new List<DeferredMessage>();
        var capturingStore = new CapturingDeferralStore(innerStore, msg => captured.Add(msg));
        var attempts = 0;

        var builder = Host.CreateDefaultBuilder();
        builder.ConfigureServices(services =>
        {
            services.AddTalaria(opts =>
            {
                opts.ApplicationName = "test-app";
                opts.MinRetryDelay = TimeSpan.FromMilliseconds(50);
            })
            .UseInMemoryTransport(transport)
            .UseIdempotencyStore<InMemoryIdempotencyStore>()
            .Services.AddSingleton<IDeferralStore>(capturingStore);
        });

        var host = builder.Build();

        host.Services.MapTopic<RetryMessage>("retry.topic",
            (msg, ct) =>
            {
                if (Interlocked.Increment(ref attempts) < 3)
                {
                    throw new InvalidOperationException("boom");
                }

                return Task.CompletedTask;
            },
            new RetryPolicy
            {
                MaxRetryAttempts = 3,
                RetryInterval = TimeSpan.FromMilliseconds(50),
                BackoffType = RetryBackoffType.Fixed,
            });

        var producer = await transport.CreateProducerAsync<RetryMessage>("retry.topic", new ProducerOptions());
        await producer.ProduceAsync(new RetryMessage { Id = "MSG-ROOT" }, new MessageHeaders { MessageId = "root-1" });

        await host.StartAsync();

        var succeeded = await PollUntilAsync(
            () => Task.FromResult(Volatile.Read(ref attempts) >= 3), TimeSpan.FromSeconds(10));
        Assert.True(succeeded, "Handler did not succeed within timeout.");

        Assert.Equal(2, captured.Count);
        Assert.Equal("root-1:retry:1", captured[0].Headers.MessageId);
        Assert.Equal("root-1", captured[0].Headers.RetryRootMessageId);
        Assert.Equal(1, captured[0].Headers.RetryAttempt);
        Assert.Equal("root-1:retry:2", captured[1].Headers.MessageId);
        Assert.Equal("root-1", captured[1].Headers.RetryRootMessageId);
        Assert.Equal(2, captured[1].Headers.RetryAttempt);

        await host.StopAsync();
        host.Dispose();
    }

    [Fact]
    public async Task Exhaustion_RoutesToDLQ_WithRetriesExhaustedReason()
    {
        var transport = new InMemoryTransport();
        var attempts = 0;

        var builder = Host.CreateDefaultBuilder();
        builder.ConfigureServices(services =>
        {
            services.AddTalaria(opts =>
            {
                opts.ApplicationName = "test-app";
                opts.MinRetryDelay = TimeSpan.FromMilliseconds(50);
            })
            .UseInMemoryTransport(transport)
            .UseInMemoryDeferralStore();
        });

        var host = builder.Build();

        host.Services.MapTopic<RetryMessage>("retry.topic",
            (msg, ct) =>
            {
                Interlocked.Increment(ref attempts);
                throw new InvalidOperationException("boom");
            },
            new RetryPolicy
            {
                MaxRetryAttempts = 1,
                RetryInterval = TimeSpan.FromMilliseconds(50),
                BackoffType = RetryBackoffType.Fixed,
            });

        var producer = await transport.CreateProducerAsync<RetryMessage>("retry.topic", new ProducerOptions());
        await producer.ProduceAsync(new RetryMessage { Id = "MSG-EXHAUST" }, new MessageHeaders { MessageId = "exhaust-1" });

        await host.StartAsync();

        var dlq = await ReadUntilAsync<RetryMessage>(transport, "retry.topic.dlq", 1);

        Assert.Single(dlq);
        Assert.Equal("retries_exhausted", dlq[0].Headers.DlqReason);
        Assert.Equal(1, dlq[0].Headers.DlqAttempts);

        await host.StopAsync();
        host.Dispose();
    }

    [Fact]
    public async Task RetryEnabledWithoutDeferralStore_RoutesToDLQ_AsRetryUnavailable()
    {
        var transport = new InMemoryTransport();

        var builder = Host.CreateDefaultBuilder();
        builder.ConfigureServices(services =>
        {
            services.AddTalaria(opts => opts.ApplicationName = "test-app")
                    .UseInMemoryTransport(transport);
            // Deliberately no deferral store.
        });

        var host = builder.Build();

        host.Services.MapTopic<RetryMessage>("retry.topic",
            (msg, ct) => throw new InvalidOperationException("boom"),
            new RetryPolicy
            {
                MaxRetryAttempts = 3,
                RetryInterval = TimeSpan.FromSeconds(1),
                BackoffType = RetryBackoffType.Fixed,
            });

        var producer = await transport.CreateProducerAsync<RetryMessage>("retry.topic", new ProducerOptions());
        await producer.ProduceAsync(new RetryMessage { Id = "MSG-UNAVAIL" }, new MessageHeaders { MessageId = "unavail-1" });

        await host.StartAsync();

        var dlq = await ReadUntilAsync<RetryMessage>(transport, "retry.topic.dlq", 1);

        Assert.Single(dlq);
        Assert.Equal("retry_unavailable", dlq[0].Headers.DlqReason);

        await host.StopAsync();
        host.Dispose();
    }

    [Fact]
    public async Task DuplicateRetryCopy_SuppressedByIdempotency()
    {
        var transport = new InMemoryTransport();
        var idempotencyStore = new InMemoryIdempotencyStore();
        var attempts = 0;

        var builder = Host.CreateDefaultBuilder();
        builder.ConfigureServices(services =>
        {
            services.AddTalaria(opts =>
            {
                opts.ApplicationName = "test-app";
                opts.MinRetryDelay = TimeSpan.FromMilliseconds(50);
            })
            .UseInMemoryTransport(transport)
            .UseInMemoryDeferralStore()
            .UseIdempotencyStore(idempotencyStore);
        });

        var host = builder.Build();

        host.Services.MapTopic<RetryMessage>("retry.topic",
            (msg, ct) =>
            {
                Interlocked.Increment(ref attempts);
                if (Volatile.Read(ref attempts) < 2)
                {
                    throw new InvalidOperationException("boom");
                }

                return Task.CompletedTask;
            },
            new RetryPolicy
            {
                MaxRetryAttempts = 3,
                RetryInterval = TimeSpan.FromMilliseconds(50),
                BackoffType = RetryBackoffType.Fixed,
            });

        var producer = await transport.CreateProducerAsync<RetryMessage>("retry.topic", new ProducerOptions());
        await producer.ProduceAsync(new RetryMessage { Id = "MSG-DUP" }, new MessageHeaders { MessageId = "dup-1" });

        await host.StartAsync();

        var succeeded = await PollUntilAsync(
            () => Task.FromResult(Volatile.Read(ref attempts) >= 2), TimeSpan.FromSeconds(10));
        Assert.True(succeeded, "Handler did not succeed within timeout.");

        // Stability window: no duplicate retry copies should re-trigger the handler.
        var stable = await PollStableAsync(
            () => Task.FromResult(Volatile.Read(ref attempts) == 2), TimeSpan.FromSeconds(2));
        Assert.True(stable, $"Duplicate retry copy re-ran the handler (attempts = {Volatile.Read(ref attempts)}).");

        Assert.Empty(await transport.ReadAllFromTopicAsync<RetryMessage>("retry.topic.dlq"));

        await host.StopAsync();
        host.Dispose();
    }

    [Fact]
    public async Task OperationCanceledException_IsNotRetried()
    {
        var transport = new InMemoryTransport();
        var attempts = 0;

        var builder = Host.CreateDefaultBuilder();
        builder.ConfigureServices(services =>
        {
            services.AddTalaria(opts =>
            {
                opts.ApplicationName = "test-app";
                opts.MinRetryDelay = TimeSpan.FromMilliseconds(50);
            })
            .UseInMemoryTransport(transport)
            .UseInMemoryDeferralStore();
        });

        var host = builder.Build();

        host.Services.MapTopic<RetryMessage>("retry.topic",
            (msg, ct) =>
            {
                Interlocked.Increment(ref attempts);
                throw new OperationCanceledException();
            },
            new RetryPolicy
            {
                MaxRetryAttempts = 3,
                RetryInterval = TimeSpan.FromMilliseconds(50),
                BackoffType = RetryBackoffType.Fixed,
            });

        var producer = await transport.CreateProducerAsync<RetryMessage>("retry.topic", new ProducerOptions());
        await producer.ProduceAsync(new RetryMessage { Id = "MSG-CANCEL" }, new MessageHeaders { MessageId = "cancel-1" });

        await host.StartAsync();

        var dlq = await ReadUntilAsync<RetryMessage>(transport, "retry.topic.dlq", 1);

        Assert.Single(dlq);
        Assert.Equal(1, Volatile.Read(ref attempts));

        await host.StopAsync();
        host.Dispose();
    }

    [Fact]
    public async Task RetriedCopy_PreservesPartitionKey()
    {
        var transport = new InMemoryTransport();
        var attempts = 0;
        DeferredMessage? captured = null;
        var innerStore = new InMemoryDeferralStore();
        var capturingStore = new CapturingDeferralStore(innerStore, msg => captured = msg);

        var builder = Host.CreateDefaultBuilder();
        builder.ConfigureServices(services =>
        {
            services.AddTalaria(opts =>
            {
                opts.ApplicationName = "test-app";
                opts.MinRetryDelay = TimeSpan.FromMilliseconds(50);
            })
            .UseInMemoryTransport(transport)
            .UseIdempotencyStore<InMemoryIdempotencyStore>()
            .Services.AddSingleton<IDeferralStore>(capturingStore);
        });

        var host = builder.Build();

        host.Services.MapTopic<RetryMessage>("retry.topic",
            (msg, ct) =>
            {
                Interlocked.Increment(ref attempts);
                if (Volatile.Read(ref attempts) < 2)
                {
                    throw new InvalidOperationException("boom");
                }

                return Task.CompletedTask;
            },
            new RetryPolicy
            {
                MaxRetryAttempts = 2,
                RetryInterval = TimeSpan.FromMilliseconds(50),
                BackoffType = RetryBackoffType.Fixed,
            });

        var producer = await transport.CreateProducerAsync<RetryMessage>("retry.topic", new ProducerOptions());
        await producer.ProduceAsync(
            new RetryMessage { Id = "MSG-PART" },
            headers: new MessageHeaders { MessageId = "part-1" },
            partitionKey: "order-42");

        await host.StartAsync();

        // Wait until the retry has been durably scheduled and inspect the captured copy.
        var deferred = await PollUntilAsync(
            () => Task.FromResult(captured != null), TimeSpan.FromSeconds(5));
        Assert.True(deferred, "No deferred retry message was scheduled.");
        Assert.Equal("order-42", captured!.PartitionKey);

        // Confirm the retry eventually succeeds (sweeper republishes the deferred copy).
        var succeeded = await PollUntilAsync(
            () => Task.FromResult(Volatile.Read(ref attempts) >= 2), TimeSpan.FromSeconds(10));
        Assert.True(succeeded, "Handler did not succeed on retry.");

        await host.StopAsync();
        host.Dispose();
    }

    private sealed class CapturingDeferralStore : IDeferralStore
    {
        private readonly IDeferralStore _inner;
        private readonly Action<DeferredMessage> _onEnqueue;

        public CapturingDeferralStore(IDeferralStore inner, Action<DeferredMessage> onEnqueue)
        {
            _inner = inner;
            _onEnqueue = onEnqueue;
        }

        public async Task EnqueueAsync(DeferredMessage message, CancellationToken ct = default)
        {
            _onEnqueue(message);
            await _inner.EnqueueAsync(message, ct);
        }

        public Task<IReadOnlyList<LeasedDeferral>> AcquireDueAsync(DateTimeOffset now, TimeSpan leaseDuration, int maxBatch, CancellationToken ct = default)
            => _inner.AcquireDueAsync(now, leaseDuration, maxBatch, ct);

        public Task<bool> CompleteAsync(DeferralLease lease, CancellationToken ct = default)
            => _inner.CompleteAsync(lease, ct);

        public Task<bool> AbandonAsync(DeferralLease lease, DateTimeOffset? visibleAt = null, CancellationToken ct = default)
            => _inner.AbandonAsync(lease, visibleAt, ct);
    }

    // ---- Helpers ----

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

    private static async Task<bool> PollUntilAsync(Func<Task<bool>> condition, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (await condition())
            {
                return true;
            }

            await Task.Delay(50);
        }

        return await condition();
    }

    private static async Task<bool> PollStableAsync(Func<Task<bool>> condition, TimeSpan window)
    {
        var deadline = DateTime.UtcNow + window;
        while (DateTime.UtcNow < deadline)
        {
            if (!await condition())
            {
                return false;
            }

            await Task.Delay(50);
        }

        return await condition();
    }
}
