using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using Talaria.Core;
using Talaria.Core.Abstractions;
using Talaria.Core.Hosting;
using Talaria.Core.Registration;
using Talaria.Core.Sagas;
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

        var succeeded = await TestAsyncHelpers.PollUntilAsync(
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

        var succeeded = await TestAsyncHelpers.PollUntilAsync(
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

        var dlq = await TestAsyncHelpers.ReadUntilAsync<RetryMessage>(transport, "retry.topic.dlq", 1);

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

        var dlq = await TestAsyncHelpers.ReadUntilAsync<RetryMessage>(transport, "retry.topic.dlq", 1);

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

        var succeeded = await TestAsyncHelpers.PollUntilAsync(
            () => Task.FromResult(Volatile.Read(ref attempts) >= 2), TimeSpan.FromSeconds(10));
        Assert.True(succeeded, "Handler did not succeed within timeout.");

        // Stability window: no duplicate retry copies should re-trigger the handler.
        var stable = await TestAsyncHelpers.PollStableAsync(
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

        var dlq = await TestAsyncHelpers.ReadUntilAsync<RetryMessage>(transport, "retry.topic.dlq", 1);

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
        var deferred = await TestAsyncHelpers.PollUntilAsync(
            () => Task.FromResult(captured != null), TimeSpan.FromSeconds(5));
        Assert.True(deferred, "No deferred retry message was scheduled.");
        Assert.Equal("order-42", captured!.PartitionKey);

        // Confirm the retry eventually succeeds (sweeper republishes the deferred copy).
        var succeeded = await TestAsyncHelpers.PollUntilAsync(
            () => Task.FromResult(Volatile.Read(ref attempts) >= 2), TimeSpan.FromSeconds(10));
        Assert.True(succeeded, "Handler did not succeed on retry.");

        await host.StopAsync();
        host.Dispose();
    }

    [Fact]
    public async Task OperationCanceledException_DuringShutdown_IsNotDLQed_AndRedelivers()
    {
        var transport = new InMemoryTransport();
        var attempts = 0;
        var handlerCts = new CancellationTokenSource();

        var topicReg = new TopicRegistry();
        topicReg.MapTopic<RetryMessage>("shutdown.topic",
            async (msg, ct) =>
            {
                Interlocked.Increment(ref attempts);
                await Task.Delay(Timeout.Infinite, handlerCts.Token);
            },
            new RetryPolicy
            {
                MaxRetryAttempts = 3,
                RetryInterval = TimeSpan.FromMilliseconds(50),
                BackoffType = RetryBackoffType.Fixed,
            });

        var listener = new TalariaListener(
            transport,
            topicReg,
            new SagaRegistry(),
            new TalariaOptions
            {
                ApplicationName = "shutdown-app",
                MinRetryDelay = TimeSpan.FromMilliseconds(50),
            },
            NullLogger<TalariaListener>.Instance);

        await listener.StartAsync();

        var producer = await transport.CreateProducerAsync<RetryMessage>("shutdown.topic", new ProducerOptions());
        await producer.ProduceAsync(new RetryMessage { Id = "MSG-SHUTDOWN" }, new MessageHeaders { MessageId = "shutdown-1" });

        // Wait until the handler has been entered at least once.
        await TestAsyncHelpers.PollUntilAsync(
            () => Task.FromResult(Volatile.Read(ref attempts) >= 1), TimeSpan.FromSeconds(5));

        // Signal the handler to cancel, then stop the listener. The handler will throw OCE.
        handlerCts.Cancel();
        await listener.StopAsync();

        // The message must NOT be in the DLQ.
        Assert.Empty(await transport.ReadAllFromTopicAsync<RetryMessage>("shutdown.topic.dlq"));

        // Start a fresh listener; the uncommitted message redelivers.
        var received = new List<string>();
        var topicReg2 = new TopicRegistry();
        topicReg2.MapTopic<RetryMessage>("shutdown.topic", (msg, ct) =>
        {
            received.Add(msg.Id);
            return Task.CompletedTask;
        });

        var listener2 = new TalariaListener(
            transport,
            topicReg2,
            new SagaRegistry(),
            new TalariaOptions { ApplicationName = "shutdown-app" },
            NullLogger<TalariaListener>.Instance);

        await listener2.StartAsync();

        await TestAsyncHelpers.PollUntilAsync(
            () => Task.FromResult(received.Count == 1), TimeSpan.FromSeconds(5));
        Assert.Equal("MSG-SHUTDOWN", received[0]);

        await listener2.StopAsync();
    }

    [Fact]
    public async Task Manual_TalariaListener_TopicRetry_FailsThenSucceeds()
    {
        var transport = new InMemoryTransport();
        var deferralStore = new InMemoryDeferralStore();
        var attempts = 0;
        var received = new List<string>();

        var topicReg = new TopicRegistry();
        topicReg.MapTopic<RetryMessage>("manual-retry.topic",
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

        var listener = new TalariaListener(
            transport,
            topicReg,
            new SagaRegistry(),
            new TalariaOptions
            {
                ApplicationName = "manual-retry-app",
                MinRetryDelay = TimeSpan.FromMilliseconds(50),
                DeferralBackoff = TimeSpan.FromMilliseconds(50),
            },
            NullLogger<TalariaListener>.Instance,
            stores: new TalariaListenerStores(null, deferralStore, null));

        await listener.StartAsync();

        var producer = await transport.CreateProducerAsync<RetryMessage>("manual-retry.topic", new ProducerOptions());
        await producer.ProduceAsync(new RetryMessage { Id = "MSG-MANUAL-RETRY" }, new MessageHeaders { MessageId = "manual-1" });

        var succeeded = await TestAsyncHelpers.PollUntilAsync(
            () => Task.FromResult(Volatile.Read(ref attempts) >= 3), TimeSpan.FromSeconds(10));
        Assert.True(succeeded, $"Handler did not run 3 times (ran {Volatile.Read(ref attempts)}).");

        Assert.Equal(["MSG-MANUAL-RETRY", "MSG-MANUAL-RETRY", "MSG-MANUAL-RETRY"], received);
        Assert.Empty(await transport.ReadAllFromTopicAsync<RetryMessage>("manual-retry.topic.dlq"));

        await listener.StopAsync();
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

}
