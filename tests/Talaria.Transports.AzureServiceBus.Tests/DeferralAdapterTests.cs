// SPDX-License-Identifier: Apache-2.0

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Azure;
using Azure.Messaging.ServiceBus;
using Talaria.Core.Abstractions;
using Talaria.Transports.AzureServiceBus.Deferral;
using Talaria.Transports.InMemory;
using Xunit;

namespace Talaria.Transports.AzureServiceBus.Tests;

/// <summary>
/// Unit tests for <see cref="DeferralAdapter"/>. The host-side sagas only care that
/// the adapter splits enqueues between the broker's ScheduledEnqueueTime path and
/// the durable store + sweeper path; these tests pin that contract.
/// </summary>
public class DeferralAdapterTests
{
    private static readonly TimeSpan Cutoff = TimeSpan.FromMinutes(5);
    private const int MaxBytes = 32 * 1024;

    private static (DeferralAdapter adapter, RecordingScheduler scheduler, InMemoryDeferralStore longTerm, FakeTimeProvider clock)
        Build()
    {
        var scheduler = new RecordingScheduler();
        var longTerm = new InMemoryDeferralStore();
        var adapterOptions = new DeferralAdapterOptions
        {
            ShortTermCutoff = Cutoff,
            MaxPayloadBytes = MaxBytes,
        };
        var clock = new FakeTimeProvider();
        var adapter = new DeferralAdapter(
            scheduler,
            longTerm,
            adapterOptions,
            clock);
        return (adapter, scheduler, longTerm, clock);
    }

    private static DeferredMessage MakeMessage(DateTimeOffset dueAt, int payloadBytes = 16, string topic = "topic-a", string? partitionKey = null)
        => new(
            Id: Guid.NewGuid(),
            Topic: topic,
            MessageType: typeof(object).AssemblyQualifiedName!,
            PayloadJson: new string('x', payloadBytes),
            Headers: new MessageHeaders { MessageId = "msg-1" },
            CorrelationId: "corr-1",
            Attempt: 1,
            DueAt: dueAt,
            PartitionKey: partitionKey);

    [Fact]
    public async Task EnqueueAsync_DueAtWithinCutoff_CallsSchedulerWithScheduledEnqueueTime()
    {
        var (adapter, scheduler, longTerm, clock) = Build();
        var now = new DateTimeOffset(2026, 1, 1, 12, 0, 0, TimeSpan.Zero);
        clock.SetUtcNow(now);

        var message = MakeMessage(now.AddMinutes(2)); // within Cutoff
        await adapter.EnqueueAsync(message);

        Assert.Single(scheduler.Calls);
        Assert.Equal(message.Topic, scheduler.Calls[0].Topic);
        Assert.Equal(message.DueAt, scheduler.Calls[0].ScheduledEnqueueTime);
        Assert.Equal(message.PayloadJson.Length, scheduler.Calls[0].Body.ToString().Length);
        // MessageHeaders keys must round-trip into ApplicationProperties.
        Assert.Contains(MessageHeaders.MessageIdKey, scheduler.Calls[0].ApplicationProperties.Keys);
        Assert.Equal(0, longTerm.Count);
    }

    [Fact]
    public async Task EnqueueAsync_DueAtBeyondCutoff_FallsThroughToDurableStore()
    {
        var (adapter, scheduler, longTerm, clock) = Build();
        var now = new DateTimeOffset(2026, 1, 1, 12, 0, 0, TimeSpan.Zero);
        clock.SetUtcNow(now);

        var message = MakeMessage(now.AddHours(1)); // beyond Cutoff
        await adapter.EnqueueAsync(message);

        Assert.Empty(scheduler.Calls);
        Assert.Equal(1, longTerm.Count);
    }

    [Fact]
    public async Task EnqueueAsync_PayloadOverMaxBytes_FallsThroughToDurableStore()
    {
        var (adapter, scheduler, longTerm, clock) = Build();
        var now = new DateTimeOffset(2026, 1, 1, 12, 0, 0, TimeSpan.Zero);
        clock.SetUtcNow(now);

        // Payload exceeds the configured 32 KB cap; deferral must go to the long store
        // even though DueAt is well within the cutoff window.
        var message = MakeMessage(now.AddMinutes(1), payloadBytes: MaxBytes + 1);
        await adapter.EnqueueAsync(message);

        Assert.Empty(scheduler.Calls);
        Assert.Equal(1, longTerm.Count);
    }

    [Fact]
    public async Task EnqueueAsync_AtExactCutoff_GoesToScheduler()
    {
        var (adapter, scheduler, longTerm, clock) = Build();
        var now = new DateTimeOffset(2026, 1, 1, 12, 0, 0, TimeSpan.Zero);
        clock.SetUtcNow(now);

        var message = MakeMessage(now.Add(Cutoff)); // boundary equality -> short path
        await adapter.EnqueueAsync(message);

        Assert.Single(scheduler.Calls);
        Assert.Equal(0, longTerm.Count);
    }

    [Fact]
    public async Task AcquireDueAsync_ForwardsToDurableStore_AndReturnsItsResult()
    {
        var (adapter, scheduler, longTerm, clock) = Build();
        var now = new DateTimeOffset(2026, 1, 1, 12, 0, 0, TimeSpan.Zero);
        clock.SetUtcNow(now);

        await longTerm.EnqueueAsync(MakeMessage(now.AddMinutes(-1)));
        await longTerm.EnqueueAsync(MakeMessage(now.AddHours(1)));

        var leased = await adapter.AcquireDueAsync(now.AddSeconds(1), TimeSpan.FromSeconds(30), maxBatch: 10);

        Assert.Empty(scheduler.Calls); // sweeper path never touches the broker
        Assert.Single(leased);
        Assert.Equal(2, longTerm.Count); // leased entry hidden, not removed
    }

    [Fact]
    public async Task CompleteAsync_ForwardsToDurableStore_AndReturnsFencedResult()
    {
        var (adapter, scheduler, longTerm, clock) = Build();
        var now = new DateTimeOffset(2026, 1, 1, 12, 0, 0, TimeSpan.Zero);
        clock.SetUtcNow(now);

        await longTerm.EnqueueAsync(MakeMessage(now.AddMinutes(-1)));

        var leased = Assert.Single(await adapter.AcquireDueAsync(now, TimeSpan.FromSeconds(30), 10));
        Assert.True(await adapter.CompleteAsync(leased.Lease));
        Assert.Equal(0, longTerm.Count);

        // Stale token must be rejected; the adapter is just a pass-through, the store
        // retains its fencing-token contract.
        Assert.False(await adapter.CompleteAsync(leased.Lease));
    }

    [Fact]
    public async Task EnqueueAsync_WithPartitionKey_PassesPartitionKeyToScheduler()
    {
        var (adapter, scheduler, longTerm, clock) = Build();
        var now = new DateTimeOffset(2026, 1, 1, 12, 0, 0, TimeSpan.Zero);
        clock.SetUtcNow(now);

        var message = MakeMessage(now.AddMinutes(2), partitionKey: "pk-1");
        await adapter.EnqueueAsync(message);

        var call = Assert.Single(scheduler.Calls);
        Assert.Equal("pk-1", call.PartitionKey);
        Assert.Equal(0, longTerm.Count);
    }

    [Fact]
    public async Task ServiceBusMessageScheduler_SetsSessionIdFromPartitionKey()
    {
        var sender = new RecordingSender();
        await using var client = new FakeServiceBusClient(sender);
        var scheduler = new ServiceBusMessageScheduler(client);
        var dueAt = new DateTimeOffset(2026, 1, 1, 12, 0, 0, TimeSpan.Zero);
        var properties = new Dictionary<string, object>
        {
            [MessageHeaders.MessageTypeKey] = typeof(object).FullName!,
        };

        await scheduler.ScheduleAsync("topic-a", BinaryData.FromString("{}"), properties, dueAt, partitionKey: "pk-1");

        var scheduled = Assert.Single(sender.Scheduled);
        Assert.Equal("pk-1", scheduled.Message.SessionId);
        Assert.Equal(dueAt, scheduled.ScheduledEnqueueTime);
    }

    [Fact]
    public async Task ServiceBusMessageScheduler_WithoutPartitionKey_UsesCorrelationIdHeaderAsSessionId()
    {
        var sender = new RecordingSender();
        await using var client = new FakeServiceBusClient(sender);
        var scheduler = new ServiceBusMessageScheduler(client);
        var dueAt = new DateTimeOffset(2026, 1, 1, 12, 0, 0, TimeSpan.Zero);
        var properties = new Dictionary<string, object>
        {
            [MessageHeaders.MessageTypeKey] = typeof(object).FullName!,
            [MessageHeaders.CorrelationIdKey] = "corr-1",
        };

        await scheduler.ScheduleAsync("topic-a", BinaryData.FromString("{}"), properties, dueAt, partitionKey: null);

        var scheduled = Assert.Single(sender.Scheduled);
        Assert.Equal("corr-1", scheduled.Message.SessionId);
    }

    [Fact]
    public async Task ServiceBusMessageScheduler_WithEmptyPartitionKey_FallsBackToCorrelationIdHeader()
    {
        var sender = new RecordingSender();
        await using var client = new FakeServiceBusClient(sender);
        var scheduler = new ServiceBusMessageScheduler(client);
        var dueAt = new DateTimeOffset(2026, 1, 1, 12, 0, 0, TimeSpan.Zero);
        var properties = new Dictionary<string, object>
        {
            [MessageHeaders.MessageTypeKey] = typeof(object).FullName!,
            [MessageHeaders.CorrelationIdKey] = "corr-1",
        };

        await scheduler.ScheduleAsync("topic-a", BinaryData.FromString("{}"), properties, dueAt, partitionKey: "");

        var scheduled = Assert.Single(sender.Scheduled);
        Assert.Equal("corr-1", scheduled.Message.SessionId);
    }

    private sealed class FakeServiceBusClient : ServiceBusClient
    {
        private readonly ServiceBusSender _sender;

        public FakeServiceBusClient(ServiceBusSender sender)
        {
            _sender = sender;
        }

        public override ServiceBusSender CreateSender(string queueOrTopicName) => _sender;

        public override ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}

/// <summary>
/// Minimal time provider for deterministic deferral routing tests. The deferred
/// message routing rule depends on a single <c>GetUtcNow()</c> call against
/// <see cref="DeferralAdapter"/>; freezing that call lets tests put DueAt anywhere
/// relative to "now" without fiddling with the host clock.
/// </summary>
internal sealed class FakeTimeProvider : TimeProvider
{
    private DateTimeOffset _now;

    public FakeTimeProvider(DateTimeOffset? initial = null)
    {
        _now = initial ?? new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
    }

    public override DateTimeOffset GetUtcNow() => _now;

    public void SetUtcNow(DateTimeOffset now) => _now = now;
}
