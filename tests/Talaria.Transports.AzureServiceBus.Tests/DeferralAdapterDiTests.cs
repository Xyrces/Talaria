// SPDX-License-Identifier: Apache-2.0

using System;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Talaria.Core.Abstractions;
using Talaria.Core.Registration;
using Talaria.Transports.AzureServiceBus.Deferral;
using Talaria.Transports.InMemory;
using Xunit;

namespace Talaria.Transports.AzureServiceBus.Tests;

/// <summary>
/// DI wiring tests for <see cref="AzureServiceBusDeferralExtensions.UseAzureServiceBusDeferral(Talaria.Core.Registration.TalariaBuilder, System.Action{DeferralAdapterOptions})"/>. These guard the
/// registration contract that the reviewer flagged on PR #21: the extension must
/// capture the previously-registered durable <see cref="IDeferralStore"/> BEFORE
/// clearing the <c>IDeferralStore</c> slot, otherwise the adapter factory resolves
/// itself (circular) and the long-term store is lost. The tests below exercise the
/// happy path (in-memory durable + ASB adapter) and the missing-durable-store
/// precondition that protects against silent misconfiguration.
/// </summary>
public class DeferralAdapterDiTests
{
    /// <summary>
    /// Wiring for the happy path: a real <see cref="InMemoryDeferralStore"/> registered
    /// first, then <see cref="AzureServiceBusDeferralExtensions.UseAzureServiceBusDeferral(Talaria.Core.Registration.TalariaBuilder, System.Action{DeferralAdapterOptions})"/> on top of a fake scheduler.
    /// The fake scheduler is registered as an instance BEFORE
    /// <c>UseAzureServiceBusDeferral</c> so the adapter's
    /// <c>TryAddSingleton&lt;IServiceBusMessageScheduler&gt;</c> is a no-op — this is
    /// the public seam tests should rely on. The in-memory transport keeps
    /// <c>BuildServiceProvider</c> from complaining about missing hosted-service
    /// dependencies.
    /// </summary>
    private static IServiceProvider BuildProviderWithInMemoryDurableAndAsbAdapter()
    {
        var services = new ServiceCollection();
        services
            .AddTalaria()
            .UseInMemoryDeferralStore()
            .UseInMemoryTransport();

        // Pre-register the fake scheduler before the ASB extension so its
        // TryAddSingleton short-circuits and the adapter resolves our fake.
        services.AddSingleton<IServiceBusMessageScheduler>(new RecordingScheduler());
        services.AddTalaria().UseAzureServiceBusDeferral();

        return services.BuildServiceProvider();
    }

    [Fact]
    public void UseInMemoryDeferralStore_ThenUseAzureServiceBusDeferral_ResolvesAdapter()
    {
        // Arrange: standard composition (in-memory durable + ASB adapter).
        var provider = BuildProviderWithInMemoryDurableAndAsbAdapter();

        // Act: resolve the engine-facing IDeferralStore.
        var resolved = provider.GetRequiredService<IDeferralStore>();

        // Assert: the slot is now the adapter, not the in-memory store. If the
        // extension had regressed to the original "RemoveAll then re-resolve"
        // pattern, BuildServiceProvider would have thrown a circular-dependency
        // or InvalidOperationException before this Assert could run.
        Assert.IsType<DeferralAdapter>(resolved);
    }

    [Fact]
    public async Task ResolvedAdapter_LongTermEnqueue_BypassesScheduler()
    {
        // Arrange.
        var provider = BuildProviderWithInMemoryDurableAndAsbAdapter();
        var resolved = provider.GetRequiredService<IDeferralStore>();
        var scheduler = (RecordingScheduler)provider
            .GetRequiredService<IServiceBusMessageScheduler>();

        var message = new DeferredMessage(
            Id: Guid.NewGuid(),
            Topic: "topic-a",
            MessageType: typeof(object).AssemblyQualifiedName!,
            PayloadJson: "{}",
            Headers: new MessageHeaders { MessageId = "msg-1" },
            CorrelationId: "corr-1",
            Attempt: 1,
            // 1 day out: well beyond the adapter's 10-minute default short-term cutoff,
            // so the adapter must hand it to the durable store, not the scheduler.
            DueAt: DateTimeOffset.UtcNow.AddDays(1),
            PartitionKey: null);

        // Act: route a long-term deferral through the resolved adapter.
        await resolved.EnqueueAsync(message);

        // Assert: the broker scheduler was bypassed (long path), proving the adapter
        // has a live durable store behind it. If the adapter's long-term store were
        // null or the registration had wired up a circular reference, this enqueue
        // would either have thrown or sent the message to the scheduler.
        Assert.Empty(scheduler.Calls);

        // AcquireDueAsync is a pure pass-through to the long-term store. The fact
        // that this returns a non-throwing, empty list (no due entries yet) confirms
        // the adapter's call chain reaches the in-memory backing store.
        var leased = await resolved.AcquireDueAsync(
            DateTimeOffset.UtcNow,
            TimeSpan.FromSeconds(30),
            maxBatch: 10);
        Assert.NotNull(leased);
        Assert.Empty(leased);
    }

    [Fact]
    public async Task ResolvedAdapter_ShortTermEnqueue_RoutesToScheduler()
    {
        // Arrange.
        var provider = BuildProviderWithInMemoryDurableAndAsbAdapter();
        var resolved = provider.GetRequiredService<IDeferralStore>();
        var scheduler = (RecordingScheduler)provider
            .GetRequiredService<IServiceBusMessageScheduler>();

        var message = new DeferredMessage(
            Id: Guid.NewGuid(),
            Topic: "topic-a",
            MessageType: typeof(object).AssemblyQualifiedName!,
            PayloadJson: "{}",
            Headers: new MessageHeaders { MessageId = "msg-1" },
            CorrelationId: "corr-1",
            Attempt: 1,
            // Due in the past: the short-term cutoff (10 min) admits it to the
            // broker-side schedule path immediately.
            DueAt: DateTimeOffset.UtcNow,
            PartitionKey: null);

        // Act.
        await resolved.EnqueueAsync(message);

        // Assert: the scheduler (not the durable store) saw the message. The
        // recording scheduler captures every ScheduleAsync call.
        var call = Assert.Single(scheduler.Calls);
        Assert.Equal("topic-a", call.Topic);
        Assert.Equal(message.DueAt, call.ScheduledEnqueueTime);
    }

    [Fact]
    public void UseAzureServiceBusDeferral_WithoutDurableStore_Throws()
    {
        // Arrange: same composition but no UseInMemoryDeferralStore. The extension
        // must throw synchronously at registration time so the misconfiguration
        // surfaces during host startup, not at first message deferral.
        var services = new ServiceCollection();
        services
            .AddTalaria()
            .UseInMemoryTransport();
        services.AddSingleton<IServiceBusMessageScheduler>(new RecordingScheduler());

        // Act + Assert: the registration call itself throws because the eager
        // precondition (no captured durable descriptor) fires before the adapter
        // factory is even reached.
        var ex = Assert.Throws<InvalidOperationException>(() =>
            services.AddTalaria().UseAzureServiceBusDeferral());
        Assert.Contains("IDeferralStore", ex.Message);
    }
}
