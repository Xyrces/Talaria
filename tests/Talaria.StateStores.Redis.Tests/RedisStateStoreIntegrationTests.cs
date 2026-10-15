using Microsoft.Extensions.DependencyInjection;
using Talaria.Core.Abstractions;
using Talaria.Core.Registration;
using Testcontainers.Redis;
using Xunit;

namespace Talaria.StateStores.Redis.Tests;

public class RedisStateStoreIntegrationTests : IAsyncLifetime
{
    private RedisContainer? _redisContainer;
    private IServiceProvider _serviceProvider = null!;

    public async Task InitializeAsync()
    {
        if (!DockerFactAttribute.IsDockerRunning()) return;

        _redisContainer = new RedisBuilder("redis:7.2")
            .Build();

        await _redisContainer.StartAsync();

        var services = new ServiceCollection();
        var builder = services.AddTalaria();
        builder.UseRedisStateStore(opts =>
        {
            opts.Configuration = _redisContainer!.GetConnectionString();
            opts.KeyPrefix = "test-store:";
        });

        _serviceProvider = services.BuildServiceProvider();
    }

    public async Task DisposeAsync()
    {
        if (_redisContainer != null)
        {
            await _redisContainer.DisposeAsync();
        }
    }

    [DockerFact]
    public async Task SaveAndLoad_SagaState_Successfully()
    {
        var stateStore = _serviceProvider.GetRequiredService<IStateStore<DummyState>>();
        
        var correlationId = Guid.NewGuid().ToString("N");
        var state = new DummyState { Value = "Test 123", StepCount = 5 };

        // Save
        await stateStore.SaveAsync(correlationId, state);

        // Load
        var loaded = await stateStore.GetAsync(correlationId);

        Assert.NotNull(loaded);
        Assert.Equal("Test 123", loaded.Value);
        Assert.Equal(5, loaded.StepCount);

        // Delete
        await stateStore.DeleteAsync(correlationId);

        var loadedAfterDelete = await stateStore.GetAsync(correlationId);
        Assert.Null(loadedAfterDelete);
    }

    [DockerFact]
    public async Task CorrelationIds_CannotOverlapSagaHistoryKeys()
    {
        var stateStore = _serviceProvider.GetRequiredService<IStateStore<DummyState>>();
        await stateStore.SaveAsync("x", new DummyState { Value = "first", StepCount = 1 });
        await stateStore.SaveAsync("x:history", new DummyState { Value = "second", StepCount = 2 });

        Assert.Equal("first", (await stateStore.GetAsync("x"))!.Value);
        Assert.Equal("second", (await stateStore.GetAsync("x:history"))!.Value);
    }

    [DockerFact]
    public async Task TryTransitionAsync_WithAdditionalReceipts_PersistsTimeoutCancellationReceipts()
    {
        var stateStore = _serviceProvider.GetRequiredService<IStateStore<DummyState>>();
        var correlationId = Guid.NewGuid().ToString("N");

        Assert.Equal(SagaCommitStatus.Committed, await stateStore.TryTransitionAsync(
            correlationId,
            "starter-1",
            expectedVersion: 0,
            new DummyState { Value = "started", StepCount = 1 },
            []));

        const string cancelReceipt = "timeout-canceled:Talaria.Specs.OrderTimeout:v1";
        Assert.Equal(SagaCommitStatus.Committed, await stateStore.TryTransitionAsync(
            correlationId,
            "cancel-1",
            expectedVersion: 1,
            new DummyState { Value = "canceled", StepCount = 2 },
            [],
            [cancelReceipt]));

        var snapshot = await stateStore.ReadSnapshotAsync(correlationId, cancelReceipt);
        Assert.True(snapshot.MessageProcessed);
        Assert.Equal(2, snapshot.Version);
        Assert.Equal("canceled", snapshot.State?.Value);
    }

    [DockerFact]
    public async Task TryTransitionAsync_WithCompensatableSagaState_RoundTripsSagaCompensationStateAndOutboxAndTimeoutReceipts()
    {
        var stateStore = _serviceProvider.GetRequiredService<IStateStore<CompensatableRedisState>>();
        var correlationId = Guid.NewGuid().ToString("N");

        var initialState = new CompensatableRedisState
        {
            OrderId = correlationId,
            Compensation = new Talaria.Core.Sagas.SagaCompensationState
            {
                Phase = Talaria.Core.Sagas.SagaPhase.Compensating,
                Generation = 1,
                RecordedSteps = ["reserve-inventory", "charge-payment"],
                PendingSteps =
                [
                    new Talaria.Core.Sagas.PendingCompensationEntry(
                        $"{correlationId}:comp:1:0:refund-payment",
                        "refund-payment",
                        typeof(string).AssemblyQualifiedName!,
                        "payments.refund",
                        "\"refund-cmd\"",
                        1),
                    new Talaria.Core.Sagas.PendingCompensationEntry(
                        $"{correlationId}:comp:1:1:release-inventory",
                        "release-inventory",
                        typeof(string).AssemblyQualifiedName!,
                        "inventory.release",
                        "\"release-cmd\"",
                        1)
                ],
                CompletedSteps = []
            }
        };

        var outboxMsg = new OutboxMessage(
            Guid.NewGuid(),
            "payments.refund",
            typeof(string).AssemblyQualifiedName!,
            "\"refund-cmd\"",
            new MessageHeaders
            {
                MessageId = "comp-out-1",
                CorrelationId = correlationId,
                ["talaria.saga.compensation"] = "true",
                ["talaria.saga.compensation_step"] = "refund-payment"
            },
            DateTimeOffset.UtcNow);

        const string cancelReceipt = "timeout-canceled:Talaria.Specs.OrderPaymentTimeout:v1";
        Assert.Equal(SagaCommitStatus.Committed, await stateStore.TryTransitionAsync(
            correlationId,
            "fail-trigger-1",
            expectedVersion: 0,
            initialState,
            [outboxMsg],
            [cancelReceipt]));

        var snapshot = await stateStore.ReadSnapshotAsync(correlationId, "ack-check-1");
        Assert.NotNull(snapshot.State);
        Assert.Equal(1, snapshot.Version);
        Assert.Equal(Talaria.Core.Sagas.SagaPhase.Compensating, snapshot.State!.Compensation.Phase);
        Assert.Equal(1, snapshot.State.Compensation.Generation);
        Assert.Equal(["reserve-inventory", "charge-payment"], snapshot.State.Compensation.RecordedSteps);
        Assert.Equal(2, snapshot.State.Compensation.PendingSteps.Count);
        Assert.Equal("refund-payment", snapshot.State.Compensation.PendingSteps[0].StepName);
        Assert.Equal("release-inventory", snapshot.State.Compensation.PendingSteps[1].StepName);

        var receiptCheck = await stateStore.ReadSnapshotAsync(correlationId, cancelReceipt);
        Assert.True(receiptCheck.MessageProcessed);
    }

    public class DummyState
    {
        public string Value { get; set; } = string.Empty;
        public int StepCount { get; set; }
    }

    public sealed class CompensatableRedisState : Talaria.Core.Sagas.ICompensatableState
    {
        public string OrderId { get; set; } = string.Empty;
        public Talaria.Core.Sagas.SagaCompensationState Compensation { get; set; } = new();
    }
}

