// SPDX-License-Identifier: Apache-2.0

using System.Diagnostics.Metrics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Talaria.Core;
using Talaria.Core.Abstractions;
using Talaria.Core.Diagnostics;
using Talaria.Core.Hosting;
using Talaria.Core.Registration;
using Talaria.Core.Sagas;
using Talaria.Transports.InMemory;

namespace Talaria.Specs.Tests;

/// <summary>
/// Comprehensive Given/When/Then acceptance tests for sequential and batch saga compensation workflows.
/// </summary>
public sealed class SagaCompensationBehaviorTests
{
    public sealed class OrderCompensationSagaState : ICompensatableState
    {
        public string OrderId { get; set; } = string.Empty;
        public bool InventoryReserved { get; set; }
        public string? ReservationId { get; set; }
        public bool PaymentCharged { get; set; }
        public string? ChargeId { get; set; }
        public bool ShipmentBooked { get; set; }
        public string? ShipmentId { get; set; }
        public string Status { get; set; } = "Started";
        public SagaCompensationState Compensation { get; set; } = new();
    }

    public sealed record OrderPlaced(string OrderId, string ReservationId);
    public sealed record PaymentSucceeded(string OrderId, string ChargeId);
    public sealed record ShipmentBookedEvent(string OrderId, string ShipmentId);
    public sealed record ShipmentBookingFailed(string OrderId, string Reason);
    public sealed record SecondaryFailureTrigger(string OrderId, string Reason);
    public sealed record OrderPaymentTimeout(string OrderId);

    public sealed record ReleaseInventory(string OrderId, string ReservationId);
    public sealed record InventoryReleased(string OrderId, string? OperationId = null);

    public sealed record RefundPayment(string OrderId, string ChargeId);
    public sealed record PaymentRefunded(string OrderId, string? OperationId = null);

    public sealed record CancelShipment(string OrderId, string ShipmentId);
    public sealed record ShipmentCanceled(string OrderId, string? OperationId = null);

    public sealed record OrderFailed(string OrderId, string Status);

    // -------------------------------------------------------------------------
    // Scenario 1: Strict Sequential Execution (LIFO 1-by-1 Gated on TAck)
    // -------------------------------------------------------------------------
    [Fact]
    public async Task Given_SequentialCompensationSaga_When_Step3Fails_Then_DispatchesOneByOneInLifoOrderAndCompletesOnlyAfterFinalAck()
    {
        var transport = new InMemoryTransport();
        var outboxStore = new InMemoryOutboxStore();
        var stateStore = new InMemoryStateStore<OrderCompensationSagaState>(outboxStore);
        var deferralStore = new InMemoryDeferralStore();
        var registry = new SagaRegistry();

        const string refundTopic = "seq-1.payments.refund";
        const string refundAckTopic = "seq-1.payments.refunded";
        const string releaseTopic = "seq-1.inventory.release";
        const string releaseAckTopic = "seq-1.inventory.released";
        const string cancelShipTopic = "seq-1.shipping.cancel";
        const string cancelShipAckTopic = "seq-1.shipping.canceled";
        const string failedTopic = "seq-1.orders.failed";

        var config = new SagaConfigurator<OrderCompensationSagaState>(registry);
        config
            .CompensateStep<ReleaseInventory, InventoryReleased>(
                when: s => s.InventoryReserved,
                create: s => new ReleaseInventory(s.OrderId, s.ReservationId!),
                onCompensated: (s, _) => s.InventoryReserved = false,
                correlateBy: a => a.OrderId,
                stepName: "release-inventory",
                commandTopic: releaseTopic,
                ackTopic: releaseAckTopic)
            .CompensateStep<RefundPayment, PaymentRefunded>(
                when: s => s.PaymentCharged,
                create: s => new RefundPayment(s.OrderId, s.ChargeId!),
                onCompensated: (s, _) => s.PaymentCharged = false,
                correlateBy: a => a.OrderId,
                stepName: "refund-payment",
                commandTopic: refundTopic,
                ackTopic: refundAckTopic)
            .CompensateStep<CancelShipment, ShipmentCanceled>(
                when: s => s.ShipmentBooked,
                create: s => new CancelShipment(s.OrderId, s.ShipmentId!),
                onCompensated: (s, _) => s.ShipmentBooked = false,
                correlateBy: a => a.OrderId,
                stepName: "cancel-shipment",
                commandTopic: cancelShipTopic,
                ackTopic: cancelShipAckTopic)
            .OnCompensationCompleted((state, ctx) =>
            {
                state.Status = "Compensated";
                return Task.FromResult(ctx
                    .Dispatch(new OrderFailed(state.OrderId, "Compensated"))
                    .Complete());
            })
            .DispatchTo<OrderFailed>(failedTopic)
            .StartedBy<OrderPlaced>(
                "seq-1.orders.placed",
                (msg, ctx) => Task.FromResult(ctx.Transition(new OrderCompensationSagaState
                {
                    OrderId = msg.OrderId,
                    InventoryReserved = true,
                    ReservationId = msg.ReservationId
                })),
                m => m.OrderId)
            .On<PaymentSucceeded>(
                "seq-1.payments.succeeded",
                (state, msg, ctx) =>
                {
                    state.PaymentCharged = true;
                    state.ChargeId = msg.ChargeId;
                    return Task.FromResult(ctx.Transition(state));
                },
                m => m.OrderId)
            .On<ShipmentBookingFailed>(
                "seq-1.shipping.failed",
                (state, _, ctx) =>
                {
                    state.Status = "Compensating";
                    return Task.FromResult(ctx.Compensate(state));
                },
                m => m.OrderId);
        config.Complete();

        var services = new ServiceCollection()
            .AddSingleton<ITransport>(transport)
            .AddSingleton<IOutboxStore>(outboxStore)
            .AddSingleton<IStateStore<OrderCompensationSagaState>>(stateStore)
            .AddSingleton<IDeferralStore>(deferralStore)
            .BuildServiceProvider();

        var listener = new TalariaListener(
            transport,
            new TopicRegistry(),
            registry,
            new TalariaOptions
            {
                ApplicationName = "seq-1-app",
                OutboxRelayInterval = TimeSpan.FromMilliseconds(15),
                DeferralBackoff = TimeSpan.FromMilliseconds(20)
            },
            NullLogger<TalariaListener>.Instance,
            services);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await listener.StartAsync(cts.Token);

        try
        {
            await using var placedProducer = await transport.CreateProducerAsync<OrderPlaced>("seq-1.orders.placed", new ProducerOptions());
            await placedProducer.ProduceAsync(new OrderPlaced("ord-seq-1", "res-100"));
            Assert.True(await TestAsyncHelpers.PollUntilAsync(async () => (await stateStore.GetAsync("ord-seq-1"))?.InventoryReserved == true, TimeSpan.FromSeconds(3)));

            await using var payProducer = await transport.CreateProducerAsync<PaymentSucceeded>("seq-1.payments.succeeded", new ProducerOptions());
            await payProducer.ProduceAsync(new PaymentSucceeded("ord-seq-1", "chg-200"));
            Assert.True(await TestAsyncHelpers.PollUntilAsync(async () => (await stateStore.GetAsync("ord-seq-1"))?.PaymentCharged == true, TimeSpan.FromSeconds(3)));

            await using var failProducer = await transport.CreateProducerAsync<ShipmentBookingFailed>("seq-1.shipping.failed", new ProducerOptions());
            await failProducer.ProduceAsync(new ShipmentBookingFailed("ord-seq-1", "CarrierDown"));

            // Wait for RefundPayment (first LIFO compensation) to be published
            MessageEnvelope<RefundPayment>? refundEnv = null;
            Assert.True(await TestAsyncHelpers.PollUntilAsync(async () =>
            {
                var msgs = await transport.ReadAllFromTopicAsync<RefundPayment>(refundTopic);
                if (msgs.Count > 0) refundEnv = msgs[0];
                return refundEnv is not null;
            }, TimeSpan.FromSeconds(3)));

            Assert.NotNull(refundEnv);
            Assert.Equal("ord-seq-1", refundEnv!.Payload.OrderId);
            Assert.Equal("chg-200", refundEnv.Payload.ChargeId);
            Assert.Equal("true", refundEnv.Headers["talaria.saga.compensation"]);
            Assert.Equal("command", refundEnv.Headers["talaria.intent"]);
            Assert.Equal("refund-payment", refundEnv.Headers["talaria.saga.compensation_step"]);
            Assert.Equal("1", refundEnv.Headers["talaria.saga.compensation_generation"]);

            // ReleaseInventory, CancelShipment, and OrderFailed MUST NOT be dispatched yet!
            Assert.Empty(await transport.ReadAllFromTopicAsync<ReleaseInventory>(releaseTopic));
            Assert.Empty(await transport.ReadAllFromTopicAsync<CancelShipment>(cancelShipTopic));
            Assert.Empty(await transport.ReadAllFromTopicAsync<OrderFailed>(failedTopic));

            var compensatingState = await stateStore.GetAsync("ord-seq-1");
            Assert.NotNull(compensatingState);
            Assert.Equal(SagaPhase.Compensating, compensatingState!.Compensation.Phase);
            Assert.Equal(2, compensatingState.Compensation.PendingSteps.Count);
            Assert.Equal("refund-payment", compensatingState.Compensation.PendingSteps[0].StepName);
            Assert.Equal("release-inventory", compensatingState.Compensation.PendingSteps[1].StepName);

            // Now send PaymentRefunded ack
            await using var refundAckProducer = await transport.CreateProducerAsync<PaymentRefunded>(refundAckTopic, new ProducerOptions());
            await refundAckProducer.ProduceAsync(new PaymentRefunded("ord-seq-1"));

            // Wait for ReleaseInventory (second LIFO compensation) to be published
            MessageEnvelope<ReleaseInventory>? releaseEnv = null;
            Assert.True(await TestAsyncHelpers.PollUntilAsync(async () =>
            {
                var msgs = await transport.ReadAllFromTopicAsync<ReleaseInventory>(releaseTopic);
                if (msgs.Count > 0) releaseEnv = msgs[0];
                return releaseEnv is not null;
            }, TimeSpan.FromSeconds(3)));

            Assert.NotNull(releaseEnv);
            Assert.Equal("ord-seq-1", releaseEnv!.Payload.OrderId);
            Assert.Equal("res-100", releaseEnv.Payload.ReservationId);
            Assert.Equal("release-inventory", releaseEnv.Headers["talaria.saga.compensation_step"]);

            // OrderFailed MUST STILL NOT be published yet!
            Assert.Empty(await transport.ReadAllFromTopicAsync<OrderFailed>(failedTopic));

            // Now send InventoryReleased ack
            await using var releaseAckProducer = await transport.CreateProducerAsync<InventoryReleased>(releaseAckTopic, new ProducerOptions());
            await releaseAckProducer.ProduceAsync(new InventoryReleased("ord-seq-1"));

            // Now OrderFailed should be published and saga state purged
            var failedMessages = new List<MessageEnvelope<OrderFailed>>();
            Assert.True(await TestAsyncHelpers.PollUntilAsync(async () =>
            {
                failedMessages.AddRange(await transport.ReadAllFromTopicAsync<OrderFailed>(failedTopic));
                return failedMessages.Count == 1 && await stateStore.GetAsync("ord-seq-1") is null;
            }, TimeSpan.FromSeconds(3)));
        }
        finally
        {
            await listener.StopAsync(CancellationToken.None);
        }
    }

    // -------------------------------------------------------------------------
    // Scenario 2: Payment Succeeds Then Timeout Fires (State Preservation Regression)
    // -------------------------------------------------------------------------
    [Fact]
    public async Task Given_PaymentSucceedsBeforeTimeoutFires_When_TimeoutTriggersCompensate_Then_PreservesBothPaymentAndInventoryAndCompensatesBothInLifoOrder()
    {
        var transport = new InMemoryTransport();
        var outboxStore = new InMemoryOutboxStore();
        var stateStore = new InMemoryStateStore<OrderCompensationSagaState>(outboxStore);
        var deferralStore = new InMemoryDeferralStore();
        var registry = new SagaRegistry();

        const string refundTopic = "timeout-reg.payments.refund";
        const string refundAckTopic = "timeout-reg.payments.refunded";
        const string releaseTopic = "timeout-reg.inventory.release";
        const string releaseAckTopic = "timeout-reg.inventory.released";
        const string failedTopic = "timeout-reg.orders.failed";

        var config = new SagaConfigurator<OrderCompensationSagaState>(registry);
        config
            .CompensateStep<ReleaseInventory, InventoryReleased>(
                when: s => s.InventoryReserved,
                create: s => new ReleaseInventory(s.OrderId, s.ReservationId!),
                onCompensated: (s, _) => s.InventoryReserved = false,
                correlateBy: a => a.OrderId,
                commandTopic: releaseTopic,
                ackTopic: releaseAckTopic)
            .CompensateStep<RefundPayment, PaymentRefunded>(
                when: s => s.PaymentCharged,
                create: s => new RefundPayment(s.OrderId, s.ChargeId!),
                onCompensated: (s, _) => s.PaymentCharged = false,
                correlateBy: a => a.OrderId,
                commandTopic: refundTopic,
                ackTopic: refundAckTopic)
            .OnCompensationCompleted((state, ctx) =>
                Task.FromResult(ctx.Dispatch(new OrderFailed(state.OrderId, state.Status)).Complete()))
            .DispatchTo<OrderFailed>(failedTopic)
            .StartedBy<OrderPlaced>(
                "timeout-reg.orders.placed",
                (msg, ctx) =>
                {
                    ctx.RequestTimeout(new OrderPaymentTimeout(msg.OrderId), TimeSpan.FromMilliseconds(180));
                    return Task.FromResult(ctx.Transition(new OrderCompensationSagaState
                    {
                        OrderId = msg.OrderId,
                        InventoryReserved = true,
                        ReservationId = msg.ReservationId
                    }));
                },
                m => m.OrderId)
            .On<PaymentSucceeded>(
                "timeout-reg.payments.succeeded",
                (state, msg, ctx) =>
                {
                    state.PaymentCharged = true;
                    state.ChargeId = msg.ChargeId;
                    return Task.FromResult(ctx.Transition(state));
                },
                m => m.OrderId)
            .OnTimeout<OrderPaymentTimeout>(
                (state, _, ctx) =>
                {
                    state.Status = "TimedOut";
                    return Task.FromResult(ctx.Compensate(state));
                },
                m => m.OrderId);
        config.Complete();

        var services = new ServiceCollection()
            .AddSingleton<ITransport>(transport)
            .AddSingleton<IOutboxStore>(outboxStore)
            .AddSingleton<IStateStore<OrderCompensationSagaState>>(stateStore)
            .AddSingleton<IDeferralStore>(deferralStore)
            .BuildServiceProvider();

        var listener = new TalariaListener(
            transport,
            new TopicRegistry(),
            registry,
            new TalariaOptions
            {
                ApplicationName = "timeout-reg-app",
                OutboxRelayInterval = TimeSpan.FromMilliseconds(15),
                DeferralBackoff = TimeSpan.FromMilliseconds(20)
            },
            NullLogger<TalariaListener>.Instance,
            services);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await listener.StartAsync(cts.Token);

        try
        {
            await using var placedProducer = await transport.CreateProducerAsync<OrderPlaced>("timeout-reg.orders.placed", new ProducerOptions());
            await placedProducer.ProduceAsync(new OrderPlaced("ord-to-1", "res-to-1"));
            Assert.True(await TestAsyncHelpers.PollUntilAsync(async () => (await stateStore.GetAsync("ord-to-1"))?.InventoryReserved == true, TimeSpan.FromSeconds(3)));

            await using var payProducer = await transport.CreateProducerAsync<PaymentSucceeded>("timeout-reg.payments.succeeded", new ProducerOptions());
            await payProducer.ProduceAsync(new PaymentSucceeded("ord-to-1", "chg-to-1"));
            Assert.True(await TestAsyncHelpers.PollUntilAsync(async () => (await stateStore.GetAsync("ord-to-1"))?.PaymentCharged == true, TimeSpan.FromSeconds(3)));

            // Wait for the 180ms timeout to fire and trigger RefundPayment first (since PaymentCharged was preserved!)
            var refunds = new List<MessageEnvelope<RefundPayment>>();
            Assert.True(await TestAsyncHelpers.PollUntilAsync(async () =>
            {
                refunds.AddRange(await transport.ReadAllFromTopicAsync<RefundPayment>(refundTopic));
                return refunds.Count == 1;
            }, TimeSpan.FromSeconds(5)));

            Assert.Empty(await transport.ReadAllFromTopicAsync<ReleaseInventory>(releaseTopic));

            await using var refundAckProducer = await transport.CreateProducerAsync<PaymentRefunded>(refundAckTopic, new ProducerOptions());
            await refundAckProducer.ProduceAsync(new PaymentRefunded("ord-to-1"));

            var releases = new List<MessageEnvelope<ReleaseInventory>>();
            Assert.True(await TestAsyncHelpers.PollUntilAsync(async () =>
            {
                releases.AddRange(await transport.ReadAllFromTopicAsync<ReleaseInventory>(releaseTopic));
                return releases.Count == 1;
            }, TimeSpan.FromSeconds(3)));

            await using var releaseAckProducer = await transport.CreateProducerAsync<InventoryReleased>(releaseAckTopic, new ProducerOptions());
            await releaseAckProducer.ProduceAsync(new InventoryReleased("ord-to-1"));

            var failures = new List<MessageEnvelope<OrderFailed>>();
            Assert.True(await TestAsyncHelpers.PollUntilAsync(async () =>
            {
                failures.AddRange(await transport.ReadAllFromTopicAsync<OrderFailed>(failedTopic));
                return failures.Count == 1 && await stateStore.GetAsync("ord-to-1") is null;
            }, TimeSpan.FromSeconds(3)));
        }
        finally
        {
            await listener.StopAsync(CancellationToken.None);
        }
    }

    // -------------------------------------------------------------------------
    // Scenario 3: Branching / Recorded Chronological History (RecordStep)
    // -------------------------------------------------------------------------
    [Fact]
    public async Task Given_SagaRecordingDynamicStepOrderViaRecordStep_When_Compensating_Then_ExecutesInReverseChronologicalRecordedOrderRatherThanRegistrationOrder()
    {
        var transport = new InMemoryTransport();
        var outboxStore = new InMemoryOutboxStore();
        var stateStore = new InMemoryStateStore<OrderCompensationSagaState>(outboxStore);
        var deferralStore = new InMemoryDeferralStore();
        var registry = new SagaRegistry();

        const string refundTopic = "branch.payments.refund";
        const string refundAckTopic = "branch.payments.refunded";
        const string releaseTopic = "branch.inventory.release";
        const string releaseAckTopic = "branch.inventory.released";

        // Note: RefundPayment is registered FIRST and ReleaseInventory is registered SECOND.
        // Without RecordStep, reverse registration order would execute ReleaseInventory first, then RefundPayment.
        // However, at runtime the saga executes "reserve-inventory" FIRST, then "charge-payment" SECOND via RecordStep!
        // Therefore, reverse chronological order MUST execute "charge-payment" (RefundPayment) FIRST, then "reserve-inventory" (ReleaseInventory) SECOND!
        var config = new SagaConfigurator<OrderCompensationSagaState>(registry);
        config
            .CompensateStep<RefundPayment, PaymentRefunded>(
                when: s => s.PaymentCharged,
                create: s => new RefundPayment(s.OrderId, s.ChargeId!),
                onCompensated: (s, _) => s.PaymentCharged = false,
                correlateBy: a => a.OrderId,
                stepName: "charge-payment",
                commandTopic: refundTopic,
                ackTopic: refundAckTopic)
            .CompensateStep<ReleaseInventory, InventoryReleased>(
                when: s => s.InventoryReserved,
                create: s => new ReleaseInventory(s.OrderId, s.ReservationId!),
                onCompensated: (s, _) => s.InventoryReserved = false,
                correlateBy: a => a.OrderId,
                stepName: "reserve-inventory",
                commandTopic: releaseTopic,
                ackTopic: releaseAckTopic)
            .StartedBy<OrderPlaced>(
                "branch.orders.placed",
                (msg, ctx) => Task.FromResult(ctx
                    .RecordStep("reserve-inventory")
                    .Transition(new OrderCompensationSagaState
                    {
                        OrderId = msg.OrderId,
                        InventoryReserved = true,
                        ReservationId = msg.ReservationId
                    })),
                m => m.OrderId)
            .On<PaymentSucceeded>(
                "branch.payments.succeeded",
                (state, msg, ctx) =>
                {
                    state.PaymentCharged = true;
                    state.ChargeId = msg.ChargeId;
                    return Task.FromResult(ctx
                        .RecordStep("charge-payment")
                        .Transition(state));
                },
                m => m.OrderId)
            .On<ShipmentBookingFailed>(
                "branch.shipping.failed",
                (state, _, ctx) => Task.FromResult(ctx.Compensate(state)),
                m => m.OrderId);
        config.Complete();

        var services = new ServiceCollection()
            .AddSingleton<ITransport>(transport)
            .AddSingleton<IOutboxStore>(outboxStore)
            .AddSingleton<IStateStore<OrderCompensationSagaState>>(stateStore)
            .AddSingleton<IDeferralStore>(deferralStore)
            .BuildServiceProvider();

        var listener = new TalariaListener(
            transport,
            new TopicRegistry(),
            registry,
            new TalariaOptions
            {
                ApplicationName = "branch-app",
                OutboxRelayInterval = TimeSpan.FromMilliseconds(15),
                DeferralBackoff = TimeSpan.FromMilliseconds(20)
            },
            NullLogger<TalariaListener>.Instance,
            services);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await listener.StartAsync(cts.Token);

        try
        {
            await using var placedProducer = await transport.CreateProducerAsync<OrderPlaced>("branch.orders.placed", new ProducerOptions());
            await placedProducer.ProduceAsync(new OrderPlaced("ord-branch-1", "res-b1"));
            Assert.True(await TestAsyncHelpers.PollUntilAsync(async () => (await stateStore.GetAsync("ord-branch-1"))?.InventoryReserved == true, TimeSpan.FromSeconds(3)));

            await using var payProducer = await transport.CreateProducerAsync<PaymentSucceeded>("branch.payments.succeeded", new ProducerOptions());
            await payProducer.ProduceAsync(new PaymentSucceeded("ord-branch-1", "chg-b1"));
            Assert.True(await TestAsyncHelpers.PollUntilAsync(async () => (await stateStore.GetAsync("ord-branch-1"))?.PaymentCharged == true, TimeSpan.FromSeconds(3)));

            await using var failProducer = await transport.CreateProducerAsync<ShipmentBookingFailed>("branch.shipping.failed", new ProducerOptions());
            await failProducer.ProduceAsync(new ShipmentBookingFailed("ord-branch-1", "Failed"));

            // Because "charge-payment" was recorded AFTER "reserve-inventory", RefundPayment must be dispatched FIRST!
            var refunds = new List<MessageEnvelope<RefundPayment>>();
            Assert.True(await TestAsyncHelpers.PollUntilAsync(async () =>
            {
                refunds.AddRange(await transport.ReadAllFromTopicAsync<RefundPayment>(refundTopic));
                return refunds.Count == 1;
            }, TimeSpan.FromSeconds(3)));
            Assert.Empty(await transport.ReadAllFromTopicAsync<ReleaseInventory>(releaseTopic));

            await using var refundAckProducer = await transport.CreateProducerAsync<PaymentRefunded>(refundAckTopic, new ProducerOptions());
            await refundAckProducer.ProduceAsync(new PaymentRefunded("ord-branch-1"));

            var releases = new List<MessageEnvelope<ReleaseInventory>>();
            Assert.True(await TestAsyncHelpers.PollUntilAsync(async () =>
            {
                releases.AddRange(await transport.ReadAllFromTopicAsync<ReleaseInventory>(releaseTopic));
                return releases.Count == 1;
            }, TimeSpan.FromSeconds(3)));
        }
        finally
        {
            await listener.StopAsync(CancellationToken.None);
        }
    }

    // -------------------------------------------------------------------------
    // Scenario 4: Duplicate Failure Trigger & Stale/Duplicate Acknowledgment Fencing
    // -------------------------------------------------------------------------
    [Fact]
    public async Task Given_ActiveSequentialCompensation_When_DuplicateFailureTriggerOrStaleAckArrives_Then_SuppressedWithoutDuplicateCommands()
    {
        var transport = new InMemoryTransport();
        var outboxStore = new InMemoryOutboxStore();
        var stateStore = new InMemoryStateStore<OrderCompensationSagaState>(outboxStore);
        var deferralStore = new InMemoryDeferralStore();
        var registry = new SagaRegistry();

        const string refundTopic = "fence.payments.refund";
        const string refundAckTopic = "fence.payments.refunded";
        const string releaseTopic = "fence.inventory.release";
        const string releaseAckTopic = "fence.inventory.released";

        var config = new SagaConfigurator<OrderCompensationSagaState>(registry);
        config
            .CompensateStep<ReleaseInventory, InventoryReleased>(
                when: s => s.InventoryReserved,
                create: s => new ReleaseInventory(s.OrderId, s.ReservationId!),
                onCompensated: (s, _) => s.InventoryReserved = false,
                correlateBy: a => a.OrderId,
                operationIdSelector: a => a.OperationId,
                stepName: "release-inventory",
                commandTopic: releaseTopic,
                ackTopic: releaseAckTopic)
            .CompensateStep<RefundPayment, PaymentRefunded>(
                when: s => s.PaymentCharged,
                create: s => new RefundPayment(s.OrderId, s.ChargeId!),
                onCompensated: (s, _) => s.PaymentCharged = false,
                correlateBy: a => a.OrderId,
                operationIdSelector: a => a.OperationId,
                stepName: "refund-payment",
                commandTopic: refundTopic,
                ackTopic: refundAckTopic)
            .StartedBy<OrderPlaced>(
                "fence.orders.placed",
                (msg, ctx) => Task.FromResult(ctx.Transition(new OrderCompensationSagaState
                {
                    OrderId = msg.OrderId,
                    InventoryReserved = true,
                    ReservationId = msg.ReservationId,
                    PaymentCharged = true,
                    ChargeId = "chg-fence-1"
                })),
                m => m.OrderId)
            .On<ShipmentBookingFailed>(
                "fence.shipping.failed",
                (state, _, ctx) => Task.FromResult(ctx.Compensate(state)),
                m => m.OrderId)
            .On<SecondaryFailureTrigger>(
                "fence.secondary.failed",
                (state, _, ctx) => Task.FromResult(ctx.Compensate(state)),
                m => m.OrderId);
        config.Complete();

        var services = new ServiceCollection()
            .AddSingleton<ITransport>(transport)
            .AddSingleton<IOutboxStore>(outboxStore)
            .AddSingleton<IStateStore<OrderCompensationSagaState>>(stateStore)
            .AddSingleton<IDeferralStore>(deferralStore)
            .BuildServiceProvider();

        var listener = new TalariaListener(
            transport,
            new TopicRegistry(),
            registry,
            new TalariaOptions
            {
                ApplicationName = "fence-app",
                OutboxRelayInterval = TimeSpan.FromMilliseconds(15),
                DeferralBackoff = TimeSpan.FromMilliseconds(20)
            },
            NullLogger<TalariaListener>.Instance,
            services);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await listener.StartAsync(cts.Token);

        try
        {
            await using var placedProducer = await transport.CreateProducerAsync<OrderPlaced>("fence.orders.placed", new ProducerOptions());
            await placedProducer.ProduceAsync(new OrderPlaced("ord-fence-1", "res-fence-1"));
            Assert.True(await TestAsyncHelpers.PollUntilAsync(async () => await stateStore.GetAsync("ord-fence-1") is not null, TimeSpan.FromSeconds(3)));

            await using var failProducer1 = await transport.CreateProducerAsync<ShipmentBookingFailed>("fence.shipping.failed", new ProducerOptions());
            await failProducer1.ProduceAsync(new ShipmentBookingFailed("ord-fence-1", "PrimaryFail"));

            var refunds = new List<MessageEnvelope<RefundPayment>>();
            var releases = new List<MessageEnvelope<ReleaseInventory>>();

            Assert.True(await TestAsyncHelpers.PollUntilAsync(async () =>
            {
                refunds.AddRange(await transport.ReadAllFromTopicAsync<RefundPayment>(refundTopic));
                return refunds.Count == 1;
            }, TimeSpan.FromSeconds(3)));

            // 1. Send a second failure trigger while already compensating -> must NOT emit a second RefundPayment!
            await using var failProducer2 = await transport.CreateProducerAsync<SecondaryFailureTrigger>("fence.secondary.failed", new ProducerOptions());
            await failProducer2.ProduceAsync(new SecondaryFailureTrigger("ord-fence-1", "DuplicateFail"));

            // 2. Send an out-of-order InventoryReleased ack while RefundPayment is the active PendingSteps[0] -> must be ignored!
            await using var releaseAckProducer = await transport.CreateProducerAsync<InventoryReleased>(releaseAckTopic, new ProducerOptions());
            await releaseAckProducer.ProduceAsync(new InventoryReleased("ord-fence-1"));

            // 3. Send a PaymentRefunded ack with a mismatched OperationId -> must be ignored!
            await using var refundAckProducer = await transport.CreateProducerAsync<PaymentRefunded>(refundAckTopic, new ProducerOptions());
            await refundAckProducer.ProduceAsync(new PaymentRefunded("ord-fence-1", OperationId: "stale-wrong-op-id"));

            var stayedOnFirstStep = await TestAsyncHelpers.PollStableAsync(async () =>
            {
                refunds.AddRange(await transport.ReadAllFromTopicAsync<RefundPayment>(refundTopic));
                releases.AddRange(await transport.ReadAllFromTopicAsync<ReleaseInventory>(releaseTopic));
                var s = await stateStore.GetAsync("ord-fence-1");
                return refunds.Count == 1 && releases.Count == 0 && s?.Compensation.PendingSteps.Count == 2;
            }, TimeSpan.FromMilliseconds(150));
            Assert.True(stayedOnFirstStep);

            // 4. Now send the matching PaymentRefunded ack (using the operation ID from the dispatched RefundPayment header)
            var dispatchedRefund = Assert.Single(refunds);
            var validOpId = dispatchedRefund.Headers["talaria.saga.compensation_operation_id"];
            await refundAckProducer.ProduceAsync(new PaymentRefunded("ord-fence-1", OperationId: validOpId));

            Assert.True(await TestAsyncHelpers.PollUntilAsync(async () =>
            {
                releases.AddRange(await transport.ReadAllFromTopicAsync<ReleaseInventory>(releaseTopic));
                return releases.Count == 1;
            }, TimeSpan.FromSeconds(3)));

            // 5. Send a duplicate PaymentRefunded ack -> must be ignored!
            await refundAckProducer.ProduceAsync(new PaymentRefunded("ord-fence-1", OperationId: validOpId));

            var singleRelease = await TestAsyncHelpers.PollStableAsync(async () =>
            {
                releases.AddRange(await transport.ReadAllFromTopicAsync<ReleaseInventory>(releaseTopic));
                return releases.Count == 1;
            }, TimeSpan.FromMilliseconds(120));
            Assert.True(singleRelease);
        }
        finally
        {
            await listener.StopAsync(CancellationToken.None);
        }
    }

    // -------------------------------------------------------------------------
    // Scenario 5: Late Forward Message Ignored + Metric vs. OnLateArrival Hook
    // -------------------------------------------------------------------------
    [Fact]
    public async Task Given_CompensatingSaga_When_UnconfiguredForwardStepArrives_Then_IsBlockedAndIncrementsLateMessageIgnoredMetric()
    {
        var transport = new InMemoryTransport();
        var outboxStore = new InMemoryOutboxStore();
        var stateStore = new InMemoryStateStore<OrderCompensationSagaState>(outboxStore);
        var deferralStore = new InMemoryDeferralStore();
        var registry = new SagaRegistry();

        var ignoredMetricCount = 0L;
        using var meterListener = new MeterListener();
        meterListener.InstrumentPublished = (instrument, listener) =>
        {
            if (instrument.Meter.Name == TalariaDiagnostics.MeterName
                && instrument.Name == "talaria.saga.compensation.late_message_ignored")
            {
                listener.EnableMeasurementEvents(instrument);
            }
        };
        meterListener.SetMeasurementEventCallback<long>((_, measurement, tags, _) =>
        {
            foreach (var tag in tags)
            {
                if (tag.Key == "saga.type" && Equals(tag.Value, nameof(OrderCompensationSagaState)))
                {
                    Interlocked.Add(ref ignoredMetricCount, measurement);
                }
            }
        });
        meterListener.Start();

        var forwardShipmentHandlerInvocations = 0;

        var config = new SagaConfigurator<OrderCompensationSagaState>(registry);
        config
            .CompensateStep<ReleaseInventory, InventoryReleased>(
                when: s => s.InventoryReserved,
                create: s => new ReleaseInventory(s.OrderId, s.ReservationId!),
                onCompensated: (s, _) => s.InventoryReserved = false,
                correlateBy: a => a.OrderId,
                commandTopic: "late-ignore.inventory.release",
                ackTopic: "late-ignore.inventory.released")
            .StartedBy<OrderPlaced>(
                "late-ignore.orders.placed",
                (msg, ctx) => Task.FromResult(ctx.Transition(new OrderCompensationSagaState
                {
                    OrderId = msg.OrderId,
                    InventoryReserved = true,
                    ReservationId = msg.ReservationId
                })),
                m => m.OrderId)
            .On<ShipmentBookedEvent>(
                "late-ignore.shipping.booked",
                (state, msg, ctx) =>
                {
                    Interlocked.Increment(ref forwardShipmentHandlerInvocations);
                    state.ShipmentBooked = true;
                    state.ShipmentId = msg.ShipmentId;
                    return Task.FromResult(ctx.Transition(state));
                },
                m => m.OrderId)
            .On<ShipmentBookingFailed>(
                "late-ignore.shipping.failed",
                (state, _, ctx) => Task.FromResult(ctx.Compensate(state)),
                m => m.OrderId);
        config.Complete();

        var services = new ServiceCollection()
            .AddSingleton<ITransport>(transport)
            .AddSingleton<IOutboxStore>(outboxStore)
            .AddSingleton<IStateStore<OrderCompensationSagaState>>(stateStore)
            .AddSingleton<IDeferralStore>(deferralStore)
            .BuildServiceProvider();

        var listener = new TalariaListener(
            transport,
            new TopicRegistry(),
            registry,
            new TalariaOptions
            {
                ApplicationName = "late-ignore-app",
                OutboxRelayInterval = TimeSpan.FromMilliseconds(15),
                DeferralBackoff = TimeSpan.FromMilliseconds(20)
            },
            NullLogger<TalariaListener>.Instance,
            services);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await listener.StartAsync(cts.Token);

        try
        {
            await using var placedProducer = await transport.CreateProducerAsync<OrderPlaced>("late-ignore.orders.placed", new ProducerOptions());
            await placedProducer.ProduceAsync(new OrderPlaced("ord-late-ig-1", "res-1"));
            Assert.True(await TestAsyncHelpers.PollUntilAsync(async () => (await stateStore.GetAsync("ord-late-ig-1"))?.InventoryReserved == true, TimeSpan.FromSeconds(3)));

            await using var failProducer = await transport.CreateProducerAsync<ShipmentBookingFailed>("late-ignore.shipping.failed", new ProducerOptions());
            await failProducer.ProduceAsync(new ShipmentBookingFailed("ord-late-ig-1", "Failed"));
            Assert.True(await TestAsyncHelpers.PollUntilAsync(
                async () => (await stateStore.GetAsync("ord-late-ig-1"))?.Compensation.Phase == SagaPhase.Compensating,
                TimeSpan.FromSeconds(3)));

            // Publish an unconfigured forward ShipmentBookedEvent while the saga is already Compensating
            await using var bookedProducer = await transport.CreateProducerAsync<ShipmentBookedEvent>("late-ignore.shipping.booked", new ProducerOptions());
            await bookedProducer.ProduceAsync(new ShipmentBookedEvent("ord-late-ig-1", "ship-999"));

            Assert.True(await TestAsyncHelpers.PollUntilAsync(
                () => Task.FromResult(Interlocked.Read(ref ignoredMetricCount) >= 1),
                TimeSpan.FromSeconds(3)));

            Assert.Equal(0, Volatile.Read(ref forwardShipmentHandlerInvocations));
            var currentState = await stateStore.GetAsync("ord-late-ig-1");
            Assert.NotNull(currentState);
            Assert.False(currentState!.ShipmentBooked);
        }
        finally
        {
            await listener.StopAsync(CancellationToken.None);
        }
    }

    // -------------------------------------------------------------------------
    // Scenario 6: Factory Exception Atomicity
    // -------------------------------------------------------------------------
    [Fact]
    public async Task Given_CompensationFactoryThrowsException_When_CompensateTriggered_Then_SagaStateAndOutboxRemainUnchanged()
    {
        var transport = new InMemoryTransport();
        var outboxStore = new InMemoryOutboxStore();
        var stateStore = new InMemoryStateStore<OrderCompensationSagaState>(outboxStore);
        var deferralStore = new InMemoryDeferralStore();
        var registry = new SagaRegistry();

        const string releaseTopic = "atomicity.inventory.release";
        const string releaseAckTopic = "atomicity.inventory.released";
        const string refundTopic = "atomicity.payments.refund";
        const string refundAckTopic = "atomicity.payments.refunded";

        var config = new SagaConfigurator<OrderCompensationSagaState>(registry);
        config
            .CompensateStep<ReleaseInventory, InventoryReleased>(
                when: s => s.InventoryReserved,
                create: s => new ReleaseInventory(s.OrderId, s.ReservationId!),
                correlateBy: a => a.OrderId,
                commandTopic: releaseTopic,
                ackTopic: releaseAckTopic)
            .CompensateStep<RefundPayment, PaymentRefunded>(
                when: s => s.PaymentCharged,
                create: _ => throw new InvalidOperationException("Simulated compensation factory failure"),
                correlateBy: a => a.OrderId,
                commandTopic: refundTopic,
                ackTopic: refundAckTopic)
            .StartedBy<OrderPlaced>(
                "atomicity.orders.placed",
                (msg, ctx) => Task.FromResult(ctx.Transition(new OrderCompensationSagaState
                {
                    OrderId = msg.OrderId,
                    InventoryReserved = true,
                    ReservationId = msg.ReservationId,
                    PaymentCharged = true,
                    ChargeId = "chg-atom-1",
                    Status = "ActiveForward"
                })),
                m => m.OrderId)
            .On<ShipmentBookingFailed>(
                "atomicity.shipping.failed",
                (state, _, ctx) =>
                {
                    state.Status = "ShouldRollBack";
                    return Task.FromResult(ctx.Compensate(state));
                },
                m => m.OrderId);
        config.Complete();

        var services = new ServiceCollection()
            .AddSingleton<ITransport>(transport)
            .AddSingleton<IOutboxStore>(outboxStore)
            .AddSingleton<IStateStore<OrderCompensationSagaState>>(stateStore)
            .AddSingleton<IDeferralStore>(deferralStore)
            .BuildServiceProvider();

        var listener = new TalariaListener(
            transport,
            new TopicRegistry(),
            registry,
            new TalariaOptions
            {
                ApplicationName = "atomicity-app",
                OutboxRelayInterval = TimeSpan.FromMilliseconds(15),
                DeferralBackoff = TimeSpan.FromMilliseconds(20)
            },
            NullLogger<TalariaListener>.Instance,
            services);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await listener.StartAsync(cts.Token);

        try
        {
            await using var placedProducer = await transport.CreateProducerAsync<OrderPlaced>("atomicity.orders.placed", new ProducerOptions());
            await placedProducer.ProduceAsync(new OrderPlaced("ord-atom-1", "res-atom-1"));
            Assert.True(await TestAsyncHelpers.PollUntilAsync(async () => (await stateStore.GetAsync("ord-atom-1"))?.Status == "ActiveForward", TimeSpan.FromSeconds(3)));

            await using var failProducer = await transport.CreateProducerAsync<ShipmentBookingFailed>("atomicity.shipping.failed", new ProducerOptions());
            await failProducer.ProduceAsync(new ShipmentBookingFailed("ord-atom-1", "Boom"));

            // Wait for the failing message to be routed to the DLQ
            var dlqMessages = new List<MessageEnvelope<ShipmentBookingFailed>>();
            Assert.True(await TestAsyncHelpers.PollUntilAsync(async () =>
            {
                dlqMessages.AddRange(await transport.ReadAllFromTopicAsync<ShipmentBookingFailed>("atomicity.shipping.failed.dlq"));
                return dlqMessages.Count == 1;
            }, TimeSpan.FromSeconds(5)));

            // State and outbox must be completely unchanged!
            var storedState = await stateStore.GetAsync("ord-atom-1");
            Assert.NotNull(storedState);
            Assert.Equal("ActiveForward", storedState!.Status);
            Assert.Equal(SagaPhase.Forward, storedState.Compensation.Phase);
            Assert.Empty(storedState.Compensation.PendingSteps);
            Assert.Equal(0, outboxStore.Count);
            Assert.Empty(await transport.ReadAllFromTopicAsync<ReleaseInventory>(releaseTopic));
            Assert.Empty(await transport.ReadAllFromTopicAsync<RefundPayment>(refundTopic));
        }
        finally
        {
            await listener.StopAsync(CancellationToken.None);
        }
    }

    // -------------------------------------------------------------------------
    // Scenario 7: Binary/Deconstruction & Custom ISagaContext Compatibility
    // -------------------------------------------------------------------------
    private sealed class CustomMinimalSagaContext<TState> : ISagaContext<TState>
    {
        public CancellationToken CancellationToken => CancellationToken.None;
        public ISagaContext<TState> Dispatch<TMessage>(TMessage message) where TMessage : class => this;
        public SagaResult<TState> Complete() => SagaResult<TState>.Complete([]);
        public SagaResult<TState> Transition(TState newState) => SagaResult<TState>.Transition(newState, []);
        public SagaResult<TState> Defer() => SagaResult<TState>.Defer();
    }

    [Fact]
    public void Given_SagaScheduledDispatchAndCustomISagaContext_When_Using3ArgCtorDeconstructionAndDefaultInterfaceMethods_Then_PreservesCompatibility()
    {
        var payload = new ReleaseInventory("ord-1", "res-1");
        var now = DateTimeOffset.UtcNow;

        // 3-parameter positional constructor and 3-tuple deconstruction
        var dispatch = new SagaScheduledDispatch(payload, now, IsTimeout: false)
        {
            IsCompensation = true,
            CompensationStepName = "release-inventory",
            CompensationOperationId = "op-123",
            CompensationGeneration = 1
        };

        var (msg, scheduledAt, isTimeout) = dispatch;
        Assert.Same(payload, msg);
        Assert.Equal(now, scheduledAt);
        Assert.False(isTimeout);
        Assert.True(dispatch.IsCompensation);
        Assert.Equal("release-inventory", dispatch.CompensationStepName);
        Assert.Equal("op-123", dispatch.CompensationOperationId);
        Assert.Equal(1, dispatch.CompensationGeneration);

        // Custom ISagaContext implementation compiles without implementing RecordStep/Compensate/CompensateAndComplete
        ISagaContext<OrderCompensationSagaState> customCtx = new CustomMinimalSagaContext<OrderCompensationSagaState>();
        Assert.Throws<NotSupportedException>(() => customCtx.RecordStep("step-1"));
        Assert.Throws<NotSupportedException>(() => customCtx.Compensate(new OrderCompensationSagaState()));
        Assert.Throws<NotSupportedException>(() => customCtx.CompensateAndComplete());
    }
}
