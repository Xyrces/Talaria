using System;
using System.Threading.Tasks;
using Talaria.Core.Sagas;
using Xunit;

namespace Talaria.Specs.Tests;

public class SagaConfiguratorTests
{
    private class TestState { public string Id { get; set; } = ""; }
    private class TestMessage { public string CorrelationId { get; set; } = "123"; }

    [Fact]
    public void Implies_CorrelationResolver_If_Omitted()
    {
        var reg = new SagaRegistry();
        var config = new SagaConfigurator<TestState>(reg);
        
        config.On<TestMessage>("topic1", handler: (state, msg, ctx) => Task.FromResult(ctx.Transition(state)));

        // Two-phase registration: nothing is published until Complete() runs.
        Assert.Empty(reg.Registrations);

        config.Complete();

        Assert.Single(reg.Registrations);
        Assert.Single(reg.Registrations[0].Steps);
        
        var resolver = reg.Registrations[0].Steps[0].CorrelationResolver;
        Assert.Null(resolver); // Because it relies on CorrelationResolver fallback when null is passed
    }

    [Fact]
    public async Task Throws_If_State_Is_Missing()
    {
        var reg = new SagaRegistry();
        var config = new SagaConfigurator<TestState>(reg);
        config.On<TestMessage>("test", (s, m, c) => Task.FromResult(c.Transition(s)));
        config.Complete();

        var handler = reg.Registrations[0].Steps[0].Handler;
        await Assert.ThrowsAsync<InvalidOperationException>(() => handler(null!, new TestMessage(), new SagaContext<object>()));
    }

    private sealed class TestTimeoutMessage
    {
        public string CorrelationId { get; set; } = "123";
    }

    [Fact]
    public void OnTimeout_WithoutTopic_UsesConventionTopicAndRegistersDispatchTopicAutomatically()
    {
        var reg = new SagaRegistry();
        var config = new SagaConfigurator<TestState>(reg);

        config.OnTimeout<TestTimeoutMessage>((state, _, ctx) => Task.FromResult(ctx.Transition(state)));
        config.Complete();

        var registration = Assert.Single(reg.Registrations);
        var step = Assert.Single(registration.Steps);
        var expectedTopic = Talaria.Core.Registration.MessageNames.Destination(
            Talaria.Core.Registration.MessageNames.Contract(typeof(TestTimeoutMessage)));

        Assert.Equal(expectedTopic, step.TopicName);
        Assert.Equal(typeof(TestTimeoutMessage), step.MessageType);
        Assert.False(step.IsStarter);
        Assert.True(step.IsTimeout);
        Assert.True(registration.DispatchTopics.TryGetValue(typeof(TestTimeoutMessage), out var dispatchTopic));
        Assert.Equal(expectedTopic, dispatchTopic);
    }

    [Fact]
    public void OnTimeout_WithExplicitTopic_UsesExplicitTopicInStepAndDispatchTopics()
    {
        var reg = new SagaRegistry();
        var config = new SagaConfigurator<TestState>(reg);

        config.OnTimeout<TestTimeoutMessage>(
            "custom-timeout-topic",
            (state, _, ctx) => Task.FromResult(ctx.Transition(state)),
            correlateBy: m => m.CorrelationId);
        config.Complete();

        var registration = Assert.Single(reg.Registrations);
        var step = Assert.Single(registration.Steps);

        Assert.Equal("custom-timeout-topic", step.TopicName);
        Assert.True(step.IsTimeout);
        Assert.False(step.IsStarter);
        Assert.NotNull(step.CorrelationResolver);
        Assert.Equal("custom-timeout-topic", registration.DispatchTopics[typeof(TestTimeoutMessage)]);
    }

    private sealed class CompConfigState : ICompensatableState
    {
        public string Id { get; set; } = string.Empty;
        public bool Step1Done { get; set; }
        public bool Step2Done { get; set; }
        public SagaCompensationState Compensation { get; set; } = new();
    }

    private sealed record UndoStep1(string Id);
    private sealed record Step1Undone(string Id);
    private sealed record UndoStep2(string Id);
    private sealed record Step2Undone(string Id);
    private sealed record LateEvent(string Id);
    private sealed record TriggerFail(string Id);
    private sealed record FinalFailedEvent(string Id);

    [Fact]
    public void CompensateStep_And_OnLateArrival_RegisterCompensationRulesAckStepsAndDispatchRoutes()
    {
        var reg = new SagaRegistry();
        var config = new SagaConfigurator<CompConfigState>(reg);

        config
            .CompensateStep<UndoStep1, Step1Undone>(
                when: s => s.Step1Done,
                create: s => new UndoStep1(s.Id),
                onCompensated: (s, _) => s.Step1Done = false,
                correlateBy: a => a.Id,
                stepName: "undo-1",
                commandTopic: "custom.undo1",
                ackTopic: "custom.undone1")
            .CompensateStep<UndoStep2, Step2Undone>(
                when: s => s.Step2Done,
                create: s => new UndoStep2(s.Id))
            .OnLateArrival<LateEvent>(
                (s, _) => s.Step2Done = true,
                topic: "custom.late",
                correlateBy: m => m.Id)
            .OnCompensationCompleted((_, ctx) => Task.FromResult(ctx.Complete()));

        config.Complete();

        var registration = Assert.Single(reg.Registrations);
        Assert.Equal(2, registration.Compensations.Count);
        Assert.True(registration.Compensations[0].IsSequential);
        Assert.Equal("undo-1", registration.Compensations[0].StepName);
        Assert.Equal("custom.undo1", registration.Compensations[0].CommandTopic);
        Assert.Equal("custom.undone1", registration.Compensations[0].AckTopic);
        Assert.Equal("custom.undo1", registration.DispatchTopics[typeof(UndoStep1)]);

        Assert.True(registration.Compensations[1].IsSequential);
        Assert.True(registration.DispatchTopics.ContainsKey(typeof(UndoStep2)));

        // Verify Ack steps and standalone LateArrival step were added
        Assert.Contains(registration.Steps, s => s.IsCompensationAck && s.MessageType == typeof(Step1Undone) && s.TopicName == "custom.undone1");
        Assert.Contains(registration.Steps, s => s.IsCompensationAck && s.MessageType == typeof(Step2Undone));
        Assert.Contains(registration.Steps, s => s.IsLateArrival && s.MessageType == typeof(LateEvent) && s.TopicName == "custom.late");
        Assert.NotNull(registration.OnCompensationCompleted);
    }

    [Fact]
    public async Task CompensateWith_AllOverloads_EvaluateInLifoOrderAndCancelConfiguredTimeoutsOnBatchCompensateAndComplete()
    {
        var reg = new SagaRegistry();
        var config = new SagaConfigurator<CompConfigState>(reg);

        config
            .CompensateWith<UndoStep1>("explicit.undo1", s => s.Step1Done, s => new UndoStep1(s.Id))
            .CompensateWith<UndoStep2>("explicit.undo2", s => s.Step2Done ? new UndoStep2(s.Id) : null)
            .DispatchTo<FinalFailedEvent>("explicit.failed")
            .OnTimeout<TestTimeoutMessage>((s, _, ctx) => Task.FromResult(ctx.Transition(s)))
            .On<TriggerFail>("explicit.fail", (state, msg, ctx) =>
                Task.FromResult(ctx
                    .Dispatch(new FinalFailedEvent(msg.Id))
                    .CompensateAndComplete(state)));

        config.Complete();

        var registration = Assert.Single(reg.Registrations);
        var failStep = Assert.Single(registration.Steps, s => s.MessageType == typeof(TriggerFail));

        var activeState = new CompConfigState { Id = "c-1", Step1Done = true, Step2Done = true };
        var result = await failStep.Handler(activeState, new TriggerFail("c-1"), new SagaContext<object>());

        Assert.True(result.IsCompleted);
        Assert.True(result.IsCompensating);
        Assert.Equal(3, result.Dispatches.Count);

        // LIFO compensation dispatches come first (UndoStep2 then UndoStep1), followed by explicit FinalFailedEvent
        Assert.IsType<UndoStep2>(result.Dispatches[0].Message);
        Assert.True(result.Dispatches[0].IsCompensation);
        Assert.IsType<UndoStep1>(result.Dispatches[1].Message);
        Assert.True(result.Dispatches[1].IsCompensation);
        Assert.IsType<FinalFailedEvent>(result.Dispatches[2].Message);
        Assert.False(result.Dispatches[2].IsCompensation);

        // Configured timeout type is automatically included in CanceledTimeouts
        Assert.Contains(typeof(TestTimeoutMessage), result.CanceledTimeouts);
    }
}

