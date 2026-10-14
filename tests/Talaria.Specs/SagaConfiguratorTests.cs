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
}

