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

        var handler = reg.Registrations[0].Steps[0].Handler;
        await Assert.ThrowsAsync<InvalidOperationException>(() => handler(null!, new TestMessage(), new SagaContext<object>()));
    }
}
