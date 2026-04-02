using System.Linq;
using Talaria.Core.Sagas;
using Xunit;

namespace Talaria.Specs.Tests;

public class SagaContextTests
{
    private class DummyState { }
    private class DummyMessage { }

    [Fact]
    public void Dispatch_Adds_Outbound_Message()
    {
        var ctx = new SagaContext<DummyState>();
        var msg = new DummyMessage();

        ctx.Dispatch(msg);

        var result = ctx.Transition(new DummyState());
        Assert.Single(result.OutboundMessages);
        Assert.Equal(msg, result.OutboundMessages.First());
    }

    [Fact]
    public void Transition_Sets_State_And_Flags()
    {
        var ctx = new SagaContext<DummyState>();
        var state = new DummyState();

        var result = ctx.Transition(state);
        Assert.Equal(state, result.State);
        Assert.False(result.IsDeferred);
        Assert.False(result.IsCompleted);
    }

    [Fact]
    public void Complete_Sets_Complete_Flag()
    {
        var ctx = new SagaContext<DummyState>();
        
        var result = ctx.Complete();
        Assert.True(result.IsCompleted);
        Assert.False(result.IsDeferred);
    }

    [Fact]
    public void Defer_Sets_Deferred_Flag()
    {
        var ctx = new SagaContext<DummyState>();
        
        var result = ctx.Defer();
        Assert.True(result.IsDeferred);
        Assert.False(result.IsCompleted);
    }
}
