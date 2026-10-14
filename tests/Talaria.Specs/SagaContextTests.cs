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

    private sealed class DummyTimeout
    {
        public int Generation { get; init; }
    }

    private sealed class FixedTimeProvider(DateTimeOffset now) : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => now;
    }

    [Fact]
    public void Dispatch_WithDelayAndDueAt_StagesScheduledDispatches()
    {
        var baseTime = new DateTimeOffset(2026, 10, 14, 10, 0, 0, TimeSpan.Zero);
        var ctx = new SagaContext<DummyState>(new FixedTimeProvider(baseTime));
        var msg1 = new DummyMessage();
        var msg2 = new DummyMessage();
        var dueAt2 = baseTime.AddMinutes(10);

        ctx.Dispatch(msg1, TimeSpan.FromMinutes(5))
           .Dispatch(msg2, dueAt2);

        var result = ctx.Transition(new DummyState());
        Assert.Equal(2, result.OutboundMessages.Count);
        Assert.Equal(2, result.Dispatches.Count);
        Assert.Equal(new SagaScheduledDispatch(msg1, baseTime.AddMinutes(5), IsTimeout: false), result.Dispatches[0]);
        Assert.Equal(new SagaScheduledDispatch(msg2, dueAt2, IsTimeout: false), result.Dispatches[1]);
    }

    [Fact]
    public void RequestTimeout_SupersedesEarlierTimeoutInSameStep_AndRecordsCanceledTimeoutType()
    {
        var baseTime = new DateTimeOffset(2026, 10, 14, 10, 0, 0, TimeSpan.Zero);
        var ctx = new SagaContext<DummyState>(new FixedTimeProvider(baseTime));
        var first = new DummyTimeout { Generation = 1 };
        var second = new DummyTimeout { Generation = 2 };

        ctx.RequestTimeout(first, TimeSpan.FromSeconds(10))
           .RequestTimeout(second, TimeSpan.FromSeconds(30));

        var result = ctx.Transition(new DummyState());
        var staged = Assert.Single(result.Dispatches);
        Assert.Same(second, staged.Message);
        Assert.True(staged.IsTimeout);
        Assert.Equal(baseTime.AddSeconds(30), staged.ScheduledAt);
        Assert.Same(second, Assert.Single(result.OutboundMessages));
        Assert.Equal(typeof(DummyTimeout), Assert.Single(result.CanceledTimeouts));
    }

    [Fact]
    public void CancelTimeout_RemovesStagedTimeoutInSameStep_AndRecordsCanceledTimeoutType()
    {
        var ctx = new SagaContext<DummyState>();
        ctx.RequestTimeout(new DummyTimeout { Generation = 1 }, TimeSpan.FromSeconds(15))
           .CancelTimeout<DummyTimeout>();

        var result = ctx.Transition(new DummyState());
        Assert.Empty(result.Dispatches);
        Assert.Empty(result.OutboundMessages);
        Assert.Equal(typeof(DummyTimeout), Assert.Single(result.CanceledTimeouts));
    }

    [Fact]
    public void RequestTimeout_AfterCancelTimeoutInSameStep_StagesNewTimeoutAndKeepsCancelReceiptForPriorVersions()
    {
        var dueAt = new DateTimeOffset(2026, 10, 14, 11, 0, 0, TimeSpan.Zero);
        var ctx = new SagaContext<DummyState>();
        var rearmed = new DummyTimeout { Generation = 2 };

        ctx.CancelTimeout<DummyTimeout>()
           .RequestTimeout(rearmed, dueAt);

        var result = ctx.Transition(new DummyState());
        var staged = Assert.Single(result.Dispatches);
        Assert.Same(rearmed, staged.Message);
        Assert.True(staged.IsTimeout);
        Assert.Equal(dueAt, staged.ScheduledAt);
        Assert.Equal(typeof(DummyTimeout), Assert.Single(result.CanceledTimeouts));
    }

    [Fact]
    public void Defer_DiscardsStagedDispatchesTimeoutsAndCancellations()
    {
        var ctx = new SagaContext<DummyState>();
        ctx.Dispatch(new DummyMessage(), TimeSpan.FromSeconds(5))
           .RequestTimeout(new DummyTimeout { Generation = 1 }, TimeSpan.FromSeconds(10))
           .CancelTimeout<DummyMessage>();

        var result = ctx.Defer();
        Assert.True(result.IsDeferred);
        Assert.Empty(result.OutboundMessages);
        Assert.Empty(result.Dispatches);
        Assert.Empty(result.CanceledTimeouts);
    }

    [Fact]
    public void Dispatch_And_RequestTimeout_ValidateNullAndNegativeDelay()
    {
        var ctx = new SagaContext<DummyState>();
        Assert.Throws<ArgumentNullException>(() => ctx.Dispatch<DummyMessage>(null!));
        Assert.Throws<ArgumentNullException>(() => ctx.Dispatch<DummyMessage>(null!, TimeSpan.FromSeconds(1)));
        Assert.Throws<ArgumentNullException>(() => ctx.Dispatch<DummyMessage>(null!, DateTimeOffset.UtcNow));
        Assert.Throws<ArgumentOutOfRangeException>(() => ctx.Dispatch(new DummyMessage(), TimeSpan.FromMilliseconds(-1)));

        Assert.Throws<ArgumentNullException>(() => ctx.RequestTimeout<DummyTimeout>(null!, TimeSpan.FromSeconds(1)));
        Assert.Throws<ArgumentNullException>(() => ctx.RequestTimeout<DummyTimeout>(null!, DateTimeOffset.UtcNow));
        Assert.Throws<ArgumentOutOfRangeException>(() => ctx.RequestTimeout(new DummyTimeout(), TimeSpan.FromTicks(-1)));
    }
}

