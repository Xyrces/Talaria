using Talaria.Testing;

public sealed class MinimalMessagingHarnessTests
{
    [Fact]
    public async Task Sends_command_through_a_real_host_and_observes_handler()
    {
        await using var app = TalariaTestHost.Create()
            .MapCommand<PlaceOrder>();
        await app.StartAsync();
        var expected = new PlaceOrder(Guid.NewGuid());
        await app.SendAsync(expected);
        var received = await app.AssertReceivedAsync<PlaceOrder>(x => x.OrderId == expected.OrderId, TimeSpan.FromSeconds(3));
        Assert.Equal(expected, received);
    }

    private sealed record PlaceOrder(Guid OrderId);

    [Fact]
    public async Task Does_not_report_success_before_the_handler_finishes()
    {
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var finish = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var app = TalariaTestHost.Create().MapCommand<PlaceOrder>(async (_, ct) =>
        {
            entered.TrySetResult();
            await finish.Task.WaitAsync(ct);
        });
        await app.StartAsync();
        await app.SendAsync(new PlaceOrder(Guid.NewGuid()));
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(3));
        try
        {
            await Assert.ThrowsAsync<TimeoutException>(() => app.ReceiveAsync<PlaceOrder>(TimeSpan.FromMilliseconds(30)));
        }
        finally { finish.TrySetResult(); }
        await app.ReceiveAsync<PlaceOrder>(TimeSpan.FromSeconds(3));
    }

    [Fact]
    public async Task Given_MultipleHandledMessages_When_AssertReceivedMatchesOutOfOrder_Then_EarlierMessagesAreNotDiscarded()
    {
        await using var app = TalariaTestHost.Create().MapCommand<PlaceOrder>();
        await app.StartAsync();
        var first = new PlaceOrder(Guid.NewGuid());
        var second = new PlaceOrder(Guid.NewGuid());

        await app.SendAsync(first);
        await app.SendAsync(second);

        var matchedSecond = await app.AssertReceivedAsync<PlaceOrder>(x => x.OrderId == second.OrderId, TimeSpan.FromSeconds(3));
        var matchedFirst = await app.AssertReceivedAsync<PlaceOrder>(x => x.OrderId == first.OrderId, TimeSpan.FromSeconds(3));

        Assert.Equal(second, matchedSecond);
        Assert.Equal(first, matchedFirst);
    }

    [Fact]
    public async Task Given_ConcurrentAssertReceivedAsyncCalls_When_SecondMatchingMessageArrivesFirst_Then_SecondWaiterCompletesWithoutBlockingOnFirstWaiter()
    {
        await using var app = TalariaTestHost.Create().MapCommand<PlaceOrder>();
        await app.StartAsync();
        var orderA = new PlaceOrder(Guid.NewGuid());
        var orderB = new PlaceOrder(Guid.NewGuid());

        var waitForA = app.AssertReceivedAsync<PlaceOrder>(x => x.OrderId == orderA.OrderId, TimeSpan.FromSeconds(5));
        var waitForB = app.AssertReceivedAsync<PlaceOrder>(x => x.OrderId == orderB.OrderId, TimeSpan.FromMilliseconds(200));

        await app.SendAsync(orderB);
        var receivedB = await waitForB;
        Assert.Equal(orderB, receivedB);
        Assert.False(waitForA.IsCompleted);

        await app.SendAsync(orderA);
        var receivedA = await waitForA;
        Assert.Equal(orderA, receivedA);
    }

    [Fact]
    public async Task Given_WaitingAssertionWithThrowingPredicate_When_MessageIsHandled_Then_ThrowingWaiterFaultsWithoutRoutingToDlqAndPreservesMessageForOtherAssertions()
    {
        var handlerCount = 0;
        var dlq = new Talaria.Transports.InMemory.InMemoryFailedMessageStore();
        await using var app = TalariaTestHost.Create(services =>
                Microsoft.Extensions.DependencyInjection.ServiceCollectionServiceExtensions
                    .AddSingleton<Talaria.Core.Abstractions.IFailedMessageStore>(services, dlq))
            .MapCommand<PlaceOrder>((_, _) =>
            {
                Interlocked.Increment(ref handlerCount);
                return Task.CompletedTask;
            });
        await app.StartAsync();
        var expected = new PlaceOrder(Guid.NewGuid());

        var faultingWait = app.AssertReceivedAsync<PlaceOrder>(
            _ => throw new InvalidOperationException("predicate boom"),
            TimeSpan.FromMilliseconds(250));
        var validWait = app.AssertReceivedAsync<PlaceOrder>(
            x => x.OrderId == expected.OrderId,
            TimeSpan.FromMilliseconds(250));

        await app.SendAsync(expected);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => faultingWait);
        Assert.Equal("predicate boom", ex.Message);

        var received = await validWait;
        Assert.Equal(expected, received);
        Assert.Equal(1, Volatile.Read(ref handlerCount));
        Assert.Empty(await dlq.ListAsync(10));
    }
}

