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
}
