using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Talaria.Core.Abstractions;
using Talaria.Core.Registration;
using Talaria.Transports.InMemory;

var placed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddTalaria(options => options.ApplicationName = "quickstart")
    .UseInMemory();
using var app = builder.Build();
Func<PlaceOrder, IMessageBus, CancellationToken, Task> placeOrder = async (PlaceOrder command, IMessageBus bus, CancellationToken ct) =>
{
    Console.WriteLine($"Accepted order {command.OrderId}");
    await bus.PublishAsync(new OrderPlaced(command.OrderId), ct);
};
app.Services.MapCommand<PlaceOrder>(placeOrder);
Action<OrderPlaced> onPlaced = (OrderPlaced message) =>
{
    Console.WriteLine($"Order placed event: {message.OrderId}");
    placed.TrySetResult();
};
app.Services.MapEvent<OrderPlaced>(onPlaced);
await app.StartAsync();
await using (var scope = app.Services.CreateAsyncScope())
    await scope.ServiceProvider.GetRequiredService<IMessageBus>().SendAsync(new PlaceOrder(Guid.NewGuid()));
await placed.Task.WaitAsync(TimeSpan.FromSeconds(5));
await app.StopAsync();

public sealed record PlaceOrder(Guid OrderId);
public sealed record OrderPlaced(Guid OrderId);
