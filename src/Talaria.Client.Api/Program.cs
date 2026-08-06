using Microsoft.AspNetCore.Mvc;
using Talaria.Client.Api.Sagas;
using Talaria.Core.Abstractions;
using Talaria.Core.Registration;
using Talaria.StateStores.Redis;
using Talaria.Transports.InMemory;
using Talaria.Transports.Kafka;

var builder = WebApplication.CreateBuilder(args);

// Add service defaults & Aspire components
builder.AddServiceDefaults();

var messagingProvider = builder.Configuration["Messaging:Provider"] ?? "Kafka";
var talaria = builder.Services.AddTalaria();

if (messagingProvider.Equals("InMemory", StringComparison.OrdinalIgnoreCase))
{
    talaria
        .UseInMemoryTransport()
        .UseInMemoryStateStore()
        .UseInMemoryIdempotencyStore();
}
else
{
    talaria
        .UseKafkaTransport(opts =>
        {
            opts.BootstrapServers = builder.Configuration.GetConnectionString("kafka") ?? "localhost:9092";
        })
        .UseRedisStateStore(opts =>
        {
            opts.Configuration = builder.Configuration.GetConnectionString("redis") ?? "localhost:6379";
            opts.KeyPrefix = "onboarding:";
        })
        .UseRedisIdempotencyStore(opts => 
        {
            opts.Configuration = builder.Configuration.GetConnectionString("redis") ?? "localhost:6379";
            opts.KeyPrefix = "onboarding:";
        });
}

var app = builder.Build();

// Configure the saga mappings
OnboardingSagaConfigurator.ConfigureOnboardingSaga(app.Services);

// Configure a stateless consumer mapping
app.Services.MapTopic<SendVerificationEmailCommand>("email-commands", async (msg, ct) =>
{
    Console.WriteLine($"[STATELESS CONSUMER] Intercepted email command for {msg.AccountId} -> sending to {msg.Email}...");
    await Task.Delay(500, ct); // simulate sending email
    Console.WriteLine($"[STATELESS CONSUMER] Email sent successfully!");
});

app.MapDefaultEndpoints();

// Simple API to trigger the saga
app.MapPost("/api/accounts", async (
    [FromBody] CreateAccountRequest request,
    [FromQuery] string? messageId,
    [FromQuery] string? accountId,
    [FromServices] ITransport transport) =>
{
    var determinedId = accountId ?? Guid.NewGuid().ToString("N");
    var headers = new MessageHeaders();
    
    if (!string.IsNullOrEmpty(messageId))
    {
        headers.MessageId = messageId;
    }
    
    // Simulate sending the command that the saga listens for
    var producer = await transport.CreateProducerAsync<CreateAccountCommand>("onboarding-commands", new ProducerOptions());
    await producer.ProduceAsync(new CreateAccountCommand
    {
        AccountId = determinedId,
        Email = request.Email
    }, headers: headers);

    return Results.Accepted($"/api/accounts/{determinedId}", new { AccountId = determinedId });
});

// Simple API to trigger verification event
app.MapPost("/api/accounts/{accountId}/verify", async (
    string accountId,
    [FromServices] ITransport transport) =>
{
    // Simulate external system verifying the account
    var producer = await transport.CreateProducerAsync<AccountVerifiedEvent>("account-events", new ProducerOptions());
    await producer.ProduceAsync(new AccountVerifiedEvent
    {
        AccountId = accountId
    });

    return Results.Ok(new { Verified = true });
});

app.Run();

public class CreateAccountRequest
{
    public string Email { get; set; } = string.Empty;
}
