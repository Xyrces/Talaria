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
        .UseInMemoryIdempotencyStore()
        .UseInMemoryDeferralStore();
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
        })
        .UseRedisDeferralStore(opts =>
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
    var logger = app.Services.GetRequiredService<ILoggerFactory>().CreateLogger("EmailConsumer");
    logger.LogInformation("Intercepted email command for account {AccountId} — sending verification email...", msg.AccountId);
    await Task.Delay(500, ct); // simulate sending email
    logger.LogInformation("Verification email for account {AccountId} sent.", msg.AccountId);
});

app.MapDefaultEndpoints();

// Simple API to trigger the saga
app.MapPost("/api/accounts", async (
    [FromBody] CreateAccountRequest request,
    [FromQuery] string? accountId,
    [FromServices] ITransport transport) =>
{
    if (string.IsNullOrWhiteSpace(request.Email) || request.Email.Length > 254 || !request.Email.Contains('@'))
    {
        return Results.BadRequest(new { Error = "A valid email address is required." });
    }

    var determinedId = accountId ?? Guid.NewGuid().ToString("N");

    // The server always generates the message id — clients must not control dedup keys.

    // Simulate sending the command that the saga listens for
    var producer = await transport.CreateProducerAsync<CreateAccountCommand>("onboarding-commands", new ProducerOptions());
    await producer.ProduceAsync(new CreateAccountCommand
    {
        AccountId = determinedId,
        Email = request.Email
    });

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
    [System.ComponentModel.DataAnnotations.EmailAddress]
    [System.ComponentModel.DataAnnotations.MaxLength(254)]
    public string Email { get; set; } = string.Empty;
}
