// SPDX-License-Identifier: Apache-2.0

using Microsoft.AspNetCore.Mvc;
using Talaria.Client.Api.Sagas;
using Talaria.Core.Abstractions;
using Talaria.Core.Registration;
using Talaria.StateStores.Redis;
using Talaria.Transports.AzureServiceBus;
using Talaria.Transports.InMemory;
using Talaria.Transports.Kafka;

var builder = WebApplication.CreateBuilder(args);

// Sample-API configuration. The literal defaults match the onboarding saga sample;
// override via IConfiguration (env vars, appsettings.json, or Aspire service discovery)
// when adapting the sample to a different tenant / environment.
var redisKeyPrefix = builder.Configuration["Talaria:Redis:KeyPrefix"] ?? "onboarding:";
var onboardingCommandsTopic = builder.Configuration["Talaria:Topics:OnboardingCommands"] ?? "onboarding-commands";
var accountEventsTopic = builder.Configuration["Talaria:Topics:AccountEvents"] ?? "account-events";
var emailCommandsTopic = builder.Configuration["Talaria:Topics:EmailCommands"] ?? "email-commands";

// Add service defaults & Aspire components
builder.AddServiceDefaults();

var messagingProvider = builder.Configuration["Messaging:Provider"] ?? "Kafka";
builder.Services.AddSingleton<Talaria.Client.Api.ProcessingTracker>();
var talaria = builder.Services.AddTalaria();

if (messagingProvider.Equals("InMemory", StringComparison.OrdinalIgnoreCase))
{
    talaria
        .UseInMemoryTransport()
        .UseInMemoryStateStore()
        .UseInMemoryIdempotencyStore()
        .UseInMemoryDeferralStore();
}
else if (messagingProvider.Equals("ServiceBus", StringComparison.OrdinalIgnoreCase))
{
    // ASB transport: connection string is read from Messaging:ServiceBus:ConnectionString
    // or the "servicebus" connection-string section. The local Service Bus emulator
    // connection string ("UseDevelopmentEnvironment=true") is the saga sample's default
    // so the sample is runnable without an Azure subscription.
    var serviceBusConnection = builder.Configuration["Messaging:ServiceBus:ConnectionString"]
        ?? builder.Configuration.GetConnectionString("servicebus")
        ?? "UseDevelopmentEnvironment=true";

    talaria
        .UseAzureServiceBusTransport(opts =>
        {
            opts.ConnectionString = serviceBusConnection;
        })
        .UseRedisStateStore(opts =>
        {
            opts.Configuration = builder.Configuration.GetConnectionString("redis") ?? "localhost:6379";
            opts.KeyPrefix = redisKeyPrefix;
        })
        .UseRedisIdempotencyStore(opts =>
        {
            opts.Configuration = builder.Configuration.GetConnectionString("redis") ?? "localhost:6379";
            opts.KeyPrefix = redisKeyPrefix;
        })
        .UseRedisDeferralStore(opts =>
        {
            opts.Configuration = builder.Configuration.GetConnectionString("redis") ?? "localhost:6379";
            opts.KeyPrefix = redisKeyPrefix;
        });
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
            opts.KeyPrefix = redisKeyPrefix;
        })
        .UseRedisIdempotencyStore(opts =>
        {
            opts.Configuration = builder.Configuration.GetConnectionString("redis") ?? "localhost:6379";
            opts.KeyPrefix = redisKeyPrefix;
        })
        .UseRedisDeferralStore(opts =>
        {
            opts.Configuration = builder.Configuration.GetConnectionString("redis") ?? "localhost:6379";
            opts.KeyPrefix = redisKeyPrefix;
        });
}

var app = builder.Build();

// Configure the saga mappings
OnboardingSagaConfigurator.ConfigureOnboardingSaga(
    app.Services,
    OnboardingSagaTopics.FromConfiguration(builder.Configuration));

// Configure a stateless consumer mapping
app.Services.MapTopic<SendVerificationEmailCommand>(emailCommandsTopic, async (msg, ct) =>
{
    var logger = app.Services.GetRequiredService<ILoggerFactory>().CreateLogger("EmailConsumer");
    logger.LogInformation("Intercepted email command for account {AccountId} — sending verification email...", msg.AccountId);
    await Task.Delay(500, ct); // simulate sending email
    app.Services.GetRequiredService<Talaria.Client.Api.ProcessingTracker>().Increment($"emails:{msg.AccountId}");
    logger.LogInformation("Verification email for account {AccountId} sent.", msg.AccountId);
});

// Diagnostics endpoint used by the AppHost integration tests to assert idempotent duplicate suppression of side effects.
// Includes the replica id so multi-replica tests can sum counts across instances.
app.MapGet("/api/diagnostics/count/{key}", (string key, [FromServices] Talaria.Client.Api.ProcessingTracker tracker) =>
    Results.Ok(new { Key = key, Count = tracker.Get(key), Instance = Environment.MachineName }));

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
    var producer = await transport.CreateProducerAsync<CreateAccountCommand>(onboardingCommandsTopic, new ProducerOptions());
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
    var producer = await transport.CreateProducerAsync<AccountVerifiedEvent>(accountEventsTopic, new ProducerOptions());
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
