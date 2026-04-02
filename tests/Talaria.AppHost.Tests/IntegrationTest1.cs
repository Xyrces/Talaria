using System.Net;
using System.Net.Http.Json;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Talaria.AppHost.Tests;

public class IntegrationTest1
{
    private static readonly TimeSpan DefaultTimeout = TimeSpan.FromSeconds(60);

    [Fact]
    public async Task OrchestratedSaga_TriggersAndTransitions_Successfully()
    {
        // Arrange
        var cancellationToken = new CancellationTokenSource(DefaultTimeout).Token;
        var appHost = await DistributedApplicationTestingBuilder.CreateAsync<Projects.Talaria_AppHost>(cancellationToken);
        appHost.Services.AddLogging(logging =>
        {
            logging.SetMinimumLevel(LogLevel.Debug);
            logging.AddFilter("Aspire.", LogLevel.Debug);
        });

        // We extend timeouts because TestContainers inside Aspire can take a bit to pull
        await using var app = await appHost.BuildAsync(cancellationToken).WaitAsync(DefaultTimeout, cancellationToken);
        await app.StartAsync(cancellationToken).WaitAsync(DefaultTimeout, cancellationToken);

        // Act
        // Connect to the API
        var httpClient = app.CreateHttpClient("talaria-client");
        
        // Wait for the API to be ready
        await app.ResourceNotifications.WaitForResourceHealthyAsync("talaria-client", cancellationToken).WaitAsync(DefaultTimeout, cancellationToken);
        
        // Create an account to trigger the saga via our endpoint
        var request = new { Email = "test@example.com" };
        var response = await httpClient.PostAsJsonAsync("/api/accounts", request, cancellationToken);

        // Assert
        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
    }

    [Fact]
    public async Task ScaledIdempotency_IdenticalMessagesBombardment_ExecutesExactlyOnce()
    {
        // Arrange
        // We use a high timeout as pulling 3x replicas and kafka takes a moment in testcontainers
        var cancellationToken = new CancellationTokenSource(TimeSpan.FromMinutes(3)).Token;
        var appHost = await DistributedApplicationTestingBuilder.CreateAsync<Projects.Talaria_AppHost>(cancellationToken);
        
        await using var app = await appHost.BuildAsync(cancellationToken).WaitAsync(TimeSpan.FromMinutes(3), cancellationToken);
        await app.StartAsync(cancellationToken).WaitAsync(TimeSpan.FromMinutes(3), cancellationToken);

        var httpClient = app.CreateHttpClient("talaria-client");
        await app.ResourceNotifications.WaitForResourceHealthyAsync("talaria-client", cancellationToken).WaitAsync(TimeSpan.FromMinutes(3), cancellationToken);

        // Act
        var targetMessageId = Guid.NewGuid().ToString("N");
        var targetAccountId = Guid.NewGuid().ToString("N");

        var request = new { Email = "duplicate-test@example.com" };

        var tasks = new List<Task<HttpResponseMessage>>();

        // Bombard the environment with 10 physically identical representations representing network retry overlapping
        for (int i = 0; i < 10; i++)
        {
            tasks.Add(httpClient.PostAsJsonAsync($"/api/accounts?messageId={targetMessageId}&accountId={targetAccountId}", request, cancellationToken));
        }

        var responses = await Task.WhenAll(tasks);

        // Assert API correctly ingested all 10
        foreach (var res in responses)
        {
            Assert.Equal(HttpStatusCode.Accepted, res.StatusCode);
        }

        // Delay to allow Kafka ingestion and processing
        await Task.Delay(5000, cancellationToken);

        // The Redis connection string is exposed via resource configurations
        var redisConnString = await app.GetConnectionStringAsync("redis", cancellationToken);
        Assert.NotNull(redisConnString);

        var redis = StackExchange.Redis.ConnectionMultiplexer.Connect(redisConnString);
        var db = redis.GetDatabase();

        // 1. Verify the Idempotency Lock successfully captured the execution footprint as COMPLETED
        var lockKey = $"onboarding:idemp:Talaria.Client.Api:{targetMessageId}";
        var lockVal = await db.StringGetAsync(lockKey);

        Assert.True(lockVal.HasValue, "Idempotency physical footprint was completely lost.");
        Assert.Equal("COMPLETED", (string?)lockVal);

        // 2. Verify the State output was identical to a single run through
        // Note: The physical Kafka consumers ran concurrently across all 3 AppHost nodes!
        var stateKey = $"onboarding:onboardingstate:{targetAccountId}";
        var stateVal = await db.StringGetAsync(stateKey);
        
        Assert.True(stateVal.HasValue, "Saga state wasn't generated.");
        
        var jsonValue = stateVal.ToString();
        Assert.Contains("Created", jsonValue!); // It must contain the 'Created' state
    }
}
