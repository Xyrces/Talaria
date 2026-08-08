using System.Net;
using System.Net.Http.Json;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;
using Xunit;
using StackExchange.Redis;

namespace Talaria.AppHost.Tests;

public class IntegrationTest1
{
    private static readonly TimeSpan DefaultTimeout = TimeSpan.FromMinutes(3);

    [DockerFact]
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

    [DockerFact]
    public async Task ScaledIdempotency_IdenticalMessagesBombardment_ExecutesExactlyOnce()
    {
        // Arrange
        // We use a high timeout as pulling 3x replicas and kafka takes a moment in testcontainers
        var cancellationToken = new CancellationTokenSource(TimeSpan.FromMinutes(3)).Token;
        var appHost = await DistributedApplicationTestingBuilder.CreateAsync<Projects.Talaria_AppHost>(cancellationToken);

        await using var app = await appHost.BuildAsync(cancellationToken).WaitAsync(TimeSpan.FromMinutes(3), cancellationToken);
        await app.StartAsync(cancellationToken).WaitAsync(TimeSpan.FromMinutes(3), cancellationToken);

        var httpClient = app.CreateHttpClient("talaria-client");
        httpClient.Timeout = TimeSpan.FromMinutes(3);
        await app.ResourceNotifications.WaitForResourceHealthyAsync("talaria-client", cancellationToken).WaitAsync(TimeSpan.FromMinutes(3), cancellationToken);

        // Act
        var targetAccountId = Guid.NewGuid().ToString("N");
        var request = new { Email = "duplicate-test@example.com" };

        var tasks = new List<Task<HttpResponseMessage>>();

        // Bombard the environment with 10 physically identical representations representing network retry overlapping
        for (int i = 0; i < 10; i++)
        {
            tasks.Add(httpClient.PostAsJsonAsync($"/api/accounts?accountId={targetAccountId}", request, cancellationToken));
        }

        var responses = await Task.WhenAll(tasks);

        // Assert API correctly ingested all 10
        foreach (var res in responses)
        {
            Assert.Equal(HttpStatusCode.Accepted, res.StatusCode);
        }

        // The saga starter handler must run EXACTLY ONCE for the account across all 3 replicas
        // (starter-replay guard: subsequent identical commands see existing state and skip).
        // The tracker is per-replica in-memory, so poll the diagnostics endpoint repeatedly
        // and sum counts per replica instance.
        var countsByInstance = new Dictionary<string, int>();
        var stateReached = false;

        var redisConnString = await app.GetConnectionStringAsync("redis", cancellationToken);
        Assert.NotNull(redisConnString);
        var redis = StackExchange.Redis.ConnectionMultiplexer.Connect(redisConnString);
        var db = redis.GetDatabase();
        var stateKey = $"onboarding:onboardingstate:{targetAccountId}";

        for (int i = 0; i < 60; i++)
        {
            var diag = await httpClient.GetFromJsonAsync<DiagnosticsCount>($"/api/diagnostics/count/created:{targetAccountId}", cancellationToken);
            if (diag is not null)
            {
                countsByInstance[diag.Instance] = diag.Count;
            }

            var stateVal = await db.StringGetAsync(stateKey);
            stateReached = stateVal.HasValue && stateVal.ToString().Contains("VerificationSent");

            if (stateReached && countsByInstance.Values.Sum() >= 1)
            {
                break;
            }

            await Task.Delay(500, cancellationToken);
        }

        Assert.True(stateReached, "Saga state wasn't generated.");

        // Let any in-flight duplicates settle, then do a final sweep across replicas.
        await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken);
        for (int i = 0; i < 12; i++)
        {
            var diag = await httpClient.GetFromJsonAsync<DiagnosticsCount>($"/api/diagnostics/count/created:{targetAccountId}", cancellationToken);
            if (diag is not null)
            {
                countsByInstance[diag.Instance] = diag.Count;
            }

            await Task.Delay(500, cancellationToken);
        }

        var totalHandlerExecutions = countsByInstance.Values.Sum();
        Assert.Equal(1, totalHandlerExecutions);
    }

    private sealed record DiagnosticsCount(string Key, int Count, string Instance);
}
