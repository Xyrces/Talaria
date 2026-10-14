using System.Net;
using System.Net.Http.Json;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Aspire.Hosting.ApplicationModel;
using Talaria.Client.Api.Sagas;
using Talaria.Core;
using Talaria.Core.Abstractions;
using Talaria.StateStores.Redis;
using Xunit;
using StackExchange.Redis;

namespace Talaria.AppHost.Tests;

[CollectionDefinition("AppHost integration", DisableParallelization = true)]
public sealed class AppHostIntegrationCollection { }

[Collection("AppHost integration")]
public class IntegrationTest1
{
    private static readonly TimeSpan DefaultTimeout = TimeSpan.FromMinutes(3);

    [DockerFact]
    public async Task OrchestratedSaga_TriggersAndTransitions_Successfully()
    {
        // Arrange
        var cancellationToken = new CancellationTokenSource(DefaultTimeout).Token;
        var appHost = await DistributedApplicationTestingBuilder.CreateAsync<Projects.Talaria_AppHost>(["--Talaria:DisableTelemetry=true"], cancellationToken);
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
        httpClient.Timeout = DefaultTimeout;
        
        // Wait for the API to be ready
        await app.ResourceNotifications.WaitForResourceHealthyAsync("talaria-client", cancellationToken).WaitAsync(DefaultTimeout, cancellationToken);
        
        // Create an account to trigger the saga via our endpoint
        var request = new { Email = "test@example.com" };
        HttpResponseMessage response;
        try
        {
            response = await httpClient.PostAsJsonAsync("/api/accounts", request, cancellationToken);
        }
        catch (Exception exception)
        {
            var logs = new List<string>();
            var loggerService = app.Services.GetRequiredService<ResourceLoggerService>();
            using var logTimeout = new CancellationTokenSource(TimeSpan.FromSeconds(3));
            foreach (var resource in new[] { "talaria-client", "kafka" })
            {
                try
                {
                    await foreach (var line in loggerService.GetAllAsync(resource).WithCancellation(logTimeout.Token))
                        logs.Add($"[{resource}] {line}");
                }
                catch (OperationCanceledException) { }
            }

            var kafkaConnection = await app.GetConnectionStringAsync("kafka", CancellationToken.None);
            throw new Xunit.Sdk.XunitException($"POST /api/accounts failed. Kafka connection string: {kafkaConnection}{Environment.NewLine}{exception}{Environment.NewLine}{string.Join(Environment.NewLine, logs)}");
        }

        // Assert
        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
    }

    [DockerFact]
    public async Task ScaledIdempotency_IdenticalMessagesBombardment_ExecutesExactlyOnce()
    {
        // Arrange
        // We use a high timeout as pulling 3x replicas and kafka takes a moment in testcontainers
        var cancellationToken = new CancellationTokenSource(TimeSpan.FromMinutes(3)).Token;
        var appHost = await DistributedApplicationTestingBuilder.CreateAsync<Projects.Talaria_AppHost>(["--Talaria:DisableTelemetry=true"], cancellationToken);

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
        // The tracker is backed by Redis and returns one shared count across replicas.
        int? totalHandlerExecutions = null;
        var stateReached = false;

        var redisConnString = await app.GetConnectionStringAsync("redis", cancellationToken);
        Assert.NotNull(redisConnString);
        using var redis = StackExchange.Redis.ConnectionMultiplexer.Connect(redisConnString);
        var stateStore = new RedisStateStore<OnboardingState>(redis,
            Options.Create(new TalariaRedisOptions { Configuration = redisConnString, KeyPrefix = "onboarding:" }),
            Options.Create(new TalariaOptions { ApplicationName = typeof(OnboardingState).Assembly.GetName().Name! }));

        for (int i = 0; i < 60; i++)
        {
            var diag = await httpClient.GetFromJsonAsync<DiagnosticsCount>($"/api/diagnostics/count/created:{targetAccountId}", cancellationToken);
            if (diag is not null)
            {
                totalHandlerExecutions = diag.Count;
            }

            var state = await stateStore.GetAsync(targetAccountId, cancellationToken);
            stateReached = state?.VerificationSent == true;

            if (stateReached && totalHandlerExecutions is >= 1)
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
                totalHandlerExecutions = diag.Count;
            }

            await Task.Delay(500, cancellationToken);
        }

        Assert.Equal(1, totalHandlerExecutions);
    }

    private sealed record DiagnosticsCount(string Key, int Count, string Instance);
}
