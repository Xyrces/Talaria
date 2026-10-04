// SPDX-License-Identifier: Apache-2.0
using DotNet.Testcontainers.Builders;
using Testcontainers.Kafka;

namespace Talaria.Tests.TransportContract;

/// <summary>A lazily started shared broker; mandatory in CI and opt-in for local matrix runs.</summary>
public sealed class KafkaContainerFixture
{
    public const string OptInEnvVar = "TALARIA_RUN_KAFKA_TRANSPORT_CONTRACT";
    private static readonly Lazy<Task<KafkaContainerFixture>> Initialization = new(InitializeAsync);
    private KafkaContainer? _container;
    public bool IsAvailable => _container is not null;
    public string BootstrapAddress => _container?.GetBootstrapAddress()
        ?? throw new InvalidOperationException("Kafka container is not started.");

    public static Task<KafkaContainerFixture> EnsureStartedAsync() => Initialization.Value;

    private static async Task<KafkaContainerFixture> InitializeAsync()
    {
        var fixture = new KafkaContainerFixture();
        var required = Environment.GetEnvironmentVariable("TALARIA_REQUIRE_DOCKER") == "1";
        if (!required && Environment.GetEnvironmentVariable(OptInEnvVar) != "1") return fixture;
        if (!DockerFactAttribute.IsDockerRunning())
        {
            if (required) throw new InvalidOperationException("Docker is required for the Kafka transport contract.");
            return fixture;
        }
        var container = new KafkaBuilder("confluentinc/cp-kafka:7.4.0")
            .WithWaitStrategy(Wait.ForUnixContainer().UntilInternalTcpPortIsAvailable(9092,
                strategy => strategy.WithTimeout(TimeSpan.FromMinutes(5))))
            .Build();
        try { await container.StartAsync(); }
        catch
        {
            await container.DisposeAsync();
            throw; // Once selected, broker startup failures must fail the suite.
        }
        fixture._container = container;
        return fixture;
    }
}
