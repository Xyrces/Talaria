// SPDX-License-Identifier: AGPL-3.0-or-later

using DotNet.Testcontainers.Builders;
using Testcontainers.Kafka;

namespace Talaria.Tests.TransportContract;

/// <summary>
/// xUnit class fixture that hosts one <c>KafkaContainer</c> for the
/// lifetime of the test collection. Skipped automatically when Docker is
/// absent or the container cannot start; the row's
/// <see cref="TransportContractRow.IsAvailable"/> hook then suppresses the
/// per-test skip reason.
/// </summary>
/// <remarks>
/// <para>
/// The fixture is designed to fail soft: if Docker is unavailable OR the
/// broker image pull + startup exceeds the configured wait timeout, the
/// fixture sets <see cref="IsAvailable"/> to <c>false</c> instead of
/// throwing. xUnit treats a thrown <c>InitializeAsync</c> as a fatal
/// collection failure that fails the entire test run; soft-failing here
/// mirrors the behaviour <see cref="DockerFactAttribute"/> provides to
/// the per-class Kafka suite.
/// </para>
/// <para>
/// The container wait strategy uses a five-minute timeout to absorb the
/// first-run image pull on a fresh CI runner. Once the image is cached
/// locally subsequent runs reuse the cached layer and complete in seconds.
/// </para>
/// </remarks>
/// <since>1.0.0</since>
public sealed class KafkaContainerFixture : IAsyncLifetime
{
    /// <summary>Default port the confluentinc/cp-kafka image exposes for PLAINTEXT listeners.</summary>
    private const int KafkaBrokerPort = 9092;

    /// <summary>
    /// Five minutes is generous: a freshly-provisioned runner with no
    /// <c>confluentinc/cp-kafka:7.4.0</c> layer cached pulls ~700 MB
    /// from Docker Hub, which can take 60-120 s on a saturated link,
    /// then the broker needs another 10-30 s to elect a controller and
    /// open the listener. 60 s (testcontainers' default) is too tight.
    /// </summary>
    private static readonly TimeSpan ContainerReadyTimeout = TimeSpan.FromMinutes(5);

    public bool IsAvailable { get; private set; }
    private KafkaContainer? _container;

    public string BootstrapAddress => _container?.GetBootstrapAddress()
        ?? throw new InvalidOperationException("Kafka container is not started.");

    public async Task InitializeAsync()
    {
        if (!DockerFactAttribute.IsDockerRunning())
        {
            IsAvailable = false;
            return;
        }

        KafkaContainer container = new KafkaBuilder("confluentinc/cp-kafka:7.4.0")
            .WithWaitStrategy(Wait.ForUnixContainer()
                .UntilInternalTcpPortIsAvailable(KafkaBrokerPort, strategy => strategy.WithTimeout(ContainerReadyTimeout)))
            .Build();

        try
        {
            await container.StartAsync().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            // Soft-fail: any startup failure (image pull timeout, port
            // bind failure, broker boot timeout) sets IsAvailable = false
            // so the matrix's per-test Skip.IfNot suppresses the Kafka
            // scenarios cleanly instead of failing the entire test run.
            // We surface the reason on stderr so a CI log shows why the
            // Kafka row was skipped rather than run.
            Console.Error.WriteLine(
                $"[Talaria.Tests.TransportContract] Kafka container start failed; " +
                $"Kafka row scenarios will be skipped. Reason: {ex.GetType().Name}: {ex.Message}");
            await SafeDisposeAsync(container).ConfigureAwait(false);
            IsAvailable = false;
            return;
        }

        _container = container;
        IsAvailable = true;
    }

    public async Task DisposeAsync()
    {
        if (_container is not null)
        {
            await _container.DisposeAsync().ConfigureAwait(false);
            _container = null;
        }
    }

    private static async Task SafeDisposeAsync(KafkaContainer container)
    {
        try
        {
            await container.DisposeAsync().ConfigureAwait(false);
        }
        catch
        {
            // Best-effort cleanup; the fixture has already failed.
        }
    }
}

/// <summary>
/// Marker collection that pins the Kafka container to a single instance
/// across every <see cref="TransportContractMatrix"/> scenario in the
/// class. xUnit's <see cref="IClassFixture{T}"/> wires this in via the
/// <see cref="ICollectionFixture{T}"/> interface below.
/// </summary>
[CollectionDefinition(Name)]
public sealed class KafkaRowCollection : ICollectionFixture<KafkaContainerFixture>
{
    public const string Name = "KafkaRowCollection";
}
