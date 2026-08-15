// SPDX-License-Identifier: Apache-2.0

using DotNet.Testcontainers.Builders;
using Testcontainers.Kafka;

namespace Talaria.Tests.TransportContract;

/// <summary>
/// Lazy singleton holder for one <c>KafkaContainer</c> shared across the
/// matrix's <c>Kafka_*</c> test methods. The container is started on the
/// first call to <see cref="EnsureStartedAsync"/> from a running test;
/// subsequent calls return the cached instance.
/// </summary>
/// <remarks>
/// <para>
/// The Kafka row in the matrix is gated behind the
/// <c>TALARIA_RUN_KAFKA_TRANSPORT_CONTRACT</c> environment variable. On
/// default CI invocations (env var unset) <see cref="EnsureStartedAsync"/>
/// returns an <see cref="IsAvailable"/>=<c>false</c> instance immediately
/// and no Docker work happens — the matrix's per-test
/// <c>Skip.IfNot</c> suppresses the <c>Kafka_*</c> scenarios cleanly. A
/// developer who wants to exercise the Kafka row locally sets
/// <c>TALARIA_RUN_KAFKA_TRANSPORT_CONTRACT=1</c> in their shell before
/// running <c>dotnet test</c>.
/// </para>
/// <para>
/// Why opt-in? <c>confluentinc/cp-kafka:7.4.0</c> is a ~700 MB image;
/// pulling it from Docker Hub on a fresh CI runner can take 60-180 s on
/// a saturated link, plus another 10-30 s for broker boot. The default
/// CI budget is too tight to make the Kafka row a CI gate, and the
/// canonical Kafka integration coverage already lives in
/// <c>Talaria.Transports.Kafka.Tests.KafkaReliabilityIntegrationTests</c>
/// (gated on <see cref="DockerFactAttribute"/>). This matrix's primary
/// value is the InMemory row, which is always available and proves the
/// matrix mechanism works; the Kafka row is a best-effort local-only
/// demonstration until the ASB sibling (task-25 / task-26) lands.
/// </para>
/// <para>
/// This class deliberately does NOT implement <c>IAsyncLifetime</c> and
/// the matrix class deliberately does NOT carry a <c>[Collection]</c>
/// attribute. xUnit's collection-fixture machinery instantiates
/// <c>IClassFixture&lt;T&gt;</c> implementations during test discovery
/// for every test class in the collection, which on a CI runner with
/// Docker present forces the Kafka image pull + broker port wait to
/// happen during discovery. By contrast, the static-lazy singleton here
/// is purely passive: no Docker work happens until a test method that
/// actually needs the fixture executes.
/// </para>
/// <para>
/// Thread safety: <see cref="EnsureStartedAsync"/> uses a
/// <see cref="SemaphoreSlim"/> so concurrent test invocations cannot
/// double-start the container. Once the singleton is initialized the
/// lock is released; the <see cref="Task{KafkaContainerFixture}"/> is
/// cached in <see cref="_initialization"/>.
/// </para>
/// </remarks>
/// <since>1.0.0</since>
public sealed class KafkaContainerFixture
{
    /// <summary>
    /// Environment variable that opts in to the Kafka row's container start.
    /// Set to <c>1</c> to enable; any other value (including unset) skips
    /// the Kafka row. Mirrors the <c>TALARIA_RUN_*</c> convention used by
    /// other optional integration suites in the repo.
    /// </summary>
    public const string OptInEnvVar = "TALARIA_RUN_KAFKA_TRANSPORT_CONTRACT";

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

    private static readonly SemaphoreSlim _gate = new(1, 1);
    private static Task<KafkaContainerFixture>? _initialization;

    /// <summary>
    /// True when the container start succeeded; false when the opt-in
    /// env var is unset, Docker is unavailable, or the container failed
    /// to come up before <see cref="ContainerReadyTimeout"/>. The
    /// matrix's per-test <c>Skip.IfNot</c> reads this and skips the
    /// Kafka_* scenarios when false.
    /// </summary>
    public bool IsAvailable { get; private set; }

    private KafkaContainer? _container;

    private KafkaContainerFixture() { }

    public string BootstrapAddress => _container?.GetBootstrapAddress()
        ?? throw new InvalidOperationException("Kafka container is not started.");

    /// <summary>
    /// Idempotent lazy initializer. First call blocks on container start
    /// (or returns an <c>IsAvailable=false</c> instance when the opt-in
    /// env var is unset, Docker is unavailable, or the container start
    /// times out). Subsequent calls return the same instance.
    /// </summary>
    public static Task<KafkaContainerFixture> EnsureStartedAsync()
    {
        // Fast path: the lazy initialization has already completed.
        var cached = _initialization;
        if (cached is { IsCompletedSuccessfully: true })
        {
            return cached;
        }

        return SlowInitializeAsync();
    }

    private static async Task<KafkaContainerFixture> SlowInitializeAsync()
    {
        await _gate.WaitAsync().ConfigureAwait(false);
        try
        {
            if (_initialization is not null)
            {
                return await _initialization.ConfigureAwait(false);
            }

            // Kick off the initialization outside the lock's critical
            // section so we don't deadlock if the awaited
            // StartAsync() does a sync-over-async hop on a thread-pool
            // callback. The Task is the synchronization primitive
            // other callers await on.
            var task = InitializeCoreAsync();
            _initialization = task;
            return await task.ConfigureAwait(false);
        }
        finally
        {
            _gate.Release();
        }
    }

    private static async Task<KafkaContainerFixture> InitializeCoreAsync()
    {
        var fixture = new KafkaContainerFixture();

        // Opt-in gate: unless TALARIA_RUN_KAFKA_TRANSPORT_CONTRACT=1 is set,
        // short-circuit and return an IsAvailable=false instance. This keeps
        // the Kafka row from running on default CI invocations, where the
        // confluentinc/cp-kafka:7.4.0 image pull + broker startup can exceed
        // CI's job budget even with a five-minute wait strategy.
        var optIn = Environment.GetEnvironmentVariable(OptInEnvVar);
        if (optIn != "1")
        {
            fixture.IsAvailable = false;
            return fixture;
        }

        if (!DockerFactAttribute.IsDockerRunning())
        {
            fixture.IsAvailable = false;
            return fixture;
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
            // Surface the reason on stderr so a CI log shows why the
            // Kafka row was skipped rather than run.
            Console.Error.WriteLine(
                $"[Talaria.Tests.TransportContract] Kafka container start failed; " +
                $"Kafka row scenarios will be skipped. Reason: {ex.GetType().Name}: {ex.Message}");
            await SafeDisposeAsync(container).ConfigureAwait(false);
            fixture.IsAvailable = false;
            return fixture;
        }

        fixture._container = container;
        fixture.IsAvailable = true;
        return fixture;
    }

    private static async Task SafeDisposeAsync(KafkaContainer container)
    {
        try
        {
            await container.DisposeAsync().ConfigureAwait(false);
        }
        catch
        {
            // Best-effort cleanup; the singleton has already failed.
        }
    }
}
