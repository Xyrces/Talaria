// SPDX-License-Identifier: AGPL-3.0-or-later

using Testcontainers.Kafka;

namespace Talaria.Tests.TransportContract;

/// <summary>
/// xUnit class fixture that hosts one <c>KafkaContainer</c> for the
/// lifetime of the test collection. Skipped automatically when Docker is
/// absent; the row's <see cref="TransportContractRow.IsAvailable"/> hook
/// then suppresses the per-test skip reason.
/// </summary>
/// <since>1.0.0</since>
public sealed class KafkaContainerFixture : IAsyncLifetime
{
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
        IsAvailable = true;
        _container = new KafkaBuilder("confluentinc/cp-kafka:7.4.0").Build();
        await _container.StartAsync();
    }

    public async Task DisposeAsync()
    {
        if (_container is not null)
        {
            await _container.DisposeAsync();
            _container = null;
        }
    }
}

/// <summary>
/// Marker collection that pins the Kafka container to a single instance
/// across every <see cref="TransportContractMatrix"/> scenario in the
/// class. xUnit's <see cref="IClassFixture{T}"/> wires this in.
/// </summary>
[CollectionDefinition(Name)]
public sealed class KafkaRowCollection : ICollectionFixture<KafkaContainerFixture>
{
    public const string Name = "KafkaRowCollection";
}
