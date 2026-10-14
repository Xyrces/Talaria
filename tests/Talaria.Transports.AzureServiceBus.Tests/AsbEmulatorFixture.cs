using System.Net;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using Testcontainers.MsSql;
using DotNet.Testcontainers.Networks;
using Xunit;

namespace Talaria.Transports.AzureServiceBus.Tests;

[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class AsbEmulatorCollection : ICollectionFixture<AsbEmulatorFixture>
{
    public const string Name = "Azure Service Bus emulator";
}

public sealed class AsbEmulatorFixture : IAsyncLifetime
{
    private const string SqlPassword = "Talaria_Test_Password_123!";
    private INetwork? _network;
    private MsSqlContainer? _sql;
    private IContainer? _emulator;

    public string ConnectionString { get; private set; } = EmulatorIntegrationTests.DefaultConnectionString;

    public async Task InitializeAsync()
    {
        if (Environment.GetEnvironmentVariable("TALARIA_REQUIRE_DOCKER") != "1") return;

        _network = new NetworkBuilder().Build();
        await _network.CreateAsync();
        _sql = new MsSqlBuilder("mcr.microsoft.com/mssql/server:2022-latest")
            .WithPassword(SqlPassword)
            .WithNetwork(_network)
            .WithNetworkAliases("talaria-sql")
            .Build();
        await _sql.StartAsync();

        var config = Path.Combine(AppContext.BaseDirectory, "servicebus-emulator-config.json");
        if (!File.Exists(config)) throw new FileNotFoundException("Static Service Bus emulator entities config is missing.", config);
        _emulator = new ContainerBuilder("mcr.microsoft.com/azure-messaging/servicebus-emulator:latest")
            .WithNetwork(_network)
            .WithPortBinding(5672, 5672)
            .WithPortBinding(5300, true)
            .WithBindMount(config, "/ServiceBus_Emulator/ConfigFiles/Config.json")
            .WithEnvironment("ACCEPT_EULA", "Y")
            .WithEnvironment("SQL_SERVER", "talaria-sql")
            .WithEnvironment("MSSQL_SA_PASSWORD", SqlPassword)
            .WithWaitStrategy(Wait.ForUnixContainer().UntilHttpRequestIsSucceeded(request => request
                .ForPort(5300).ForPath("/health").ForStatusCode(HttpStatusCode.OK)))
            .Build();
        await _emulator.StartAsync();
        ConnectionString = "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;";
    }

    public async Task DisposeAsync()
    {
        if (_emulator is not null) await _emulator.DisposeAsync();
        if (_sql is not null) await _sql.DisposeAsync();
        if (_network is not null) await _network.DisposeAsync();
    }
}
