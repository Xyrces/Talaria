using Talaria.Transports.AzureServiceBus;
using Xunit;

namespace Talaria.Transports.AzureServiceBus.Tests;

public sealed class TransactionalSessionDivergenceTests
{
    [Fact]
    public async Task BeginTransaction_ThrowsBecauseTransportCannotGuaranteeAtomicity()
    {
        await using var transport = new AzureServiceBusTransport(new AzureServiceBusTransportOptions
        {
            ConnectionString = "Endpoint=sb://unit-test.example/;SharedAccessKeyName=RootManage;SharedAccessKey=KEY",
        });

        var error = await Assert.ThrowsAsync<NotSupportedException>(() => transport.BeginTransactionAsync());
        Assert.Contains("persistence outbox", error.Message, StringComparison.OrdinalIgnoreCase);
    }
}
