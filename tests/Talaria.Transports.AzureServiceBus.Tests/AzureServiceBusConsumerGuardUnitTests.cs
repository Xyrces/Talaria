// SPDX-License-Identifier: AGPL-3.0-or-later

using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Logging.Abstractions;
using Talaria.Core.Abstractions;
using Xunit;

namespace Talaria.Transports.AzureServiceBus.Tests;

/// <summary>
/// Unit-level coverage for the <see cref="AzureServiceBusConsumer{T}"/> single-
/// enumeration guard. These tests do not require the ASB emulator: construction
/// and <c>ServiceBusClient.CreateProcessor</c> are client-side operations, and
/// the guard throws before any network I/O happens.
/// </summary>
public class AzureServiceBusConsumerGuardUnitTests
{
    private const string FakeConnectionString =
        "Endpoint=sb://fake.servicebus.windows.net/;SharedAccessKeyName=x;SharedAccessKey=eQ==";

    private static AzureServiceBusConsumer<string> CreateConsumer()
    {
        var client = new ServiceBusClient(FakeConnectionString);
        var processor = client.CreateProcessor("topic", "subscription");
        var sender = client.CreateSender("topic-dlq");

        return new AzureServiceBusConsumer<string>(
            processor,
            sender,
            topic: "topic",
            dlqEntity: "topic-dlq",
            bufferCapacity: 10,
            includeDlqExceptionDetails: false,
            logger: NullLogger<AzureServiceBusConsumer<string>>.Instance);
    }

    [Fact]
    public void SecondConsumeAsyncCall_ThrowsInvalidOperationException()
    {
        var consumer = CreateConsumer();

        // First call returns the enumerable without throwing.
        var first = consumer.ConsumeAsync();
        Assert.NotNull(first);

        // A second call on the same instance is forbidden.
        var ex = Assert.Throws<InvalidOperationException>(() => consumer.ConsumeAsync());
        Assert.Equal(SingleEnumerationGuard.Message, ex.Message);
    }

    [Fact]
    public async Task ReEnumerateReturnedInstance_ThrowsInvalidOperationException()
    {
        var consumer = CreateConsumer();

        using var cts = new CancellationTokenSource();
        var enumerable = consumer.ConsumeAsync(cts.Token);

        // First enumerator is allowed. MoveNextAsync runs the iterator body up to its
        // first await synchronously, which sets the _enumerating guard flag.
        var firstEnumerator = enumerable.GetAsyncEnumerator();
        var firstMove = firstEnumerator.MoveNextAsync().AsTask();

        // A second enumerator on the same returned instance is now forbidden. For async
        // iterators the guard runs when MoveNextAsync first advances the enumerator.
        var secondEnumerator = enumerable.GetAsyncEnumerator();
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => secondEnumerator.MoveNextAsync().AsTask());
        Assert.Equal(SingleEnumerationGuard.Message, ex.Message);

        // Clean up the blocked first enumeration.
        cts.Cancel();
        try { await firstMove; }
        catch (OperationCanceledException) { }
    }
}
