// SPDX-License-Identifier: AGPL-3.0-or-later

using Talaria.Core.Abstractions;
using Xunit;

namespace Talaria.Transports.AzureServiceBus.Tests;

/// <summary>
/// Verifies the single-enumeration contract of <see cref="IConsumer{T}.ConsumeAsync"/>
/// against the Azure Service Bus emulator. The guard must trip on a second enumeration
/// while still allowing the legitimate single-enumeration path used by the hosted services.
/// </summary>
public class AzureServiceBusConsumerEnumerationGuardTests : IAsyncLifetime
{
    private AzureServiceBusTransport? _transport;

    public Task InitializeAsync()
    {
        if (!EmulatorFactAttribute.IsEmulatorOptIn())
        {
            return Task.CompletedTask;
        }

        var connectionString = Environment.GetEnvironmentVariable(EmulatorIntegrationTests.ConnectionStringEnvironmentVariable);
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            connectionString = EmulatorIntegrationTests.DefaultConnectionString;
        }

        var options = new AzureServiceBusTransportOptions
        {
            ConnectionString = connectionString,
            LockDuration = TimeSpan.FromSeconds(15),
        };

        _transport = new AzureServiceBusTransport(options);
        return _transport.EnsureEntityAsync("guard-enumeration", TopologyEntityKind.Queue);
    }

    public async Task DisposeAsync()
    {
        if (_transport is not null)
        {
            await _transport.DisposeAsync();
        }
    }

    [EmulatorFact]
    public async Task FirstEnumerationWorks_SecondThrows()
    {
        var topic = "guard-enumeration";
        await using var producer = await _transport!.CreateProducerAsync<string>(topic, new ProducerOptions());
        await using var consumer = await _transport.CreateConsumerAsync<string>(
            topic,
            new ConsumerOptions { ConsumerGroup = $"guard-{Guid.NewGuid():N}" });

        await producer.ProduceAsync("guard-message", new MessageHeaders { MessageId = "g-1" });

        // First enumeration yields the message normally.
        MessageEnvelope<string>? received = null;
        await foreach (var env in consumer.ConsumeAsync(CancellationToken.None))
        {
            received = env;
            await consumer.CommitAsync(env);
            break;
        }
        Assert.NotNull(received);
        Assert.Equal("guard-message", received!.Payload);

        // A second enumeration on the same instance is forbidden by contract.
        var ex = Assert.Throws<InvalidOperationException>(() => consumer.ConsumeAsync().GetAsyncEnumerator());
        Assert.Equal(
            "ConsumeAsync may only be enumerated once per consumer instance. Create a new consumer to restart consumption.",
            ex.Message);
    }

    [EmulatorFact]
    public async Task SecondEnumerationBeforeFirstMoveNext_Throws()
    {
        var topic = "guard-enumeration";
        await using var producer = await _transport!.CreateProducerAsync<string>(topic, new ProducerOptions());
        await producer.ProduceAsync("pre-message", new MessageHeaders { MessageId = "g-pre-1" });

        await using var consumer = await _transport.CreateConsumerAsync<string>(
            topic,
            new ConsumerOptions { ConsumerGroup = $"guard-pre-{Guid.NewGuid():N}" });

        // Starting the first enumeration is allowed...
        var first = consumer.ConsumeAsync().GetAsyncEnumerator();
        Assert.NotNull(first);

        // ...but starting a second before the first is advanced is not.
        var ex = Assert.Throws<InvalidOperationException>(() => consumer.ConsumeAsync().GetAsyncEnumerator());
        Assert.Equal(
            "ConsumeAsync may only be enumerated once per consumer instance. Create a new consumer to restart consumption.",
            ex.Message);
    }

    [EmulatorFact]
    public async Task ReEnumerateReturnedInstance_Throws()
    {
        var topic = "guard-enumeration";
        await using var producer = await _transport!.CreateProducerAsync<string>(topic, new ProducerOptions());
        await using var consumer = await _transport.CreateConsumerAsync<string>(
            topic,
            new ConsumerOptions { ConsumerGroup = $"guard-reenum-{Guid.NewGuid():N}" });

        await producer.ProduceAsync("guard-message", new MessageHeaders { MessageId = "g-re-1" });

        // Capture the single IAsyncEnumerable returned by ConsumeAsync.
        var enumerable = consumer.ConsumeAsync();

        // First enumeration yields the message normally.
        MessageEnvelope<string>? received = null;
        await foreach (var env in enumerable)
        {
            received = env;
            await consumer.CommitAsync(env);
            break;
        }
        Assert.NotNull(received);
        Assert.Equal("guard-message", received!.Payload);

        // Re-enumerating the SAME returned instance is also forbidden. For async
        // iterators the guard runs when MoveNextAsync first advances the enumerator.
        var second = enumerable.GetAsyncEnumerator();
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => second.MoveNextAsync().AsTask());
        Assert.Equal(
            "ConsumeAsync may only be enumerated once per consumer instance. Create a new consumer to restart consumption.",
            ex.Message);
    }
}
