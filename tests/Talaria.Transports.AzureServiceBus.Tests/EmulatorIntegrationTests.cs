// SPDX-License-Identifier: Apache-2.0

using Microsoft.Extensions.DependencyInjection;
using Talaria.Core.Abstractions;
using Talaria.Core.Registration;
using Talaria.Transports.AzureServiceBus;
using Xunit;

namespace Talaria.Transports.AzureServiceBus.Tests;

/// <summary>
/// End-to-end integration tests that run against the Azure Service Bus
/// emulator (<c>mcr.microsoft.com/azure-messaging/servicebus-emulator</c>)
/// when <c>TALARIA_REQUIRE_DOCKER=1</c> starts its SQL-backed Testcontainers
/// fixture, or <c>TALARIA_RUN_ASB_EMULATOR=1</c> targets an already-running
/// emulator. Outside those modes, the tests skip with an actionable message (see
/// <see cref="EmulatorFactAttribute"/>).
/// </summary>
/// <remarks>
/// <para>
/// These tests deliberately mirror the behavioural matrix of the Kafka
/// reliability suite (round-trip, subscription fan-out, poison DLQ, and
/// nack DLQ). They are not a
/// substitute for the divergence unit tests in
/// <c>ProducerHeaderDivergenceTests</c> / <c>TransportOptionsTests</c> /
/// <c>AzureServiceBusTransactionTests</c>
/// — those cover behaviour that doesn't need an emulator.
/// </para>
/// <para>
/// Set <c>TALARIA_REQUIRE_DOCKER=1</c> and run
/// <c>dotnet test tests/Talaria.Transports.AzureServiceBus.Tests --framework net8.0</c>
/// to start SQL Server and the emulator with the checked-in entity configuration.
/// </para>
/// </remarks>
[Collection(AsbEmulatorCollection.Name)]
public class EmulatorIntegrationTests(AsbEmulatorFixture fixture) : IAsyncLifetime
{
    /// <summary>
    /// Environment variable carrying the emulator connection string. When
    /// unset, the suite uses the documented default
    /// (<c>Endpoint=sb://localhost;...;UseDevelopmentEmulator=true</c>).
    /// </summary>
    public const string ConnectionStringEnvironmentVariable = "TALARIA_ASB_CONNECTION_STRING";

    /// <summary>
    /// Documented default emulator connection string. Operators may
    /// override via <see cref="ConnectionStringEnvironmentVariable"/> when
    /// the local emulator uses a different SAS key or port.
    /// </summary>
    public const string DefaultConnectionString =
        "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true";

    private AzureServiceBusTransport? _transport;

    public Task InitializeAsync()
    {
        if (!EmulatorFactAttribute.IsEmulatorOptIn())
        {
            return Task.CompletedTask;
        }

        var connectionString = Environment.GetEnvironmentVariable(ConnectionStringEnvironmentVariable);
        if (string.IsNullOrWhiteSpace(connectionString)) connectionString = fixture.ConnectionString;

        var options = new AzureServiceBusTransportOptions
        {
            ConnectionString = connectionString,
            // Tighten the peek lock so a crashed consumer redelivers fast.
            LockDuration = TimeSpan.FromSeconds(15),
        };

        _transport = new AzureServiceBusTransport(options);

        // The emulator loads the checked-in entity config at startup; it does
        // not support changing entities dynamically during a test run.
        return Task.CompletedTask;
    }

    public async Task DisposeAsync()
    {
        if (_transport is not null)
        {
            await _transport.DisposeAsync();
        }
    }

    [EmulatorFact]
    public async Task Roundtrip_PreservesPayloadAndHeaders()
    {
        var topic = "it-roundtrip";
        var headers = new MessageHeaders
        {
            MessageId = "rt-1",
            TraceParent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        };

        await using var producer = await _transport!.CreateProducerAsync<string>(topic, new ProducerOptions());
        await using var consumer = await _transport.CreateConsumerAsync<string>(topic, new ConsumerOptions { ConsumerGroup = "rt-group" });

        await producer.ProduceAsync("hello", headers);

        var envelope = await FirstAsync(consumer, TimeSpan.FromSeconds(20));
        Assert.NotNull(envelope);
        Assert.Equal("hello", envelope!.Payload);
        Assert.Equal("rt-1", envelope.Headers.MessageId);
        Assert.Equal(headers.TraceParent, envelope.Headers.TraceParent);
        Assert.Equal(topic, envelope.SourceTopic);

        await consumer.CommitAsync(envelope);
    }

    [EmulatorFact]
    public async Task TwoConsumerGroups_EachReceiveTheirOwnCopy()
    {
        // Separate subscriptions each receive their own copy of a topic event.
        var topic = "it-fanout";
        await using var producer = await _transport!.CreateProducerAsync<string>(topic, new ProducerOptions());
        await using var consumerA = await _transport.CreateConsumerAsync<string>(topic, new ConsumerOptions { ConsumerGroup = "fanout-a" });
        await using var consumerB = await _transport.CreateConsumerAsync<string>(topic, new ConsumerOptions { ConsumerGroup = "fanout-b" });

        await producer.ProduceAsync("fanout-1", new MessageHeaders { MessageId = "f-1" });

        var a = await FirstAsync(consumerA, TimeSpan.FromSeconds(20));
        var b = await FirstAsync(consumerB, TimeSpan.FromSeconds(5));

        Assert.Equal("fanout-1", Assert.IsType<MessageEnvelope<string>>(a).Payload);
        Assert.Equal("fanout-1", Assert.IsType<MessageEnvelope<string>>(b).Payload);
    }

    [EmulatorFact]
    public async Task PoisonMessage_RoutesToDlqEntity()
    {
        var topic = "it-poison";
        await using var producer = await _transport!.CreateProducerAsync<string>(topic, new ProducerOptions());
        await using var poisonConsumer = await _transport.CreateConsumerAsync<int>(
            topic, new ConsumerOptions { ConsumerGroup = "poison-handler" });
        await using var dlqConsumer = await _transport.CreateConsumerAsync<string>(
            topic + ".dlq",
            new ConsumerOptions { ConsumerGroup = "poison-dlq" });

        await producer.ProduceAsync("not-a-number", new MessageHeaders { MessageId = "p-1" });

        // The poison message is deserialization-failed by the int-typed
        // consumer (not created here, since it would block forever
        // waiting for one) and routed to the DLQ entity directly by the
        // consumer pipeline. The failed typed reader should yield no payload.
        Assert.Null(await FirstAsync(poisonConsumer, TimeSpan.FromSeconds(5)));
        var dlq = await FirstAsync(dlqConsumer, TimeSpan.FromSeconds(30));
        Assert.NotNull(dlq);
        Assert.Equal("not-a-number", dlq!.Payload);
        Assert.Equal("DeserializationFailed", dlq.Headers.DlqReason);
    }

    [EmulatorFact]
    public async Task Nack_RoutesToDlqEntity()
    {
        var topic = "it-nack";
        await using var producer = await _transport!.CreateProducerAsync<string>(topic, new ProducerOptions());
        await using var consumer = await _transport.CreateConsumerAsync<string>(topic, new ConsumerOptions { ConsumerGroup = "nack-handler" });
        await using var dlqConsumer = await _transport.CreateConsumerAsync<string>(
            topic + ".dlq",
            new ConsumerOptions { ConsumerGroup = "nack-dlq" });

        await producer.ProduceAsync("nack-me", new MessageHeaders { MessageId = "n-1" });

        var envelope = await FirstAsync(consumer, TimeSpan.FromSeconds(20));
        Assert.NotNull(envelope);
        await consumer.NackAsync(envelope!);

        var dlq = await FirstAsync(dlqConsumer, TimeSpan.FromSeconds(20));
        Assert.NotNull(dlq);
        Assert.Equal("nack-me", dlq!.Payload);
    }

    /// <summary>
    /// Returns the first envelope the consumer yields within the timeout,
    /// or null if none arrives. Cancellation-suppressing on timeout for negative assertions.
    /// </summary>
    private static async Task<MessageEnvelope<T>?> FirstAsync<T>(IConsumer<T> consumer, TimeSpan timeout)
    {
        using var cts = new CancellationTokenSource(timeout);
        try
        {
            await foreach (var env in consumer.ConsumeAsync(cts.Token))
            {
                return env;
            }
        }
        catch (OperationCanceledException)
        {
            // Timeout — expected for negative-assertion cases.
        }
        return null;
    }
}
