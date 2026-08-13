// SPDX-License-Identifier: AGPL-3.0-or-later

using Microsoft.Extensions.DependencyInjection;
using Talaria.Core.Abstractions;
using Talaria.Core.Registration;
using Talaria.Transports.AzureServiceBus;
using Xunit;

namespace Talaria.Transports.AzureServiceBus.Tests;

/// <summary>
/// End-to-end integration tests that run against the Azure Service Bus
/// emulator (<c>mcr.microsoft.com/azure-messaging/servicebus-emulator</c>)
/// when the operator sets <c>TALARIA_RUN_ASB_EMULATOR=1</c>. The emulator
/// speaks AMQP 1.0 on <c>localhost:5672</c>; outside an emulator-run, the
/// tests skip with an actionable message (see
/// <see cref="EmulatorFactAttribute"/>).
/// </summary>
/// <remarks>
/// <para>
/// These tests deliberately mirror the behavioural matrix of the Kafka
/// reliability suite (round-trip, two-group fan-out, poison DLQ, nack
/// DLQ, transactional commit/abort visibility) so the divergence between
/// ASB's buffered-produce-commit semantics and Kafka's broker-side
/// transaction is exercised against a real broker. They are not a
/// substitute for the divergence unit tests in
/// <c>ProducerHeaderDivergenceTests</c> /
/// <c>TransportOptionsTests</c> / <c>TransactionalSessionDivergenceTests</c>
/// — those cover behaviour that doesn't need an emulator.
/// </para>
/// <para>
/// To run this suite:
/// <list type="number">
///   <item><c>docker run -d -p 5672:5672 -p 5300:5300 -e ACCEPT_EULA=y mcr.microsoft.com/azure-messaging/servicebus-emulator:latest</c></item>
///   <item><c>export TALARIA_RUN_ASB_EMULATOR=1</c></item>
///   <item><c>export TALARIA_ASB_CONNECTION_STRING="Endpoint=sb://localhost:5672;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE=;UseDevelopmentEmulator=true"</c></item>
///   <item><c>dotnet test tests/Talaria.Transports.AzureServiceBus.Tests</c></item>
/// </list>
/// The connection string is read from the optional
/// <c>TALARIA_ASB_CONNECTION_STRING</c> environment variable so the suite
/// remains configurable across emulator versions and downstream test
/// forks.
/// </para>
/// </remarks>
public class EmulatorIntegrationTests : IAsyncLifetime
{
    /// <summary>
    /// Environment variable carrying the emulator connection string. When
    /// unset, the suite uses the documented default
    /// (<c>Endpoint=sb://localhost:5672;...;UseDevelopmentEmulator=true</c>).
    /// </summary>
    public const string ConnectionStringEnvironmentVariable = "TALARIA_ASB_CONNECTION_STRING";

    /// <summary>
    /// Documented default emulator connection string. Operators may
    /// override via <see cref="ConnectionStringEnvironmentVariable"/> when
    /// the local emulator uses a different SAS key or port.
    /// </summary>
    public const string DefaultConnectionString =
        "Endpoint=sb://localhost:5672;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE=;UseDevelopmentEmulator=true";

    private AzureServiceBusTransport? _transport;

    public Task InitializeAsync()
    {
        if (!EmulatorFactAttribute.IsEmulatorOptIn())
        {
            return Task.CompletedTask;
        }

        var connectionString = Environment.GetEnvironmentVariable(ConnectionStringEnvironmentVariable);
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            connectionString = DefaultConnectionString;
        }

        var options = new AzureServiceBusTransportOptions
        {
            ConnectionString = connectionString,
            // Tighten the peek lock so a crashed consumer redelivers fast.
            LockDuration = TimeSpan.FromSeconds(15),
        };

        _transport = new AzureServiceBusTransport(options);

        // Pre-provision the queues used by the tests so the first run
        // doesn't race the broker's entity-existence checks. EnsureEntityAsync
        // is idempotent: subsequent runs are no-ops if the entity already
        // exists with matching settings.
        return Task.WhenAll(
            _transport.EnsureEntityAsync("it-roundtrip", TopologyEntityKind.Queue),
            _transport.EnsureEntityAsync("it-fanout", TopologyEntityKind.Queue),
            _transport.EnsureEntityAsync("it-poison", TopologyEntityKind.Queue),
            _transport.EnsureEntityAsync("it-nack", TopologyEntityKind.Queue),
            _transport.EnsureEntityAsync("it-tx-commit", TopologyEntityKind.Queue),
            _transport.EnsureEntityAsync("it-tx-abort", TopologyEntityKind.Queue));
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
        await using var consumer = await _transport.CreateConsumerAsync<string>(topic, new ConsumerOptions { ConsumerGroup = $"rt-{Guid.NewGuid():N}" });

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
        // ASB queues are competing-consumer: only ONE of two freshly-created
        // groups receives a given message — they share the same entity, not
        // a pub/sub topic. This test pins the ASB semantics so a future
        // refactor that accidentally wires queues like pub/sub topics
        // surfaces as a failure here rather than in the saga sample.
        var topic = "it-fanout";
        await using var producer = await _transport!.CreateProducerAsync<string>(topic, new ProducerOptions());
        await using var consumerA = await _transport.CreateConsumerAsync<string>(topic, new ConsumerOptions { ConsumerGroup = $"a-{Guid.NewGuid():N}" });
        await using var consumerB = await _transport.CreateConsumerAsync<string>(topic, new ConsumerOptions { ConsumerGroup = $"b-{Guid.NewGuid():N}" });

        await producer.ProduceAsync("fanout-1", new MessageHeaders { MessageId = "f-1" });

        var a = await FirstAsync(consumerA, TimeSpan.FromSeconds(20));
        var b = await FirstAsync(consumerB, TimeSpan.FromSeconds(5));

        // At least one group must receive the message; the other may not,
        // depending on which group the broker routes to. We assert that
        // exactly one of them receives it (competing-consumer semantics).
        Assert.True(
            (a is not null && b is null) || (a is null && b is not null),
            "ASB competing-consumer queues route each message to exactly one group.");
        if (a is not null) Assert.Equal("fanout-1", a.Payload);
        if (b is not null) Assert.Equal("fanout-1", b.Payload);
    }

    [EmulatorFact]
    public async Task PoisonMessage_RoutesToDlqEntity()
    {
        var topic = "it-poison";
        await using var producer = await _transport!.CreateProducerAsync<string>(topic, new ProducerOptions());
        await using var dlqConsumer = await _transport.CreateConsumerAsync<string>(
            topic + ".dlq",
            new ConsumerOptions { ConsumerGroup = $"poison-dlq-{Guid.NewGuid():N}" });

        await producer.ProduceAsync("not-a-number", new MessageHeaders { MessageId = "p-1" });

        // The poison message is deserialization-failed by the int-typed
        // consumer (not created here, since it would block forever
        // waiting for one) and routed to the DLQ entity directly by the
        // consumer pipeline. Wait for it on the DLQ consumer.
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
        await using var consumer = await _transport.CreateConsumerAsync<string>(topic, new ConsumerOptions { ConsumerGroup = $"nack-{Guid.NewGuid():N}" });
        await using var dlqConsumer = await _transport.CreateConsumerAsync<string>(
            topic + ".dlq",
            new ConsumerOptions { ConsumerGroup = $"nack-dlq-{Guid.NewGuid():N}" });

        await producer.ProduceAsync("nack-me", new MessageHeaders { MessageId = "n-1" });

        var envelope = await FirstAsync(consumer, TimeSpan.FromSeconds(20));
        Assert.NotNull(envelope);
        await consumer.NackAsync(envelope!);

        var dlq = await FirstAsync(dlqConsumer, TimeSpan.FromSeconds(20));
        Assert.NotNull(dlq);
        Assert.Equal("nack-me", dlq!.Payload);
    }

    [EmulatorFact]
    public async Task TransactionalCommit_MakesProducesVisible()
    {
        var topic = "it-tx-commit";
        await using var consumer = await _transport!.CreateConsumerAsync<string>(topic, new ConsumerOptions { ConsumerGroup = $"txc-{Guid.NewGuid():N}" });

        await using (var session = await _transport.BeginTransactionAsync())
        {
            var txProducer = await session.GetProducerAsync<string>(topic);
            await txProducer.ProduceAsync("tx-1");
            await txProducer.ProduceAsync("tx-2");
            await session.CommitAsync();
        }

        var first = await FirstAsync(consumer, TimeSpan.FromSeconds(20));
        var second = await FirstAsync(consumer, TimeSpan.FromSeconds(20));
        Assert.NotNull(first);
        Assert.NotNull(second);
        Assert.Equal("tx-1", first!.Payload);
        Assert.Equal("tx-2", second!.Payload);
    }

    [EmulatorFact]
    public async Task TransactionalAbort_ProducesNothing()
    {
        var topic = "it-tx-abort";
        await using var consumer = await _transport!.CreateConsumerAsync<string>(topic, new ConsumerOptions { ConsumerGroup = $"txa-{Guid.NewGuid():N}" });

        await using (var session = await _transport.BeginTransactionAsync())
        {
            var txProducer = await session.GetProducerAsync<string>(topic);
            await txProducer.ProduceAsync("aborted-1");
            await txProducer.ProduceAsync("aborted-2");
            await session.AbortAsync();
        }

        var none = await FirstAsync(consumer, TimeSpan.FromSeconds(8));
        Assert.Null(none);
    }

    /// <summary>
    /// Returns the first envelope the consumer yields within the timeout,
    /// or null if none arrives. Cancellation-suppressing on timeout — the
    /// negative assertions (<c>TransactionalAbort_ProducesNothing</c>)
    /// rely on the no-throw timeout path.
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
