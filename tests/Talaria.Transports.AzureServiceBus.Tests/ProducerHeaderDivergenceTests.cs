// SPDX-License-Identifier: AGPL-3.0-or-later

using System.Diagnostics;
using System.Text.Json;
using Azure.Messaging.ServiceBus;
using Talaria.Core.Abstractions;
using Talaria.Transports.AzureServiceBus;
using Xunit;

namespace Talaria.Transports.AzureServiceBus.Tests;

/// <summary>
/// Pins the Azure-Service-Bus-specific divergences on the produce path.
/// The producer is responsible for stamping engine-owned headers (MessageId,
/// MessageType, hop count, trace context) and projecting the partition-key
/// onto ASB's SessionId. These behaviors differ from the in-memory and Kafka
/// producers (which use different broker fields) so any silent change
/// would surface as a consumer pipeline regression on ASB only.
/// </summary>
public class ProducerHeaderDivergenceTests
{
    [Fact]
    public async Task ProduceAsync_StampsMessageId_WhenCallerOmitsIt()
    {
        var (producer, sender) = BuildProducer<string>();

        await producer.ProduceAsync("hello");

        var sent = Assert.Single(sender.Sent);
        Assert.False(string.IsNullOrEmpty(sent.MessageId));
        // The synthesized MessageId must also surface as a Talaria header
        // so the consumer's idempotency store can dedupe by it.
        Assert.Equal(sent.MessageId, (string?)sent.ApplicationProperties[MessageHeaders.MessageIdKey]);
    }

    [Fact]
    public async Task ProduceAsync_PreservesCallerSuppliedMessageId()
    {
        var (producer, sender) = BuildProducer<string>();

        await producer.ProduceAsync("hello", new MessageHeaders { MessageId = "fixed-id" });

        var sent = Assert.Single(sender.Sent);
        Assert.Equal("fixed-id", sent.MessageId);
        // The MessageId must also surface as a Talaria header so consumer
        // pipelines that key idempotency on talaria.message_id see it.
        Assert.Equal("fixed-id", (string?)sent.ApplicationProperties[MessageHeaders.MessageIdKey]);
    }

    [Fact]
    public async Task ProduceAsync_StampsMessageType_WithClrFullName()
    {
        var (producer, sender) = BuildProducer<ProducerHeaderDivergenceTests>();

        await producer.ProduceAsync(this);

        var sent = Assert.Single(sender.Sent);
        Assert.Equal(typeof(ProducerHeaderDivergenceTests).FullName, (string?)sent.ApplicationProperties[MessageHeaders.MessageTypeKey]);
        // The CLR type is also surfaced as the broker-side Subject so
        // receivers can route without deserializing the payload.
        Assert.Equal(typeof(ProducerHeaderDivergenceTests).FullName, sent.Subject);
    }

    [Fact]
    public async Task ProduceAsync_HopCount_IncrementsExistingCount()
    {
        var (producer, sender) = BuildProducer<string>();

        await producer.ProduceAsync("hello", new MessageHeaders { HopCount = 2 });

        var sent = Assert.Single(sender.Sent);
        // Existing hop count is incremented to defend against cyclic flows.
        var raw = Assert.IsType<string>(sent.ApplicationProperties[MessageHeaders.HopCountKey]);
        Assert.Equal(3, int.Parse(raw));
    }

    [Fact]
    public async Task ProduceAsync_HopCount_FreshMessageIsNotStamped()
    {
        // A fresh MessageHeaders() without the hop-count key is left alone
        // by the producer — the engine's IdempotencyStore / saga pipeline
        // synthesizes HopCount = 0 only when a message is replayed. The
        // producer's contract is to *increment* an existing count, not to
        // invent one for fresh messages.
        var (producer, sender) = BuildProducer<string>();

        await producer.ProduceAsync("hello", new MessageHeaders());

        var sent = Assert.Single(sender.Sent);
        Assert.False(sent.ApplicationProperties.ContainsKey(MessageHeaders.HopCountKey));
    }

    [Fact]
    public async Task ProduceAsync_PartitionKey_ProjectsOntoSessionId()
    {
        var (producer, sender) = BuildProducer<string>();

        await producer.ProduceAsync("hello", partitionKey: "tenant-42");

        var sent = Assert.Single(sender.Sent);
        // ASB's analogue to a Kafka partition key is SessionId — it pins
        // messages to a single receiver when sessions are enabled on the
        // entity. Setting it is harmless when sessions are disabled.
        Assert.Equal("tenant-42", sent.SessionId);
    }

    [Fact]
    public async Task ProduceAsync_CorrelationId_FallsBackToSessionId()
    {
        var (producer, sender) = BuildProducer<string>();

        await producer.ProduceAsync(
            "hello",
            new MessageHeaders { [MessageHeaders.CorrelationIdKey] = "corr-99" });

        var sent = Assert.Single(sender.Sent);
        Assert.Equal("corr-99", sent.CorrelationId);
        // SessionId mirrors the correlation id when no explicit partition
        // key is supplied — saga messages targeting the same correlation
        // will then land on the same session receiver.
        Assert.Equal("corr-99", sent.SessionId);
    }

    [Fact]
    public async Task ProduceAsync_PartitionKey_OverridesCorrelationIdForSessionId()
    {
        var (producer, sender) = BuildProducer<string>();

        await producer.ProduceAsync(
            "hello",
            new MessageHeaders { [MessageHeaders.CorrelationIdKey] = "corr-99" },
            partitionKey: "tenant-42");

        var sent = Assert.Single(sender.Sent);
        Assert.Equal("tenant-42", sent.SessionId);
        Assert.Equal("corr-99", sent.CorrelationId);
    }

    [Fact]
    public async Task ProduceAsync_ApplicationProperties_RoundTripsEveryHeader()
    {
        var (producer, sender) = BuildProducer<string>();

        var headers = new MessageHeaders
        {
            ["custom-a"] = "1",
            ["custom-b"] = "two",
            ["x-id"] = "abc",
        };

        await producer.ProduceAsync("hello", headers);

        var sent = Assert.Single(sender.Sent);
        Assert.Equal("1", (string?)sent.ApplicationProperties["custom-a"]);
        Assert.Equal("two", (string?)sent.ApplicationProperties["custom-b"]);
        Assert.Equal("abc", (string?)sent.ApplicationProperties["x-id"]);
    }

    [Fact]
    public async Task ProduceAsync_DropsNullHeaderValues()
    {
        // ASB's ApplicationProperties doesn't accept null values. The
        // producer treats null as an engine convention ("no correlation id
        // yet") and drops it from the outgoing application properties
        // rather than coercing to "".
        var (producer, sender) = BuildProducer<string>();
        var headers = new MessageHeaders();
        headers[MessageHeaders.CorrelationIdKey] = null!;

        await producer.ProduceAsync("hello", headers);

        var sent = Assert.Single(sender.Sent);
        Assert.False(sent.ApplicationProperties.ContainsKey(MessageHeaders.CorrelationIdKey));
    }

    [Fact]
    public async Task ProduceAsync_StampsTraceContext_FromAmbientActivity()
    {
        // Activity-based trace context stamping is an engine convention;
        // when an Activity is current and the caller didn't supply a
        // traceparent, the producer copies the W3C context into the message.
        var (producer, sender) = BuildProducer<string>();

        using var activity = new Activity("test-source").Start();
        // Activity.Id is the W3C traceparent-equivalent for in-process
        // activities. The producer reads Activity.Current.Id when no
        // traceparent is supplied.
        await producer.ProduceAsync("hello");

        var sent = Assert.Single(sender.Sent);
        Assert.Equal(activity.Id, (string?)sent.ApplicationProperties[MessageHeaders.TraceParentKey]);
    }

    [Fact]
    public async Task ProduceAsync_PreservesCallerSuppliedTraceParent()
    {
        var (producer, sender) = BuildProducer<string>();

        using var _ = new Activity("ignored").Start();
        var callerSupplied = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

        await producer.ProduceAsync("hello", new MessageHeaders { TraceParent = callerSupplied });

        var sent = Assert.Single(sender.Sent);
        // Caller's traceparent is preferred over the ambient Activity's id
        // so the producer never overrides the trace context the caller
        // explicitly chose to forward.
        Assert.Equal(callerSupplied, (string?)sent.ApplicationProperties[MessageHeaders.TraceParentKey]);
    }

    [Fact]
    public async Task ProduceAsync_BodyIsJsonUtf8()
    {
        var (producer, sender) = BuildProducer<SamplePayload>();

        await producer.ProduceAsync(new SamplePayload { Id = "abc", Value = 42 });

        var sent = Assert.Single(sender.Sent);
        Assert.Equal("application/json", sent.ContentType);
        var roundTrip = JsonSerializer.Deserialize<SamplePayload>(sent.Body.ToArray());
        Assert.NotNull(roundTrip);
        Assert.Equal("abc", roundTrip!.Id);
        Assert.Equal(42, roundTrip.Value);
    }

    private static (AzureServiceBusProducer<T> producer, RecordingSender sender) BuildProducer<T>()
    {
        var sender = new RecordingSender();
        return (new AzureServiceBusProducer<T>(sender, "test-topic"), sender);
    }

    internal sealed class SamplePayload
    {
        public string Id { get; set; } = "";
        public int Value { get; set; }
    }
}
