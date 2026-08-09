using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Talaria.Core.Diagnostics;

/// <summary>
/// Provides OpenTelemetry metrics and distributed tracing across the Talaria machinery.
/// </summary>
/// <since>1.0.0</since>
public static class TalariaDiagnostics
{
    /// <summary>The <see cref="ActivitySource"/> name registered with OpenTelemetry.</summary>
    public const string SourceName = "Talaria.Core";

    /// <summary>The <see cref="Meter"/> name registered with OpenTelemetry.</summary>
    public const string MeterName = "Talaria.Core";

    public static readonly ActivitySource ActivitySource = new(SourceName, "1.0.0");
    public static readonly Meter Meter = new(MeterName, "1.0.0");

    // Metrics
    /// <summary>Histogram of message processing duration in milliseconds.</summary>
    public static readonly Histogram<double> ProcessingDuration = Meter.CreateHistogram<double>(
        "talaria.messaging.process.duration",
        unit: "ms",
        description: "Measures the duration of message processing.");

    /// <summary>Counter of messages successfully consumed from transports.</summary>
    public static readonly Counter<long> MessagesConsumed = Meter.CreateCounter<long>(
        "talaria.messaging.consumed",
        unit: "{message}",
        description: "Number of messages consumed from transports.");

    /// <summary>Counter of messages that failed processing (handler exception or DLQ).</summary>
    public static readonly Counter<long> MessagesFailed = Meter.CreateCounter<long>(
        "talaria.messaging.failed",
        unit: "{message}",
        description: "Number of messages that failed processing.");

    /// <summary>Counter of messages deferred for later processing.</summary>
    public static readonly Counter<long> MessagesDeferred = Meter.CreateCounter<long>(
        "talaria.messaging.deferred",
        unit: "{message}",
        description: "Number of messages deferred for later processing.");

    /// <summary>Counter of messages routed to the dead-letter queue.</summary>
    public static readonly Counter<long> DlqRouted = Meter.CreateCounter<long>(
        "talaria.messaging.dlq",
        unit: "{message}",
        description: "Number of messages routed to the dead-letter queue.");

    // ---- Transactional outbox relay ----

    /// <summary>Counter of outbox entries published by the relay.</summary>
    public static readonly Counter<long> OutboxPublished = Meter.CreateCounter<long>(
        "talaria.outbox.published",
        unit: "{message}",
        description: "Number of outbox entries published to the transport by the relay.");

    /// <summary>Counter of outbox publish failures (lease abandoned for retry).</summary>
    public static readonly Counter<long> OutboxPublishFailed = Meter.CreateCounter<long>(
        "talaria.outbox.publish.failed",
        unit: "{message}",
        description: "Number of failed outbox publish attempts (lease abandoned for retry).");

    /// <summary>Counter of outbox entries acquired more than once — indicates relay instability.</summary>
    public static readonly Counter<long> OutboxReacquired = Meter.CreateCounter<long>(
        "talaria.outbox.reacquired",
        unit: "{message}",
        description: "Number of outbox entries acquired more than once — indicates a previous relay crashed (lease expired) or abandoned the entry. Track this: a rising rate means relay instability.");

    /// <summary>Up/down counter of outbox entries currently leased by this node's relay.</summary>
    public static readonly UpDownCounter<long> OutboxActiveLeases = Meter.CreateUpDownCounter<long>(
        "talaria.outbox.active_leases",
        unit: "{lease}",
        description: "Number of outbox entries currently leased by this node's relay.");

    /// <summary>Histogram of outbox staging-to-publication lag (milliseconds).</summary>
    public static readonly Histogram<double> OutboxLag = Meter.CreateHistogram<double>(
        "talaria.outbox.lag",
        unit: "ms",
        description: "Time from outbox staging to successful publication (relay lag). A rising lag means the relay is falling behind.");

    // ---- Deferral sweeper ----

    /// <summary>Counter of deferred messages republished by the sweeper.</summary>
    public static readonly Counter<long> DeferralRepublished = Meter.CreateCounter<long>(
        "talaria.deferral.republished",
        unit: "{message}",
        description: "Number of deferred messages republished by the sweeper.");

    /// <summary>Counter of deferral republication failures (lease abandoned for retry).</summary>
    public static readonly Counter<long> DeferralRepublishFailed = Meter.CreateCounter<long>(
        "talaria.deferral.republish.failed",
        unit: "{message}",
        description: "Number of failed deferral republication attempts (lease abandoned for retry).");

    /// <summary>Counter of deferred messages acquired more than once — indicates sweeper instability.</summary>
    public static readonly Counter<long> DeferralReacquired = Meter.CreateCounter<long>(
        "talaria.deferral.reacquired",
        unit: "{message}",
        description: "Number of deferred messages acquired more than once — indicates a previous sweeper crashed (lease expired) or abandoned the entry. Track this: a rising rate means sweeper instability.");

    /// <summary>Up/down counter of deferred messages currently leased by this node's sweeper.</summary>
    public static readonly UpDownCounter<long> DeferralActiveLeases = Meter.CreateUpDownCounter<long>(
        "talaria.deferral.active_leases",
        unit: "{lease}",
        description: "Number of deferred messages currently leased by this node's sweeper.");

    /// <summary>Histogram of lateness past the scheduled due time when a deferred message is republished.</summary>
    public static readonly Histogram<double> DeferralLag = Meter.CreateHistogram<double>(
        "talaria.deferral.lag",
        unit: "ms",
        description: "Lateness past the scheduled due time when a deferred message is republished. A rising lag means the sweeper is falling behind.");

    /// <summary>
    /// Attempts to extract existing W3C context from headers to resume a trace,
    /// or starts a new trace if no headers exist.
    /// </summary>
    /// <param name="topic">The topic name the consumer is processing.</param>
    /// <param name="messageType">The CLR type name of the message being processed.</param>
    /// <param name="headers">The message headers carrying W3C trace context, when present.</param>
    /// <returns>The started <see cref="Activity"/>, or null when no listener is registered.</returns>
    public static Activity? StartConsumerActivity(string topic, string messageType, Abstractions.MessageHeaders headers)
    {
        var parentContext = default(ActivityContext);
        if (!string.IsNullOrEmpty(headers.TraceParent))
        {
            ActivityContext.TryParse(headers.TraceParent, headers.TraceState, out parentContext);
        }

        var activity = ActivitySource.StartActivity(
            $"{topic} process",
            ActivityKind.Consumer,
            parentContext);

        if (activity != null)
        {
            activity.SetTag("messaging.system", "talaria");
            activity.SetTag("messaging.destination.name", topic);
            activity.SetTag("messaging.operation", "process");
            activity.SetTag("messaging.message.type", messageType);
        }

        return activity;
    }
}
