using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Talaria.Core.Diagnostics;

/// <summary>
/// Provides OpenTelemetry metrics and distributed tracing across the Talaria machinery.
/// </summary>
public static class TalariaDiagnostics
{
    public const string SourceName = "Talaria.Core";
    public const string MeterName = "Talaria.Core";

    public static readonly ActivitySource ActivitySource = new(SourceName, "1.0.0");
    public static readonly Meter Meter = new(MeterName, "1.0.0");

    // Metrics
    public static readonly Histogram<double> ProcessingDuration = Meter.CreateHistogram<double>(
        "talaria.messaging.process.duration",
        unit: "ms",
        description: "Measures the duration of message processing.");

    public static readonly Counter<long> MessagesConsumed = Meter.CreateCounter<long>(
        "talaria.messaging.consumed",
        unit: "{message}",
        description: "Number of messages consumed from transports.");

    public static readonly Counter<long> MessagesFailed = Meter.CreateCounter<long>(
        "talaria.messaging.failed",
        unit: "{message}",
        description: "Number of messages that failed processing.");

    public static readonly Counter<long> MessagesDeferred = Meter.CreateCounter<long>(
        "talaria.messaging.deferred",
        unit: "{message}",
        description: "Number of messages deferred for later processing.");

    public static readonly Counter<long> DlqRouted = Meter.CreateCounter<long>(
        "talaria.messaging.dlq",
        unit: "{message}",
        description: "Number of messages routed to the dead-letter queue.");

    /// <summary>
    /// Attempts to extract existing W3C context from headers to resume a trace,
    /// or starts a new trace if no headers exist.
    /// </summary>
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
