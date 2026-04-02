namespace Talaria.Core.Abstractions;

/// <summary>
/// Message headers carrying W3C Trace Context, OTel Baggage, and Talaria-specific metadata.
/// Extends Dictionary for flexible key-value storage while providing typed accessors
/// for well-known headers.
/// </summary>
public sealed class MessageHeaders : Dictionary<string, string>
{
    // W3C Trace Context (https://www.w3.org/TR/trace-context/)
    public const string TraceParentKey = "traceparent";
    public const string TraceStateKey = "tracestate";

    // Talaria-specific
    public const string HopCountKey = "talaria.hop_count";
    public const string DlqReasonKey = "talaria.dlq.reason";
    public const string DlqSourceTopicKey = "talaria.dlq.source_topic";
    public const string DlqAttemptsKey = "talaria.dlq.attempts";
    public const string DlqExceptionKey = "talaria.dlq.exception";
    public const string SchemaVersionKey = "talaria.schema_version";
    public const string CorrelationIdKey = "talaria.correlation_id";
    public const string MessageIdKey = "talaria.message_id";

    public MessageHeaders() : base(StringComparer.OrdinalIgnoreCase) { }

    public MessageHeaders(IDictionary<string, string>? headers)
        : base(headers ?? new Dictionary<string, string>(), StringComparer.OrdinalIgnoreCase) { }

    /// <summary>Unique identifier representing the delivery execution cycle, leveraged for guaranteed Exactly-Once stateful bindings and deduplication.</summary>
    public string? MessageId
    {
        get => TryGetValue(MessageIdKey, out var v) ? v : null;
        set { if (value is not null) this[MessageIdKey] = value; else Remove(MessageIdKey); }
    }

    /// <summary>W3C traceparent header.</summary>
    public string? TraceParent
    {
        get => TryGetValue(TraceParentKey, out var v) ? v : null;
        set { if (value is not null) this[TraceParentKey] = value; else Remove(TraceParentKey); }
    }

    /// <summary>W3C tracestate header.</summary>
    public string? TraceState
    {
        get => TryGetValue(TraceStateKey, out var v) ? v : null;
        set { if (value is not null) this[TraceStateKey] = value; else Remove(TraceStateKey); }
    }

    /// <summary>Number of hops this message has taken through the system.</summary>
    public int HopCount
    {
        get => TryGetValue(HopCountKey, out var s) && int.TryParse(s, out var v) ? v : 0;
        set => this[HopCountKey] = value.ToString();
    }

    /// <summary>Schema version of the serialized payload.</summary>
    public int SchemaVersion
    {
        get => TryGetValue(SchemaVersionKey, out var s) && int.TryParse(s, out var v) ? v : 1;
        set => this[SchemaVersionKey] = value.ToString();
    }

    public string? DlqReason
    {
        get => TryGetValue(DlqReasonKey, out var v) ? v : null;
        set { if (value is not null) this[DlqReasonKey] = value; else Remove(DlqReasonKey); }
    }

    public string? DlqException
    {
        get => TryGetValue(DlqExceptionKey, out var v) ? v : null;
        set { if (value is not null) this[DlqExceptionKey] = value; else Remove(DlqExceptionKey); }
    }
}
