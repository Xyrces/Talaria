// SPDX-License-Identifier: AGPL-3.0-or-later

using System.Collections;

namespace Talaria.Core.Abstractions;

/// <summary>
/// Message headers carrying W3C Trace Context, OTel Baggage, and Talaria-specific metadata.
/// Encapsulates a private, ordinal-ignore-case key-value store with typed accessors for
/// well-known headers. Instances are mutable, but producers always clone any headers they
/// receive before mutating or storing them — a headers object is never shared by reference
/// between a sender and the resulting message.
/// </summary>
public sealed class MessageHeaders : IDictionary<string, string>
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

    // Engine-owned transport metadata (not part of the public contract)
    public const string DeferralAttemptKey = "x-deferral-attempt";
    public const string MessageTypeKey = "talaria.message_type";

    private readonly Dictionary<string, string> _inner = new(StringComparer.OrdinalIgnoreCase);

    public MessageHeaders() { }

    public MessageHeaders(IEnumerable<KeyValuePair<string, string>>? headers)
    {
        if (headers is null)
        {
            return;
        }

        foreach (var kvp in headers)
        {
            _inner[kvp.Key] = kvp.Value;
        }
    }

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

    // IDictionary<string, string> — delegated to the private store.

    public string this[string key]
    {
        get => _inner[key];
        set => _inner[key] = value;
    }

    public ICollection<string> Keys => _inner.Keys;

    public ICollection<string> Values => _inner.Values;

    public int Count => _inner.Count;

    public bool IsReadOnly => false;

    public void Add(string key, string value) => _inner.Add(key, value);

    public void Add(KeyValuePair<string, string> item) => ((IDictionary<string, string>)_inner).Add(item);

    public void Clear() => _inner.Clear();

    public bool Contains(KeyValuePair<string, string> item) => ((IDictionary<string, string>)_inner).Contains(item);

    public bool ContainsKey(string key) => _inner.ContainsKey(key);

    public void CopyTo(KeyValuePair<string, string>[] array, int arrayIndex) => ((IDictionary<string, string>)_inner).CopyTo(array, arrayIndex);

    public IEnumerator<KeyValuePair<string, string>> GetEnumerator() => _inner.GetEnumerator();

    public bool Remove(string key) => _inner.Remove(key);

    public bool Remove(KeyValuePair<string, string> item) => ((IDictionary<string, string>)_inner).Remove(item);

    public bool TryGetValue(string key, out string value) => _inner.TryGetValue(key, out value!);

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}
