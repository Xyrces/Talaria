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
/// <since>1.0.0</since>
public sealed class MessageHeaders : IDictionary<string, string>
{
    // W3C Trace Context (https://www.w3.org/TR/trace-context/)
    /// <summary>W3C Trace Context traceparent header key.</summary>
    public const string TraceParentKey = "traceparent";

    /// <summary>W3C Trace Context tracestate header key.</summary>
    public const string TraceStateKey = "tracestate";

    // Talaria-specific
    /// <summary>Header key for the hop count used by the cyclic-loop guard.</summary>
    public const string HopCountKey = "talaria.hop_count";

    /// <summary>Header key describing why a message was dead-lettered.</summary>
    public const string DlqReasonKey = "talaria.dlq.reason";

    /// <summary>Header key carrying the originating topic of a DLQ-routed message.</summary>
    public const string DlqSourceTopicKey = "talaria.dlq.source_topic";

    /// <summary>Header key carrying the number of delivery attempts before DLQ routing.</summary>
    public const string DlqAttemptsKey = "talaria.dlq.attempts";

    /// <summary>Header key carrying the exception message associated with a DLQ-routed message.</summary>
    public const string DlqExceptionKey = "talaria.dlq.exception";

    /// <summary>Header key carrying the schema version of the serialized payload.</summary>
    public const string SchemaVersionKey = "talaria.schema_version";

    /// <summary>Header key carrying the saga correlation id.</summary>
    public const string CorrelationIdKey = "talaria.correlation_id";

    /// <summary>Header key carrying the unique message id used for idempotency deduplication.</summary>
    public const string MessageIdKey = "talaria.message_id";

    // Engine-owned transport metadata (not part of the public contract)
    /// <summary>Header key carrying the current deferral attempt count (engine-internal).</summary>
    public const string DeferralAttemptKey = "x-deferral-attempt";

    /// <summary>Header key carrying the assembly-qualified CLR type name of the message payload (engine-internal).</summary>
    public const string MessageTypeKey = "talaria.message_type";

    private readonly Dictionary<string, string> _inner = new(StringComparer.OrdinalIgnoreCase);

    public MessageHeaders() { }

    /// <summary>
    /// Creates headers initialized from an existing key/value sequence. Keys are stored
    /// ordinal-ignore-case.
    /// </summary>
    /// <param name="headers">Source headers. Null is treated as empty.</param>
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

    /// <summary>Reason recorded when the message is routed to the dead-letter queue.</summary>
    public string? DlqReason
    {
        get => TryGetValue(DlqReasonKey, out var v) ? v : null;
        set { if (value is not null) this[DlqReasonKey] = value; else Remove(DlqReasonKey); }
    }

    /// <summary>Exception message associated with a dead-letter routing (sanitized unless <see cref="TalariaOptions.IncludeExceptionDetailsInDlq"/> is enabled).</summary>
    public string? DlqException
    {
        get => TryGetValue(DlqExceptionKey, out var v) ? v : null;
        set { if (value is not null) this[DlqExceptionKey] = value; else Remove(DlqExceptionKey); }
    }

    // IDictionary<string, string> — delegated to the private store.

    /// <summary>Gets or sets the header value for the given key.</summary>
    /// <param name="key">The header key (case-insensitive).</param>
    public string this[string key]
    {
        get => _inner[key];
        set => _inner[key] = value;
    }

    public ICollection<string> Keys => _inner.Keys;

    public ICollection<string> Values => _inner.Values;

    public int Count => _inner.Count;

    public bool IsReadOnly => false;

    /// <summary>Adds a header. Throws when the key already exists.</summary>
    /// <param name="key">The header key.</param>
    /// <param name="value">The header value.</param>
    public void Add(string key, string value) => _inner.Add(key, value);

    /// <summary>Adds a header key/value pair. Throws when the key already exists.</summary>
    /// <param name="item">The header entry.</param>
    public void Add(KeyValuePair<string, string> item) => ((IDictionary<string, string>)_inner).Add(item);

    /// <summary>Removes all headers.</summary>
    public void Clear() => _inner.Clear();

    /// <summary>Determines whether the headers contain the given key/value pair.</summary>
    /// <param name="item">The header entry to look up.</param>
    public bool Contains(KeyValuePair<string, string> item) => ((IDictionary<string, string>)_inner).Contains(item);

    /// <summary>Determines whether a header with the given key is present.</summary>
    /// <param name="key">The header key (case-insensitive).</param>
    public bool ContainsKey(string key) => _inner.ContainsKey(key);

    /// <summary>Copies the headers into the given array starting at the specified index.</summary>
    /// <param name="array">Destination array.</param>
    /// <param name="arrayIndex">Starting index in <paramref name="array"/>.</param>
    public void CopyTo(KeyValuePair<string, string>[] array, int arrayIndex) => ((IDictionary<string, string>)_inner).CopyTo(array, arrayIndex);

    /// <summary>Enumerates the headers in insertion order.</summary>
    public IEnumerator<KeyValuePair<string, string>> GetEnumerator() => _inner.GetEnumerator();

    /// <summary>Removes the header with the given key.</summary>
    /// <param name="key">The header key (case-insensitive).</param>
    /// <returns>True when a header was removed.</returns>
    public bool Remove(string key) => _inner.Remove(key);

    /// <summary>Removes the given header key/value pair.</summary>
    /// <param name="item">The header entry to remove.</param>
    /// <returns>True when the entry existed and was removed.</returns>
    public bool Remove(KeyValuePair<string, string> item) => ((IDictionary<string, string>)_inner).Remove(item);

    /// <summary>Tries to get the value for the given header key.</summary>
    /// <param name="key">The header key (case-insensitive).</param>
    /// <param name="value">The header value when present; null otherwise.</param>
    /// <returns>True when the header was present.</returns>
    public bool TryGetValue(string key, out string value) => _inner.TryGetValue(key, out value!);

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}
