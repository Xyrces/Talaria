namespace Talaria.Core.Attributes;

/// <summary>
/// Marks a message handler for source generator discovery.
/// </summary>
[AttributeUsage(AttributeTargets.Method | AttributeTargets.Class)]
public sealed class TalariaHandlerAttribute : Attribute
{
    public string Topic { get; }
    public string? ConsumerGroup { get; set; }

    public TalariaHandlerAttribute(string topic)
    {
        Topic = topic;
    }
}

/// <summary>
/// Identifies the correlation property on a message type for saga state lookups.
/// If not present, the convention-based "CorrelationId" property is used.
/// </summary>
[AttributeUsage(AttributeTargets.Property)]
public sealed class SagaCorrelationAttribute : Attribute;

/// <summary>
/// Declares the schema version of a message type for versioned serialization.
/// </summary>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct)]
public sealed class MessageVersionAttribute : Attribute
{
    public int Version { get; }

    public MessageVersionAttribute(int version)
    {
        Version = version;
    }
}
