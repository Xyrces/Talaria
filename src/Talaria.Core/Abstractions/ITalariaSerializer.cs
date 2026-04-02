namespace Talaria.Core.Abstractions;

/// <summary>
/// Transport-agnostic serializer for message types.
/// Implementations are source-generated from [MessageVersion] attributes.
/// </summary>
public interface ITalariaSerializer<T>
{
    /// <summary>
    /// The current schema version of this message type.
    /// </summary>
    int SchemaVersion { get; }

    /// <summary>
    /// Serializes a message to bytes.
    /// </summary>
    byte[] Serialize(T message);

    /// <summary>
    /// Deserializes bytes to a message, handling version negotiation.
    /// </summary>
    T Deserialize(ReadOnlySpan<byte> data, int version);
}
