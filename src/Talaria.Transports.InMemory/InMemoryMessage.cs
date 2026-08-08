using Talaria.Core.Abstractions;

namespace Talaria.Transports.InMemory;

/// <summary>
/// Internal message wrapper for the in-memory channel.
/// Stores serialized payload + headers + metadata.
/// </summary>
internal sealed record InMemoryMessage
{
    public required string PayloadJson { get; init; }
    public MessageHeaders Headers { get; init; } = new();
    public long Offset { get; set; }
    public DateTimeOffset Timestamp { get; init; } = DateTimeOffset.UtcNow;
}
