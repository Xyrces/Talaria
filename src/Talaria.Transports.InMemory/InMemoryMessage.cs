// SPDX-License-Identifier: Apache-2.0

using Talaria.Core.Abstractions;

namespace Talaria.Transports.InMemory;

/// <summary>
/// Internal message wrapper for the in-memory channel.
/// Stores serialized payload + headers + metadata, including the optional partition key
/// so the transport carries it end-to-end even though it does not use it for ordering.
/// </summary>
internal sealed record InMemoryMessage
{
    public required string PayloadJson { get; init; }
    public MessageHeaders Headers { get; init; } = new();
    public string? PartitionKey { get; init; }
    public long Offset { get; set; }
    public DateTimeOffset Timestamp { get; init; } = DateTimeOffset.UtcNow;
}
