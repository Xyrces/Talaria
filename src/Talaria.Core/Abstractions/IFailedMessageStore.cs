// SPDX-License-Identifier: Apache-2.0
namespace Talaria.Core.Abstractions;

/// <summary>A retained failed delivery. Payloads are application data and require restricted operator access.</summary>
public sealed record FailedMessage(Guid Id, string Topic, string MessageType, string PayloadJson,
    MessageHeaders Headers, string? PartitionKey, string Reason, DateTimeOffset FailedAt);

/// <summary>Application-scoped failure retention; entries remain until explicitly deleted or replayed.</summary>
public interface IFailedMessageStore
{
    Task SaveAsync(FailedMessage message, CancellationToken ct = default);
    Task<FailedMessage?> GetAsync(Guid id, CancellationToken ct = default);
    Task<IReadOnlyList<FailedMessage>> ListAsync(int limit = 100, CancellationToken ct = default);
    Task<bool> DeleteAsync(Guid id, CancellationToken ct = default);
}

/// <summary>Operator API for inspecting and replaying retained failures.</summary>
public interface IFailedMessages
{
    Task<IReadOnlyList<FailedMessage>> ListAsync(int limit = 100, CancellationToken ct = default);
    /// <summary>Publishes with a new delivery ID, then removes the failure. May publish twice if interrupted after broker acceptance.</summary>
    Task ReplayAsync(Guid id, string? correctedPayloadJson = null, CancellationToken ct = default);
}
