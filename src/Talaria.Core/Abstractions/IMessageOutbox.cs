// SPDX-License-Identifier: Apache-2.0
namespace Talaria.Core.Abstractions;

/// <summary>Stages messages in the current application's persistence transaction.</summary>
public interface IMessageOutbox
{
    Task StageAsync(OutboxMessage message, CancellationToken ct = default);
}

/// <summary>Runs a handler and records completion in the same application database transaction.</summary>
public interface IMessageTransaction
{
    Task ExecuteAsync(string endpoint, string messageId, Func<CancellationToken, Task> handler, CancellationToken ct);
}
