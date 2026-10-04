// SPDX-License-Identifier: Apache-2.0
using System.Collections.Concurrent;
using Talaria.Core.Abstractions;

namespace Talaria.Transports.InMemory;

public sealed class InMemoryFailedMessageStore : IFailedMessageStore
{
    private readonly ConcurrentDictionary<Guid, FailedMessage> _messages = new();
    private static FailedMessage Copy(FailedMessage message) => message with { Headers = new(message.Headers) };
    public Task SaveAsync(FailedMessage message, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        _messages[message.Id] = Copy(message);
        return Task.CompletedTask;
    }
    public Task<FailedMessage?> GetAsync(Guid id, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        return Task.FromResult(_messages.TryGetValue(id, out var message) ? Copy(message) : null);
    }
    public Task<IReadOnlyList<FailedMessage>> ListAsync(int limit = 100, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        if (limit is < 1 or > 1000) throw new ArgumentOutOfRangeException(nameof(limit));
        return Task.FromResult<IReadOnlyList<FailedMessage>>(_messages.Values.OrderBy(x => x.FailedAt).ThenBy(x => x.Id).Take(limit).Select(Copy).ToArray());
    }
    public Task<bool> DeleteAsync(Guid id, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        return Task.FromResult(_messages.TryRemove(id, out _));
    }
}
