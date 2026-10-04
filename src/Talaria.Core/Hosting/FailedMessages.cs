// SPDX-License-Identifier: Apache-2.0
using System.Text.Json;
using Talaria.Core.Abstractions;

namespace Talaria.Core.Hosting;

internal sealed class FailedMessages(IFailedMessageStore store, ProducerCache producers) : IFailedMessages
{
    public Task<IReadOnlyList<FailedMessage>> ListAsync(int limit = 100, CancellationToken ct = default)
        => store.ListAsync(limit, ct);

    public async Task ReplayAsync(Guid id, string? correctedPayloadJson = null, CancellationToken ct = default)
    {
        var failed = await store.GetAsync(id, ct) ?? throw new KeyNotFoundException($"Failure '{id}' was not found.");
        var payload = JsonSerializer.Deserialize<JsonElement>(correctedPayloadJson ?? failed.PayloadJson);
        if (payload.ValueKind == JsonValueKind.Null) throw new ArgumentException("Replay payload must not be null.", nameof(correctedPayloadJson));
        var headers = new MessageHeaders(failed.Headers)
        {
            MessageId = Guid.NewGuid().ToString("N"), RetryAttempt = 0, RetryRootMessageId = null,
            DlqReason = null, DlqException = null,
        };
        headers.Remove(MessageHeaders.DlqAttemptsKey);
        headers.Remove(MessageHeaders.DlqSourceTopicKey);
        headers.HopCount = 0;
        headers[MessageHeaders.MessageTypeKey] = failed.MessageType.Split(',')[0];
        headers["talaria.replay.failure"] = id.ToString("N");
        var producer = await producers.GetOrCreateAsync(failed.Topic, typeof(JsonElement), ct);
        await producer.Produce(payload, headers, failed.PartitionKey, ct);
        await store.DeleteAsync(id, ct);
    }
}
