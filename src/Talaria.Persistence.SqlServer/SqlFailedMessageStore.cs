// SPDX-License-Identifier: Apache-2.0
using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using Talaria.Core;
using Talaria.Core.Abstractions;

namespace Talaria.Persistence.SqlServer;

internal sealed class SqlFailedMessageStore<TDb>(DatabaseWork<TDb> database, TalariaOptions options)
    : IFailedMessageStore where TDb : DbContext
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    public Task SaveAsync(FailedMessage message, CancellationToken ct = default)
        => database.Run(async (db, token) =>
        {
            var query = db.Database.IsSqlServer()
                ? db.Set<TalariaFailedMessageRow>().FromSqlInterpolated($"SELECT * FROM dbo.TalariaFailedMessages WITH (UPDLOCK, HOLDLOCK) WHERE Application = {options.ApplicationName} AND Id = {message.Id}")
                : db.Set<TalariaFailedMessageRow>().Where(x => x.Application == options.ApplicationName && x.Id == message.Id);
            var row = await query.SingleOrDefaultAsync(token);
            if (row is null)
                db.Add(row = new TalariaFailedMessageRow { Application = options.ApplicationName, Id = message.Id });
            row.Topic = message.Topic;
            row.MessageType = message.MessageType;
            row.PayloadJson = message.PayloadJson;
            row.HeadersJson = JsonSerializer.Serialize(message.Headers.ToDictionary(pair => pair.Key, pair => pair.Value), JsonOptions);
            row.PartitionKey = message.PartitionKey;
            row.Reason = message.Reason;
            row.FailedAt = message.FailedAt;
            return true;
        }, ct);

    public Task<FailedMessage?> GetAsync(Guid id, CancellationToken ct = default)
        => database.Run(async (db, token) => ToMessage(await db.Set<TalariaFailedMessageRow>()
            .SingleOrDefaultAsync(x => x.Application == options.ApplicationName && x.Id == id, token)), ct);

    public Task<IReadOnlyList<FailedMessage>> ListAsync(int limit = 100, CancellationToken ct = default)
    {
        if (limit is < 1 or > 1000) throw new ArgumentOutOfRangeException(nameof(limit));
        return database.Run<IReadOnlyList<FailedMessage>>(async (db, token) =>
            (await db.Set<TalariaFailedMessageRow>().Where(x => x.Application == options.ApplicationName)
                .OrderBy(x => x.FailedAt).ThenBy(x => x.Id).Take(limit).ToListAsync(token))
            .Select(row => ToMessage(row)!).ToArray(), ct);
    }

    public Task<bool> DeleteAsync(Guid id, CancellationToken ct = default)
        => database.Run(async (db, token) => await db.Set<TalariaFailedMessageRow>()
            .Where(x => x.Application == options.ApplicationName && x.Id == id)
            .ExecuteDeleteAsync(token) == 1, ct);

    private static FailedMessage? ToMessage(TalariaFailedMessageRow? row)
        => row is null ? null : new FailedMessage(row.Id, row.Topic, row.MessageType, row.PayloadJson,
            new MessageHeaders(JsonSerializer.Deserialize<Dictionary<string, string>>(row.HeadersJson, JsonOptions) ?? []),
            row.PartitionKey, row.Reason, row.FailedAt);
}
