// SPDX-License-Identifier: Apache-2.0
using Microsoft.EntityFrameworkCore;
using Talaria.Core;
using Talaria.Core.Abstractions;

namespace Talaria.Persistence.SqlServer;

internal sealed class SqlInboxStore<TDb>(DatabaseWork<TDb> database, TalariaOptions options) : IIdempotencyStore where TDb : DbContext
{
    private string Key(string id, string endpoint) => DatabaseWork<TDb>.Key("inbox", options.ApplicationName, endpoint, id);
    public Task<IdempotencyAcquisition> AcquireAsync(string messageId, string consumerQueue, TimeSpan expiration, CancellationToken ct = default)
    {
        if (expiration <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(expiration));
        return database.Run(async (db, token) =>
        {
            var id = Key(messageId, consumerQueue);
            var query = db.Database.IsSqlServer()
                ? db.Set<TalariaInboxRow>().FromSqlInterpolated($"SELECT * FROM dbo.TalariaInbox WITH (UPDLOCK, HOLDLOCK) WHERE Id = {id}")
                : db.Set<TalariaInboxRow>().Where(x => x.Id == id);
            var row = await query.SingleOrDefaultAsync(token);
            var now = DateTimeOffset.UtcNow;
            if (row is not null && row.ExpiresAt > now)
                return new IdempotencyAcquisition(row.Completed ? IdempotencyStatus.Completed : IdempotencyStatus.Busy);
            if (row is null) db.Add(row = new TalariaInboxRow { Id = id });
            row.Token = Guid.NewGuid().ToString("N"); row.Completed = false; row.ExpiresAt = now + expiration;
            return new IdempotencyAcquisition(IdempotencyStatus.Acquired, new(messageId, consumerQueue, row.Token));
        }, ct);
    }
    public async Task<IdempotencyLock?> TryAcquireLockAsync(string messageId, string consumerQueue, TimeSpan expiration, CancellationToken ct = default)
        => (await AcquireAsync(messageId, consumerQueue, expiration, ct)).Lock;

    public Task<bool> RenewAsync(IdempotencyLock lease, TimeSpan expiration, CancellationToken ct = default)
    {
        if (expiration <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(expiration));
        return database.Run(async (db, token) =>
        {
            var id = Key(lease.MessageId, lease.ConsumerQueue);
            var now = DateTimeOffset.UtcNow;
            return await db.Set<TalariaInboxRow>().Where(x => x.Id == id && x.Token == lease.Token && !x.Completed && x.ExpiresAt > now)
                .ExecuteUpdateAsync(s => s.SetProperty(x => x.ExpiresAt, now + expiration), token) == 1;
        }, ct);
    }

    public async Task MarkCompleteAsync(IdempotencyLock lease, CancellationToken ct = default)
    {
        var success = await database.Run(async (db, token) =>
        {
            var id = Key(lease.MessageId, lease.ConsumerQueue);
            var now = DateTimeOffset.UtcNow;
            return await db.Set<TalariaInboxRow>().Where(x => x.Id == id && x.Token == lease.Token && !x.Completed && x.ExpiresAt > now)
                .ExecuteUpdateAsync(s => s.SetProperty(x => x.Completed, true).SetProperty(x => x.ExpiresAt, now.AddDays(30)), token) == 1;
        }, ct);
        if (!success) throw new IdempotencyLeaseLostException(lease.MessageId);
    }

    public async Task ReleaseLockAsync(IdempotencyLock lease, CancellationToken ct = default)
    {
        await database.Run(async (db, token) =>
        {
            var id = Key(lease.MessageId, lease.ConsumerQueue);
            var now = DateTimeOffset.UtcNow;
            return await db.Set<TalariaInboxRow>().Where(x => x.Id == id && x.Token == lease.Token && !x.Completed && x.ExpiresAt > now).ExecuteDeleteAsync(token);
        }, ct);
    }
}
