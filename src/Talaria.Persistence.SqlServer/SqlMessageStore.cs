// SPDX-License-Identifier: Apache-2.0
using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Talaria.Core;
using Talaria.Core.Abstractions;

namespace Talaria.Persistence.SqlServer;

internal sealed class SqlMessageStore<TDb>(DatabaseWork<TDb> database, TalariaOptions options) : IOutboxStore, IDeferralStore where TDb : DbContext
{
    internal static TalariaMessageRow Outbox(OutboxMessage message, string application) => new()
    {
        Id = message.Id, Application = application, Kind = 0, Data = JsonSerializer.Serialize(message), VisibleAt = message.CreatedAt,
    };

    public async Task EnqueueAsync(DeferredMessage message, CancellationToken ct = default)
    {
        await database.Run(async (db, token) =>
        {
            if (!await db.Set<TalariaMessageRow>().AnyAsync(x => x.Id == message.Id, token))
                db.Add(new TalariaMessageRow { Id = message.Id, Application = options.ApplicationName, Kind = 1,
                    Data = JsonSerializer.Serialize(message), VisibleAt = message.DueAt });
            return true;
        }, ct);
    }

    private Task<List<TalariaMessageRow>> Acquire(int kind, DateTimeOffset now, TimeSpan lease, int batch, CancellationToken ct)
    {
        if (lease <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(lease));
        if (batch <= 0) throw new ArgumentOutOfRangeException(nameof(batch));
        return database.Run(async (db, token) =>
        {
            var query = db.Database.IsSqlServer()
                ? db.Set<TalariaMessageRow>().FromSqlInterpolated($"SELECT TOP ({batch}) * FROM dbo.TalariaMessages WITH (UPDLOCK, READPAST, READCOMMITTEDLOCK) WHERE Application = {options.ApplicationName} AND Kind = {kind} AND VisibleAt <= {now} ORDER BY VisibleAt, Id")
                : db.Set<TalariaMessageRow>().Where(x => x.Application == options.ApplicationName && x.Kind == kind && x.VisibleAt <= now).OrderBy(x => x.VisibleAt).Take(batch);
            var rows = await query.ToListAsync(token);
            foreach (var row in rows)
            {
                row.IsReacquired = row.LeaseToken != 0;
                row.LeaseToken = db.Database.IsSqlServer()
                    ? await NextLeaseTokenAsync(db, token)
                    : row.LeaseToken + 1;
                row.VisibleAt = now + lease;
            }
            return rows;
        }, ct, System.Data.IsolationLevel.ReadCommitted);
    }

    private static async Task<long> NextLeaseTokenAsync(TDb db, CancellationToken ct)
    {
        await using var command = db.Database.GetDbConnection().CreateCommand();
        command.Transaction = db.Database.CurrentTransaction?.GetDbTransaction();
        command.CommandText = "SELECT NEXT VALUE FOR dbo.TalariaMessageLeaseSequence";
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct));
    }

    public async Task<IReadOnlyList<LeasedOutboxMessage>> AcquirePendingAsync(DateTimeOffset now, TimeSpan leaseDuration, int maxBatch, CancellationToken ct = default)
        => (await Acquire(0, now, leaseDuration, maxBatch, ct)).Select(row =>
            new LeasedOutboxMessage(JsonSerializer.Deserialize<OutboxMessage>(row.Data)!, new(row.Id, row.LeaseToken))
            { IsReacquired = row.IsReacquired }).ToArray();
    public async Task<IReadOnlyList<LeasedDeferral>> AcquireDueAsync(DateTimeOffset now, TimeSpan leaseDuration, int maxBatch, CancellationToken ct = default)
        => (await Acquire(1, now, leaseDuration, maxBatch, ct)).Select(row =>
            new LeasedDeferral(JsonSerializer.Deserialize<DeferredMessage>(row.Data)!, new(row.Id, row.LeaseToken))
            { IsReacquired = row.IsReacquired }).ToArray();

    private Task<bool> Complete(Guid id, long lease, CancellationToken ct) => database.Run(async (db, token) =>
        await db.Set<TalariaMessageRow>().Where(x => x.Application == options.ApplicationName && x.Id == id && x.LeaseToken == lease && x.VisibleAt > DateTimeOffset.UtcNow)
            .ExecuteDeleteAsync(token) == 1, ct);
    private Task<bool> Abandon(Guid id, long lease, DateTimeOffset? visible, CancellationToken ct) => database.Run(async (db, token) =>
    {
        var now = DateTimeOffset.UtcNow;
        return await db.Set<TalariaMessageRow>().Where(x => x.Application == options.ApplicationName && x.Id == id && x.LeaseToken == lease && x.VisibleAt > now)
            .ExecuteUpdateAsync(s => s.SetProperty(x => x.VisibleAt, visible ?? now), token) == 1;
    }, ct);

    public Task<bool> CompleteAsync(OutboxLease lease, CancellationToken ct = default) => Complete(lease.Id, lease.Token, ct);
    public Task<bool> CompleteAsync(DeferralLease lease, CancellationToken ct = default) => Complete(lease.Id, lease.Token, ct);
    public Task<bool> AbandonAsync(OutboxLease lease, DateTimeOffset? visibleAt = null, CancellationToken ct = default) => Abandon(lease.Id, lease.Token, visibleAt, ct);
    public Task<bool> AbandonAsync(DeferralLease lease, DateTimeOffset? visibleAt = null, CancellationToken ct = default) => Abandon(lease.Id, lease.Token, visibleAt, ct);
}
