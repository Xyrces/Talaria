// SPDX-License-Identifier: Apache-2.0
using System.Data;
using Microsoft.EntityFrameworkCore;
using Talaria.Core;
using Talaria.Core.Abstractions;

namespace Talaria.Persistence.SqlServer;

internal sealed class SqlApplicationOutbox<TDb>(TDb db, TalariaOptions options) : IMessageOutbox, IMessageTransaction where TDb : DbContext
{
    public Task StageAsync(OutboxMessage message, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        db.Add(SqlMessageStore<TDb>.Outbox(message, options.ApplicationName));
        return Task.CompletedTask;
    }

    public async Task ExecuteAsync(string endpoint, string messageId, Func<CancellationToken, Task> handler, CancellationToken ct)
    {
        if (db.Database.CurrentTransaction is not null) throw new InvalidOperationException("The messaging endpoint owns its receive transaction.");
        await db.Database.CreateExecutionStrategy().ExecuteAsync(async () =>
        {
            db.ChangeTracker.Clear();
            await using var transaction = await db.Database.BeginTransactionAsync(IsolationLevel.Serializable, ct);
            var id = DatabaseWork<TDb>.Key("handler", options.ApplicationName, endpoint, messageId);
            var query = db.Database.IsSqlServer()
                ? db.Set<TalariaReceiptRow>().FromSqlInterpolated($"SELECT * FROM TalariaReceipts WITH (UPDLOCK, HOLDLOCK) WHERE Id = {id}")
                : db.Set<TalariaReceiptRow>().Where(x => x.Id == id);
            var receipt = await query.SingleOrDefaultAsync(ct);
            if (receipt is not null && receipt.ExpiresAt > DateTimeOffset.UtcNow)
            {
                await transaction.CommitAsync(ct);
                return;
            }
            try
            {
                await handler(ct);
                if (receipt is null) db.Add(receipt = new TalariaReceiptRow { Id = id });
                receipt.ExpiresAt = DateTimeOffset.UtcNow.AddDays(30);
                await db.SaveChangesAsync(ct);
                await transaction.CommitAsync(ct);
            }
            catch
            {
                try { await transaction.RollbackAsync(CancellationToken.None); }
                catch { /* Preserve the handler or persistence exception that caused rollback. */ }
                db.ChangeTracker.Clear();
                throw;
            }
        });
    }
}
