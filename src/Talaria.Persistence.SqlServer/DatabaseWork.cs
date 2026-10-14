// SPDX-License-Identifier: Apache-2.0
using System.Data;
using System.Security.Cryptography;
using System.Text;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace Talaria.Persistence.SqlServer;

internal sealed class DatabaseWork<TDb>(IServiceScopeFactory scopes) where TDb : DbContext
{
    private static readonly long PruneIntervalTicks = TimeSpan.FromMilliseconds(200).Ticks;
    private long _nextPruneTicks;

    public async Task PruneExpiredIfDueAsync(DateTimeOffset now, CancellationToken ct)
    {
        var current = Interlocked.Read(ref _nextPruneTicks);
        if (now.UtcTicks < current) return;
        if (Interlocked.CompareExchange(ref _nextPruneTicks, now.UtcTicks + PruneIntervalTicks, current) != current) return;

        await Run(async (db, token) =>
        {
            await db.Set<TalariaInboxRow>().Where(x => x.ExpiresAt <= now).ExecuteDeleteAsync(token);
            await db.Set<TalariaReceiptRow>().Where(x => x.ExpiresAt <= now).ExecuteDeleteAsync(token);
            await db.Set<TalariaSagaRow>().Where(x => x.ExpiresAt <= now).ExecuteDeleteAsync(token);
            return true;
        }, ct, IsolationLevel.ReadCommitted);
    }

    public async Task<T> Run<T>(Func<TDb, CancellationToken, Task<T>> work, CancellationToken ct, IsolationLevel isolation = IsolationLevel.Serializable)
    {
        await using var probeScope = scopes.CreateAsyncScope();
        var probe = probeScope.ServiceProvider.GetRequiredService<TDb>();
        return await probe.Database.CreateExecutionStrategy().ExecuteAsync(async () =>
        {
            await using var scope = scopes.CreateAsyncScope();
            var db = scope.ServiceProvider.GetRequiredService<TDb>();
            await using var transaction = await db.Database.BeginTransactionAsync(isolation, ct);
            var result = await work(db, ct);
            await db.SaveChangesAsync(ct);
            await transaction.CommitAsync(ct);
            return result;
        });
    }

    public static string Key(params string[] parts) => Convert.ToHexString(SHA256.HashData(
        Encoding.UTF8.GetBytes(System.Text.Json.JsonSerializer.Serialize(parts))));
}

