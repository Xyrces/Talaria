// SPDX-License-Identifier: Apache-2.0
using System.Data;
using System.Security.Cryptography;
using System.Text;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace Talaria.Persistence.SqlServer;

internal sealed class DatabaseWork<TDb>(IServiceScopeFactory scopes) where TDb : DbContext
{
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
