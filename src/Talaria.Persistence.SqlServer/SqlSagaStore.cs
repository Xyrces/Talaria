// SPDX-License-Identifier: Apache-2.0
using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using Talaria.Core;
using Talaria.Core.Abstractions;

namespace Talaria.Persistence.SqlServer;

internal sealed class SqlSagaStoreFactory<TDb>(DatabaseWork<TDb> database, TalariaOptions options) : IStateStoreFactory where TDb : DbContext
{
    public IStateStore<TState> GetStore<TState>() where TState : class, new() => new SqlSagaStore<TDb, TState>(database, options);
}

internal sealed class SqlSagaStore<TDb, TState>(DatabaseWork<TDb> database, TalariaOptions options) : IStateStore<TState>
    where TDb : DbContext where TState : class, new()
{
    private string Key(string id) => DatabaseWork<TDb>.Key(options.ApplicationName, typeof(TState).FullName!, id);
    private static string Receipt(string saga, string message) => DatabaseWork<TDb>.Key("saga", saga, message);

    public Task<SagaSnapshot<TState>> ReadSnapshotAsync(string correlationId, string messageId, CancellationToken ct = default)
        => database.Run(async (db, token) =>
        {
            var id = Key(correlationId);
            var row = await db.Set<TalariaSagaRow>().SingleOrDefaultAsync(x => x.Id == id, token);
            if (row is null || row.ExpiresAt <= DateTimeOffset.UtcNow) return new SagaSnapshot<TState>(null, 0, false, false);
            var receipt = Receipt(id, messageId);
            return new SagaSnapshot<TState>(row.StateJson is null ? null : JsonSerializer.Deserialize<TState>(row.StateJson), row.Version, row.Completed,
                await db.Set<TalariaReceiptRow>().AnyAsync(x => x.Id == receipt && x.ExpiresAt > DateTimeOffset.UtcNow, token));
        }, ct);

    public Task<SagaCommitStatus> TryTransitionAsync(string correlationId, string messageId, long expectedVersion,
        TState? newState, IReadOnlyList<OutboxMessage> outbox, CancellationToken ct = default)
        => database.Run(async (db, token) =>
        {
            var id = Key(correlationId);
            var query = db.Database.IsSqlServer()
                ? db.Set<TalariaSagaRow>().FromSqlInterpolated($"SELECT * FROM TalariaSagas WITH (UPDLOCK, HOLDLOCK) WHERE Id = {id}")
                : db.Set<TalariaSagaRow>().Where(x => x.Id == id);
            var row = await query.SingleOrDefaultAsync(token);
            var receiptId = Receipt(id, messageId);
            var receipt = await db.Set<TalariaReceiptRow>().SingleOrDefaultAsync(x => x.Id == receiptId, token);
            var now = DateTimeOffset.UtcNow;
            if (receipt is not null && receipt.ExpiresAt > now) return SagaCommitStatus.AlreadyProcessed;
            var version = row is null || row.ExpiresAt <= now ? 0 : row.Version;
            if (version != expectedVersion) return SagaCommitStatus.Conflict;
            if (row is null) db.Add(row = new TalariaSagaRow { Id = id });
            row.StateJson = newState is null ? null : JsonSerializer.Serialize(newState);
            row.Version = version + 1; row.Completed = newState is null; row.ExpiresAt = now.AddDays(30);
            if (receipt is null) db.Add(receipt = new TalariaReceiptRow { Id = receiptId });
            receipt.ExpiresAt = now.AddDays(30);
            foreach (var message in outbox) db.Add(SqlMessageStore<TDb>.Outbox(message, options.ApplicationName));
            return SagaCommitStatus.Committed;
        }, ct);

    public async Task<TState?> GetAsync(string correlationId, CancellationToken ct = default)
        => (await ReadSnapshotAsync(correlationId, "", ct)).State;
    public Task SaveAsync(string correlationId, TState state, CancellationToken ct = default) => TransitionAsync(correlationId, state, [], ct);
    public Task DeleteAsync(string correlationId, CancellationToken ct = default) => TransitionAsync(correlationId, null, [], ct);
    public async Task TransitionAsync(string correlationId, TState? newState, IReadOnlyList<OutboxMessage> outbox, CancellationToken ct = default)
    {
        var id = Guid.NewGuid().ToString("N");
        while (true)
        {
            ct.ThrowIfCancellationRequested();
            var snapshot = await ReadSnapshotAsync(correlationId, id, ct);
            if (await TryTransitionAsync(correlationId, id, snapshot.Version, newState, outbox, ct) != SagaCommitStatus.Conflict) return;
        }
    }
}
