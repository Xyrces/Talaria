// SPDX-License-Identifier: Apache-2.0
using Microsoft.EntityFrameworkCore;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Talaria.Core;
using Talaria.Core.Abstractions;
using Talaria.Core.Registration;
using Talaria.Persistence.SqlServer;
using Talaria.Transports.InMemory;
using Testcontainers.MsSql;

namespace Talaria.Persistence.SqlServer.Tests;

[CollectionDefinition("SQL Server persistence", DisableParallelization = true)]
public sealed class SqlServerCollection : ICollectionFixture<SqlServerFixture>
{
    public const string Name = "SQL Server persistence";
}

public sealed class SqlServerFixture : IAsyncLifetime
{
    private readonly MsSqlContainer _container = new MsSqlBuilder("mcr.microsoft.com/mssql/server:2022-latest").Build();
    private ServiceProvider? _provider;

    public IServiceProvider Services => _provider ?? throw new InvalidOperationException("Fixture is not initialized.");
    public string ConnectionString { get; private set; } = "";

    public async Task InitializeAsync()
    {
        await _container.StartAsync();
        var services = new ServiceCollection();
        services.AddLogging();
        ConnectionString = new SqlConnectionStringBuilder(_container.GetConnectionString())
        {
            InitialCatalog = "TalariaPersistenceTests"
        }.ConnectionString;
        services.AddDbContext<TestDbContext>(options => options.UseSqlServer(ConnectionString, sql =>
            sql.EnableRetryOnFailure(3, TimeSpan.FromSeconds(2), null)));
        services.AddTalaria(options => options.ApplicationName = "sql-persistence-tests")
            .UseSqlServerPersistence<TestDbContext>();
        _provider = services.BuildServiceProvider();
        await ResetAsync();
    }

    public async Task ResetAsync()
    {
        await using (var scope = Services.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<TestDbContext>();
            await db.Database.EnsureDeletedAsync();
            await db.Database.EnsureCreatedAsync();
        }

        var masterConnectionString = new SqlConnectionStringBuilder(ConnectionString) { InitialCatalog = "master" }.ConnectionString;
        await using var connection = new SqlConnection(masterConnectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = "ALTER DATABASE [TalariaPersistenceTests] SET READ_COMMITTED_SNAPSHOT ON WITH ROLLBACK IMMEDIATE";
        await command.ExecuteNonQueryAsync();
    }

    public async Task DisposeAsync()
    {
        if (_provider is not null) await _provider.DisposeAsync();
        await _container.DisposeAsync();
    }
}

[Collection(SqlServerCollection.Name)]
public sealed class SqlPersistenceTests(SqlServerFixture fixture)
{
    [Fact]
    public async Task Application_outbox_and_business_changes_commit_or_rollback_together_and_receipt_skips_duplicate()
    {
        await fixture.ResetAsync();
        var messageId = Guid.NewGuid();
        var executionCount = 0;
        await using (var scope = fixture.Services.CreateAsyncScope())
        {
            var tx = scope.ServiceProvider.GetRequiredService<IMessageTransaction>();
            var outbox = scope.ServiceProvider.GetRequiredService<IMessageOutbox>();
            Assert.Same(outbox, tx);
            await tx.ExecuteAsync("orders", "receive-1", async ct =>
            {
                executionCount++;
                scope.ServiceProvider.GetRequiredService<TestDbContext>().BusinessRows.Add(new BusinessRow { Id = 1, Value = "committed" });
                await outbox.StageAsync(NewOutbox(messageId), ct);
            }, default);
        }

        await using (var scope = fixture.Services.CreateAsyncScope())
        {
            await scope.ServiceProvider.GetRequiredService<IMessageTransaction>().ExecuteAsync("orders", "receive-1", _ =>
            {
                executionCount++;
                return Task.CompletedTask;
            }, default);
        }

        await using (var scope = fixture.Services.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<TestDbContext>();
            Assert.Equal("committed", (await db.BusinessRows.SingleAsync()).Value);
            Assert.Equal(1, await db.Set<TalariaMessageRow>().CountAsync());
        }
        Assert.Equal(1, executionCount);

        await using (var scope = fixture.Services.CreateAsyncScope())
        {
            var tx = scope.ServiceProvider.GetRequiredService<IMessageTransaction>();
            var outbox = scope.ServiceProvider.GetRequiredService<IMessageOutbox>();
            await Assert.ThrowsAsync<InvalidOperationException>(() => tx.ExecuteAsync("orders", "receive-2", async ct =>
            {
                scope.ServiceProvider.GetRequiredService<TestDbContext>().BusinessRows.Add(new BusinessRow { Id = 2, Value = "rolled back" });
                await outbox.StageAsync(NewOutbox(Guid.NewGuid()), ct);
                throw new InvalidOperationException("handler failed");
            }, default));
        }

        await using (var scope = fixture.Services.CreateAsyncScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<TestDbContext>();
            Assert.Single(await db.BusinessRows.ToListAsync());
            Assert.Single(await db.Set<TalariaMessageRow>().ToListAsync());
        }
    }

    [Fact]
    public async Task Transaction_receipt_uses_retry_root_across_new_attempt_message_ids()
    {
        await fixture.ResetAsync();
        const string application = "retry-root-tests";
        const string rootMessageId = "root-message";
        var transport = new InMemoryTransport();
        var executions = 0;
        var host = Host.CreateDefaultBuilder()
            .ConfigureLogging(logging => logging.ClearProviders())
            .ConfigureServices(services =>
            {
                services.AddDbContext<TestDbContext>(options => options.UseSqlServer(fixture.ConnectionString, sql =>
                    sql.EnableRetryOnFailure(3, TimeSpan.FromSeconds(2), null)));
                services.AddTalaria(options => options.ApplicationName = application)
                    .UseTransport(transport)
                    .UseSqlServerPersistence<TestDbContext>();
            }).Build();

        try
        {
            host.MapCommand<RetryIdentityCommand>((RetryIdentityCommand message, TestDbContext db) =>
            {
                if (message.Id == 5)
                {
                    var attempt = Interlocked.Increment(ref executions);
                    db.BusinessRows.Add(new BusinessRow { Id = attempt, Value = "retry-sensitive" });
                }
                else
                {
                    db.BusinessRows.Add(new BusinessRow { Id = 100, Value = "barrier" });
                }

                return Task.CompletedTask;
            }).WithTransaction();

            await host.StartAsync();
            var topic = MessageNames.Destination(MessageNames.Contract(typeof(RetryIdentityCommand)));
            await using var producer = await transport.CreateProducerAsync<RetryIdentityCommand>(topic, new ProducerOptions());
            await producer.ProduceAsync(new RetryIdentityCommand(5), new MessageHeaders { MessageId = rootMessageId });
            await EventuallyAsync(async () =>
            {
                await using var scope = fixture.Services.CreateAsyncScope();
                var db = scope.ServiceProvider.GetRequiredService<TestDbContext>();
                return await db.BusinessRows.CountAsync() == 1 && await db.Set<TalariaReceiptRow>().CountAsync() == 1;
            }, TimeSpan.FromSeconds(10));

            await producer.ProduceAsync(new RetryIdentityCommand(5),
                new MessageHeaders { MessageId = "attempt-2", RetryRootMessageId = rootMessageId });
            // This ordered delivery proves the retry has settled before the assertions below.
            await producer.ProduceAsync(new RetryIdentityCommand(100), new MessageHeaders { MessageId = "barrier" });
            await EventuallyAsync(async () =>
            {
                await using var scope = fixture.Services.CreateAsyncScope();
                var db = scope.ServiceProvider.GetRequiredService<TestDbContext>();
                return await db.BusinessRows.AnyAsync(row => row.Id == 100) && await db.Set<TalariaReceiptRow>().CountAsync() == 2;
            }, TimeSpan.FromSeconds(10));

            await using var verificationScope = fixture.Services.CreateAsyncScope();
            var verificationDb = verificationScope.ServiceProvider.GetRequiredService<TestDbContext>();
            Assert.Equal(1, executions);
            Assert.Equal(new[] { "retry-sensitive", "barrier" }, (await verificationDb.BusinessRows.OrderBy(row => row.Id).Select(row => row.Value).ToArrayAsync()));
        }
        finally
        {
            await host.StopAsync();
            host.Dispose();
        }
    }

    [Fact]
    public async Task Saga_transitions_enforce_versions_and_replay_completed_message_receipts()
    {
        await fixture.ResetAsync();
        var store = fixture.Services.GetRequiredService<IStateStoreFactory>().GetStore<SagaState>();
        Assert.Equal(SagaCommitStatus.Committed, await store.TryTransitionAsync("order-1", "start", 0,
            new SagaState { Step = 1 }, [], default));
        Assert.Equal(SagaCommitStatus.Conflict, await store.TryTransitionAsync("order-1", "stale", 0,
            new SagaState { Step = 2 }, [], default));

        Assert.Equal(SagaCommitStatus.Committed, await store.TryTransitionAsync("order-1", "finish", 1,
            null, [], default));
        Assert.Equal(SagaCommitStatus.AlreadyProcessed, await store.TryTransitionAsync("order-1", "finish", 1,
            new SagaState { Step = 999 }, [], default));
        var snapshot = await store.ReadSnapshotAsync("order-1", "finish", default);
        Assert.Null(snapshot.State);
        Assert.Equal(2, snapshot.Version);
        Assert.True(snapshot.IsCompleted);
        Assert.True(snapshot.MessageProcessed);
    }

    [Fact]
    public async Task Outbox_lease_token_fences_stale_completion_and_abandonment()
    {
        await fixture.ResetAsync();
        var outboxId = Guid.NewGuid();
        await using (var scope = fixture.Services.CreateAsyncScope())
        {
            var tx = scope.ServiceProvider.GetRequiredService<IMessageTransaction>();
            var outbox = scope.ServiceProvider.GetRequiredService<IMessageOutbox>();
            await tx.ExecuteAsync("seed", "seed", ct => outbox.StageAsync(NewOutbox(outboxId), ct), default);
        }

        var store = fixture.Services.GetRequiredService<IOutboxStore>();
        var now = DateTimeOffset.UtcNow;
        var first = Assert.Single(await store.AcquirePendingAsync(now, TimeSpan.FromSeconds(1), 1, default));
        Assert.False(first.IsReacquired);
        await Task.Delay(1500);
        Assert.False(await store.CompleteAsync(first.Lease));
        Assert.False(await store.AbandonAsync(first.Lease));

        var second = Assert.Single(await store.AcquirePendingAsync(DateTimeOffset.UtcNow, TimeSpan.FromSeconds(1), 1, default));
        Assert.True(second.Lease.Token > first.Lease.Token);
        Assert.True(second.IsReacquired);
        Assert.False(await store.CompleteAsync(first.Lease));
        Assert.False(await store.AbandonAsync(first.Lease));
        Assert.True(await store.CompleteAsync(second.Lease));

        await using (var scope = fixture.Services.CreateAsyncScope())
        {
            var tx = scope.ServiceProvider.GetRequiredService<IMessageTransaction>();
            var outbox = scope.ServiceProvider.GetRequiredService<IMessageOutbox>();
            await tx.ExecuteAsync("seed-again", "seed-again", ct => outbox.StageAsync(NewOutbox(outboxId), ct), default);
        }

        var third = Assert.Single(await store.AcquirePendingAsync(DateTimeOffset.UtcNow, TimeSpan.FromSeconds(1), 1, default));
        Assert.True(third.Lease.Token > second.Lease.Token);
        Assert.False(await store.CompleteAsync(second.Lease));
        Assert.True(await store.CompleteAsync(third.Lease));
    }

    [Fact]
    public async Task Inbox_lease_expiration_fences_stale_release_renew_and_completion()
    {
        await fixture.ResetAsync();
        var store = fixture.Services.GetRequiredService<IIdempotencyStore>();
        var first = await store.AcquireAsync("message-1", "orders", TimeSpan.FromMilliseconds(100));
        Assert.Equal(IdempotencyStatus.Acquired, first.Status);
        await Task.Delay(150);

        var second = await store.AcquireAsync("message-1", "orders", TimeSpan.FromSeconds(5));
        Assert.Equal(IdempotencyStatus.Acquired, second.Status);
        await store.ReleaseLockAsync(first.Lock!);
        Assert.False(await store.RenewAsync(first.Lock!, TimeSpan.FromSeconds(5)));
        await Assert.ThrowsAsync<IdempotencyLeaseLostException>(() => store.MarkCompleteAsync(first.Lock!));
        Assert.Equal(IdempotencyStatus.Busy, (await store.AcquireAsync("message-1", "orders", TimeSpan.FromSeconds(5))).Status);

        await store.MarkCompleteAsync(second.Lock!);
        Assert.Equal(IdempotencyStatus.Completed, (await store.AcquireAsync("message-1", "orders", TimeSpan.FromSeconds(5))).Status);
    }

    [Fact]
    public async Task Inbox_keys_and_lease_operations_are_scoped_to_application()
    {
        await fixture.ResetAsync();
        var otherServices = new ServiceCollection();
        otherServices.AddLogging();
        otherServices.AddDbContext<TestDbContext>(options => options.UseSqlServer(fixture.ConnectionString, sql =>
            sql.EnableRetryOnFailure(3, TimeSpan.FromSeconds(2), null)));
        otherServices.AddTalaria(options => options.ApplicationName = "sql-persistence-other-app")
            .UseSqlServerPersistence<TestDbContext>();
        await using var otherProvider = otherServices.BuildServiceProvider();

        var storeA = fixture.Services.GetRequiredService<IIdempotencyStore>();
        var storeB = otherProvider.GetRequiredService<IIdempotencyStore>();
        const string messageId = "shared-message";
        const string consumerGroup = "shared-worker-group";
        var appAAcquisition = await storeA.AcquireAsync(messageId, consumerGroup, TimeSpan.FromSeconds(30));
        Assert.Equal(IdempotencyStatus.Acquired, appAAcquisition.Status);
        var appALock = Assert.IsType<IdempotencyLock>(appAAcquisition.Lock);
        var appBAcquisition = await storeB.AcquireAsync(messageId, consumerGroup, TimeSpan.FromSeconds(30));
        Assert.Equal(IdempotencyStatus.Acquired, appBAcquisition.Status);
        var appBLock = Assert.IsType<IdempotencyLock>(appBAcquisition.Lock);

        Assert.NotEqual(appALock.Token, appBLock.Token);
        Assert.Equal(IdempotencyStatus.Busy, (await storeA.AcquireAsync(messageId, consumerGroup, TimeSpan.FromSeconds(30))).Status);
        Assert.False(await storeA.RenewAsync(appBLock, TimeSpan.FromSeconds(30)));
        Assert.False(await storeB.RenewAsync(appALock, TimeSpan.FromSeconds(30)));
        await storeA.ReleaseLockAsync(appBLock);
        await storeB.ReleaseLockAsync(appALock);
        await Assert.ThrowsAsync<IdempotencyLeaseLostException>(() => storeA.MarkCompleteAsync(appBLock));
        await Assert.ThrowsAsync<IdempotencyLeaseLostException>(() => storeB.MarkCompleteAsync(appALock));
        Assert.True(await storeA.RenewAsync(appALock, TimeSpan.FromSeconds(30)));
        Assert.True(await storeB.RenewAsync(appBLock, TimeSpan.FromSeconds(30)));

        await storeA.MarkCompleteAsync(appALock);
        Assert.Equal(IdempotencyStatus.Completed,
            (await storeA.AcquireAsync(messageId, consumerGroup, TimeSpan.FromSeconds(30))).Status);
        Assert.Equal(IdempotencyStatus.Busy,
            (await storeB.AcquireAsync(messageId, consumerGroup, TimeSpan.FromSeconds(30))).Status);

        await storeB.ReleaseLockAsync(appBLock);
        var releasedAppBAcquisition = await storeB.AcquireAsync(messageId, consumerGroup, TimeSpan.FromSeconds(30));
        Assert.Equal(IdempotencyStatus.Acquired, releasedAppBAcquisition.Status);
        var releasedAppBLock = Assert.IsType<IdempotencyLock>(releasedAppBAcquisition.Lock);
        Assert.True(await storeB.RenewAsync(releasedAppBLock, TimeSpan.FromSeconds(30)));
        await storeB.MarkCompleteAsync(releasedAppBLock);
        Assert.Equal(IdempotencyStatus.Completed,
            (await storeB.AcquireAsync(messageId, consumerGroup, TimeSpan.FromSeconds(30))).Status);

        const string appACompletedMessage = "app-a-completed-before-app-b";
        var completedInA = await storeA.AcquireAsync(appACompletedMessage, consumerGroup, TimeSpan.FromSeconds(30));
        Assert.Equal(IdempotencyStatus.Acquired, completedInA.Status);
        await storeA.MarkCompleteAsync(Assert.IsType<IdempotencyLock>(completedInA.Lock));
        Assert.Equal(IdempotencyStatus.Completed,
            (await storeA.AcquireAsync(appACompletedMessage, consumerGroup, TimeSpan.FromSeconds(30))).Status);
        var firstInB = await storeB.AcquireAsync(appACompletedMessage, consumerGroup, TimeSpan.FromSeconds(30));
        Assert.Equal(IdempotencyStatus.Acquired, firstInB.Status);
        await storeB.MarkCompleteAsync(Assert.IsType<IdempotencyLock>(firstInB.Lock));
    }

    [Fact]
    public async Task Failed_messages_are_application_scoped_and_can_be_listed_and_deleted()
    {
        await fixture.ResetAsync();
        var failed = fixture.Services.GetRequiredService<IFailedMessageStore>();
        var message = new FailedMessage(Guid.NewGuid(), "orders", typeof(string).AssemblyQualifiedName!, "\"payload\"",
            new MessageHeaders { ["traceparent"] = "00-test" }, "partition-7", "handler failed", DateTimeOffset.UtcNow);
        await failed.SaveAsync(message);

        var stored = await failed.GetAsync(message.Id);
        Assert.NotNull(stored);
        Assert.Equal(message.PayloadJson, stored.PayloadJson);
        Assert.Equal(message.Headers["traceparent"], stored.Headers["traceparent"]);
        Assert.Equal("partition-7", stored.PartitionKey);
        var listed = Assert.Single(await failed.ListAsync());
        Assert.Equal(message.Id, listed.Id);
        Assert.Equal(message.Reason, listed.Reason);
        Assert.True(await failed.DeleteAsync(message.Id));
        Assert.Null(await failed.GetAsync(message.Id));
        Assert.False(await failed.DeleteAsync(message.Id));
    }

    [Fact]
    public async Task Talaria_objects_use_dbo_when_application_context_has_a_default_schema()
    {
        await fixture.ResetAsync();
        await using var scope = fixture.Services.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<TestDbContext>();
        var schemas = new List<string>();
        await using var command = db.Database.GetDbConnection().CreateCommand();
        command.CommandText = "SELECT s.name + '.' + t.name FROM sys.tables AS t JOIN sys.schemas AS s ON s.schema_id = t.schema_id WHERE t.name LIKE 'Talaria%' UNION ALL SELECT s.name + '.' + seq.name FROM sys.sequences AS seq JOIN sys.schemas AS s ON s.schema_id = seq.schema_id WHERE seq.name = 'TalariaMessageLeaseSequence'";
        if (command.Connection!.State != System.Data.ConnectionState.Open) await command.Connection.OpenAsync();
        await using (var reader = await command.ExecuteReaderAsync())
            while (await reader.ReadAsync()) schemas.Add(reader.GetString(0));

        Assert.Equal(6, schemas.Count);
        Assert.All(schemas, name => Assert.StartsWith("dbo.Talaria", name));
        Assert.Equal("messaging", db.Model.FindEntityType(typeof(BusinessRow))!.GetSchema());
        Assert.All(db.Model.GetEntityTypes().Where(entity => entity.ClrType.Namespace == typeof(TalariaMessageRow).Namespace),
            entity => Assert.Equal("dbo", entity.GetSchema()));
    }

    [Fact]
    public async Task Due_deferrals_are_acquired_under_read_committed_snapshot()
    {
        await fixture.ResetAsync();
        var store = fixture.Services.GetRequiredService<IDeferralStore>();
        var deferred = new DeferredMessage(Guid.NewGuid(), "orders", typeof(string).AssemblyQualifiedName!, "\"payload\"",
            new MessageHeaders(), "order-1", 1, DateTimeOffset.UtcNow.AddSeconds(-1));
        await store.EnqueueAsync(deferred);

        var leased = Assert.Single(await store.AcquireDueAsync(DateTimeOffset.UtcNow, TimeSpan.FromSeconds(5), 1));

        Assert.Equal(deferred.Id, leased.Message.Id);
        Assert.False(leased.IsReacquired);
        Assert.True(await store.CompleteAsync(leased.Lease));
    }

    [Fact]
    public async Task Transactional_command_shares_scoped_db_context_and_publishes_only_committed_messages()
    {
        await fixture.ResetAsync();
        var transport = new InMemoryTransport();
        var probe = new HandlerProbe();
        var host = Host.CreateDefaultBuilder()
            .ConfigureLogging(logging => logging.ClearProviders())
            .ConfigureServices(services =>
            {
                services.AddSingleton(probe);
                services.AddScoped<DbContextWitness>();
                services.AddDbContext<TestDbContext>(options => options.UseSqlServer(fixture.ConnectionString, sql =>
                    sql.EnableRetryOnFailure(3, TimeSpan.FromSeconds(2), null)));
                services.AddTalaria(options =>
                {
                    options.ApplicationName = "sql-host-transaction-tests";
                    options.OutboxRelayInterval = TimeSpan.FromMilliseconds(20);
                    options.OutboxLeaseTimeout = TimeSpan.FromSeconds(10);
                })
                    .UseTransport(transport)
                    .UseSqlServerPersistence<TestDbContext>();
            }).Build();

        try
        {
            host.MapCommand<PlaceOrderCommand>(async (PlaceOrderCommand message, TestDbContext db, DbContextWitness witness, IMessageBus bus) =>
            {
                Assert.Same(db, witness.Context);
                Assert.NotNull(db.Database.CurrentTransaction);
                db.BusinessRows.Add(new BusinessRow { Id = message.Id, Value = message.Fail ? "failed" : "committed" });
                await bus.PublishAsync(new OrderPlaced(message.Id));
                if (message.Fail)
                {
                    probe.FailingHandlerEntered.TrySetResult();
                    throw new InvalidOperationException("planned handler failure");
                }
            }).WithTransaction();

            var commandTopic = MessageNames.Destination(MessageNames.Contract(typeof(PlaceOrderCommand)));
            var eventTopic = MessageNames.Destination(MessageNames.Contract(typeof(OrderPlaced)));
            await SendAndSaveAsync(host.Services, new PlaceOrderCommand(11, false));
            await using (var scope = host.Services.CreateAsyncScope())
            {
                var db = scope.ServiceProvider.GetRequiredService<TestDbContext>();
                Assert.Equal(1, await db.Set<TalariaMessageRow>().CountAsync(x => x.Application == "sql-host-transaction-tests"));
                Assert.Empty(await db.BusinessRows.ToListAsync());
            }

            await host.StartAsync();
            await EventuallyAsync(async () => (await transport.ReadAllFromTopicAsync<OrderPlaced>(eventTopic))
                .Any(message => message.Payload.Id == 11), TimeSpan.FromSeconds(10));
            await SendAndSaveAsync(host.Services, new PlaceOrderCommand(22, true));
            await EventuallyAsync(async () => (await transport.ReadAllFromTopicAsync<PlaceOrderCommand>(commandTopic + ".dlq"))
                .Any(message => message.Payload.Id == 22), TimeSpan.FromSeconds(10));

            await using (var scope = host.Services.CreateAsyncScope())
            {
                var db = scope.ServiceProvider.GetRequiredService<TestDbContext>();
                Assert.Equal("committed", (await db.BusinessRows.SingleAsync()).Value);
                Assert.Equal(1, await db.Set<TalariaReceiptRow>().CountAsync());
                Assert.Empty(await db.Set<TalariaMessageRow>().Where(x => x.Application == "sql-host-transaction-tests").ToListAsync());
            }
            Assert.True(probe.FailingHandlerEntered.Task.IsCompletedSuccessfully);
            Assert.DoesNotContain((await transport.ReadAllFromTopicAsync<OrderPlaced>(eventTopic)), message => message.Payload.Id == 22);
        }
        finally
        {
            await host.StopAsync();
            host.Dispose();
        }
    }

    [Fact]
    public async Task Outbox_rejects_invalid_lease_parameters()
    {
        await fixture.ResetAsync();
        var store = fixture.Services.GetRequiredService<IOutboxStore>();
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => store.AcquirePendingAsync(DateTimeOffset.UtcNow, TimeSpan.Zero, 1));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => store.AcquirePendingAsync(DateTimeOffset.UtcNow, TimeSpan.FromSeconds(1), 0));
    }

    private static OutboxMessage NewOutbox(Guid id) => new(id, "orders", typeof(string).AssemblyQualifiedName!,
        "\"payload\"", new MessageHeaders(), DateTimeOffset.UtcNow);

    private static async Task SendAndSaveAsync(IServiceProvider services, PlaceOrderCommand message)
    {
        await using var scope = services.CreateAsyncScope();
        await scope.ServiceProvider.GetRequiredService<IMessageBus>().SendAsync(message);
        await scope.ServiceProvider.GetRequiredService<TestDbContext>().SaveChangesAsync();
    }

    private static async Task EventuallyAsync(Func<Task<bool>> condition, TimeSpan timeout)
    {
        var deadline = DateTimeOffset.UtcNow + timeout;
        while (DateTimeOffset.UtcNow < deadline)
        {
            if (await condition()) return;
            await Task.Delay(25);
        }
        Assert.True(await condition(), "The expected transport message was not observed before timeout.");
    }

    private sealed record PlaceOrderCommand(int Id, bool Fail);
    private sealed record RetryIdentityCommand(int Id);
    private sealed record OrderPlaced(int Id);

    private sealed class HandlerProbe
    {
        public TaskCompletionSource FailingHandlerEntered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    private sealed class DbContextWitness(TestDbContext context)
    {
        public TestDbContext Context => context;
    }

    public sealed class SagaState
    {
        public int Step { get; set; }
    }

}

public sealed class TestDbContext(DbContextOptions<TestDbContext> options) : DbContext(options)
{
    public DbSet<BusinessRow> BusinessRows => Set<BusinessRow>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.HasDefaultSchema("messaging");
        modelBuilder.AddTalaria();
        modelBuilder.Entity<BusinessRow>(entity =>
        {
            entity.ToTable("BusinessRows");
            entity.Property(row => row.Id).ValueGeneratedNever();
        });
    }
}

public sealed class BusinessRow
{
    public int Id { get; set; }
    public string Value { get; set; } = "";
}
