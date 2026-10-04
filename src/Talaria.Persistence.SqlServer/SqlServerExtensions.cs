// SPDX-License-Identifier: Apache-2.0
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Talaria.Core.Abstractions;
using Talaria.Core.Registration;

namespace Talaria.Persistence.SqlServer;

public static class SqlServerExtensions
{
    /// <summary>Uses the application's registered DbContext. Include ModelBuilder.AddTalaria in its model and migrate normally.</summary>
    public static TalariaBuilder UseSqlServerPersistence<TDb>(this TalariaBuilder builder) where TDb : DbContext
    {
        if (builder.Services.Any(x => x.ServiceType == typeof(IStateStoreFactory) || x.ServiceType == typeof(IOutboxStore)))
            throw new InvalidOperationException("Select one persistence bundle before configuring SQL Server.");
        builder.Services.AddSingleton<DatabaseWork<TDb>>();
        builder.Services.AddSingleton<SqlMessageStore<TDb>>();
        builder.Services.AddSingleton<IOutboxStore>(sp => sp.GetRequiredService<SqlMessageStore<TDb>>());
        builder.Services.AddSingleton<IDeferralStore>(sp => sp.GetRequiredService<SqlMessageStore<TDb>>());
        builder.Services.AddSingleton<IIdempotencyStore, SqlInboxStore<TDb>>();
        builder.Services.AddSingleton<IFailedMessageStore, SqlFailedMessageStore<TDb>>();
        builder.Services.AddSingleton<IStateStoreFactory, SqlSagaStoreFactory<TDb>>();
        builder.Services.AddScoped<SqlApplicationOutbox<TDb>>();
        builder.Services.AddScoped<IMessageOutbox>(sp => sp.GetRequiredService<SqlApplicationOutbox<TDb>>());
        builder.Services.AddScoped<IMessageTransaction>(sp => sp.GetRequiredService<SqlApplicationOutbox<TDb>>());
        return builder;
    }
}
