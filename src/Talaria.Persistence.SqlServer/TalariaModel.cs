// SPDX-License-Identifier: Apache-2.0
using Microsoft.EntityFrameworkCore;
using System.ComponentModel.DataAnnotations.Schema;

namespace Talaria.Persistence.SqlServer;

public sealed class TalariaMessageRow
{
    public Guid Id { get; set; }
    public string Application { get; set; } = "";
    public int Kind { get; set; }
    public string Data { get; set; } = "";
    public DateTimeOffset VisibleAt { get; set; }
    public long LeaseToken { get; set; }
    [NotMapped]
    public bool IsReacquired { get; set; }
}

public sealed class TalariaInboxRow
{
    public string Id { get; set; } = "";
    public string Token { get; set; } = "";
    public bool Completed { get; set; }
    public DateTimeOffset ExpiresAt { get; set; }
}

public sealed class TalariaReceiptRow
{
    public string Id { get; set; } = "";
    public DateTimeOffset ExpiresAt { get; set; }
}

public sealed class TalariaSagaRow
{
    public string Id { get; set; } = "";
    public string? StateJson { get; set; }
    public long Version { get; set; }
    public bool Completed { get; set; }
    public DateTimeOffset ExpiresAt { get; set; }
}

public sealed class TalariaFailedMessageRow
{
    public Guid Id { get; set; }
    public string Application { get; set; } = "";
    public string Topic { get; set; } = "";
    public string MessageType { get; set; } = "";
    public string PayloadJson { get; set; } = "";
    public string HeadersJson { get; set; } = "{}";
    public string? PartitionKey { get; set; }
    public string Reason { get; set; } = "";
    public DateTimeOffset FailedAt { get; set; }
}

public static class TalariaModelExtensions
{
    /// <summary>Adds Talaria tables to the application's normal EF migration model.</summary>
    public static ModelBuilder AddTalaria(this ModelBuilder model)
    {
        model.HasSequence<long>("TalariaMessageLeaseSequence").StartsAt(1).IncrementsBy(1);
        model.Entity<TalariaMessageRow>(b =>
        {
            b.ToTable("TalariaMessages"); b.HasKey(x => x.Id);
            b.Property(x => x.Application).HasMaxLength(200);
            b.Property(x => x.Data).IsRequired();
            b.Property(x => x.LeaseToken).IsConcurrencyToken();
            b.HasIndex(x => new { x.Application, x.Kind, x.VisibleAt });
        });
        model.Entity<TalariaInboxRow>(b =>
        {
            b.ToTable("TalariaInbox"); b.HasKey(x => x.Id);
            b.Property(x => x.Id).HasMaxLength(64); b.Property(x => x.Token).HasMaxLength(64);
            b.HasIndex(x => x.ExpiresAt);
        });
        model.Entity<TalariaReceiptRow>(b =>
        {
            b.ToTable("TalariaReceipts"); b.HasKey(x => x.Id);
            b.Property(x => x.Id).HasMaxLength(64); b.HasIndex(x => x.ExpiresAt);
        });
        model.Entity<TalariaSagaRow>(b =>
        {
            b.ToTable("TalariaSagas"); b.HasKey(x => x.Id);
            b.Property(x => x.Id).HasMaxLength(64);
            b.Property(x => x.Version).IsConcurrencyToken(); b.HasIndex(x => x.ExpiresAt);
        });
        model.Entity<TalariaFailedMessageRow>(b =>
        {
            b.ToTable("TalariaFailedMessages"); b.HasKey(x => new { x.Application, x.Id });
            b.Property(x => x.Application).HasMaxLength(200);
            b.Property(x => x.Topic).HasMaxLength(500);
            b.Property(x => x.MessageType).HasMaxLength(1000);
            b.Property(x => x.PayloadJson).IsRequired();
            b.Property(x => x.HeadersJson).IsRequired();
            b.Property(x => x.Reason).IsRequired();
            b.HasIndex(x => new { x.Application, x.FailedAt });
        });
        return model;
    }
}
