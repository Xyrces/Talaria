// SPDX-License-Identifier: Apache-2.0

using System;
using Azure.Messaging.ServiceBus;
using Talaria.Transports.AzureServiceBus;
using Xunit;

namespace Talaria.Transports.AzureServiceBus.Tests;

/// <summary>
/// Pins the public knobs on <see cref="AzureServiceBusTransportOptions"/>.
/// Defaults are referenced by the saga sample (which expects
/// <c>.dlq</c>-suffixed DLQs and a 30-second peek lock) and by the
/// emulator-gated integration tests (which assume the producer pump has
/// enough prefetch room to drain a batch without back-pressuring), so any
/// silent change here would break the sample or surface as flakes.
/// </summary>
public class TransportOptionsTests
{
    [Fact]
    public void Defaults_MirrorKafkaDlqSuffix()
    {
        var opts = new AzureServiceBusTransportOptions();
        Assert.Equal(".dlq", opts.DlqSuffix);
    }

    [Fact]
    public void Defaults_LockDurationIsThirtySeconds()
    {
        var opts = new AzureServiceBusTransportOptions();
        // The saga sample relies on this staying short so consumer restarts
        // don't double-deliver the same message.
        Assert.Equal(TimeSpan.FromSeconds(30), opts.LockDuration);
    }

    [Fact]
    public void Defaults_PrefetchMatchesProcessorBackpressureBudget()
    {
        var opts = new AzureServiceBusTransportOptions();
        // The transport widens the SDK's default (1 concurrent call) so the
        // in-process pump can drain a transactional batch within one
        // iteration. A regression here would make the saga sample flake
        // under load.
        Assert.Equal(10, opts.PrefetchCount);
    }

    [Fact]
    public void Defaults_MaxRetriesLeavesEngineNackInCharge()
    {
        var opts = new AzureServiceBusTransportOptions();
        // Engine-level NackAsync is the primary DLQ path; the broker-side
        // retry is only a safety net. Keeping this low avoids surprising
        // double-delivery when the broker's native retries race the
        // engine's DLQ write.
        Assert.Equal(3, opts.MaxRetries);
    }

    [Fact]
    public void Defaults_BufferCapacityMatchesConsumerOptionsDefault()
    {
        var opts = new AzureServiceBusTransportOptions();
        Assert.Equal(100, opts.BufferCapacity);
    }

    [Fact]
    public void DlqSuffix_IsMutable_SoDeploymentsCanMatchConventions()
    {
        var opts = new AzureServiceBusTransportOptions { DlqSuffix = "-deadletter" };
        Assert.Equal("-deadletter", opts.DlqSuffix);
    }

    [Fact]
    public void ConnectionString_AndFullyQualifiedNamespace_AreMutuallyIndependent()
    {
        // Either field may be set without the other — the constructor picks
        // whichever is present. Pin the setters here so a future refactor
        // that promotes one to a required property surfaces a compile error
        // rather than silently breaking the saga sample.
        var opts = new AzureServiceBusTransportOptions
        {
            ConnectionString = "Endpoint=sb://example/;SharedAccessKeyName=RootManage;SharedAccessKey=KEY",
            FullyQualifiedNamespace = "example.servicebus.windows.net",
        };

        Assert.Equal("Endpoint=sb://example/;SharedAccessKeyName=RootManage;SharedAccessKey=KEY", opts.ConnectionString);
        Assert.Equal("example.servicebus.windows.net", opts.FullyQualifiedNamespace);
    }
}

/// <summary>
/// Pins the transport constructor's validation rule. Constructing the
/// transport without either a connection string or a fully-qualified
/// namespace is the most common misconfiguration callers make; the
/// constructor must surface it eagerly so the host fails fast on startup
/// rather than timing out at first publish.
/// </summary>
public class AzureServiceBusTransportConstructorTests
{
    [Fact]
    public void Constructor_WithNeitherConnectionStringNorFqns_Throws()
    {
        var options = new AzureServiceBusTransportOptions();
        // No ConnectionString, no FullyQualifiedNamespace — must throw.
        var ex = Assert.Throws<ArgumentException>(() =>
            new AzureServiceBusTransport(options));
        Assert.Equal("options", ex.ParamName);
    }

    [Fact]
    public async Task Constructor_WithConnectionString_DoesNotThrow()
    {
        // The connection string is the saga sample's default. We don't open
        // any sender here (that requires the SDK to actually connect), we
        // only assert that the constructor accepts it.
        var options = new AzureServiceBusTransportOptions
        {
            ConnectionString = "Endpoint=sb://example/;SharedAccessKeyName=RootManage;SharedAccessKey=KEY",
        };

        await using var transport = new AzureServiceBusTransport(options);
        Assert.Equal("AzureServiceBus", transport.Name);
    }

    [Fact]
    public async Task Constructor_NullOptions_Throws()
    {
        // Defensive guard: a null options reference is almost certainly a
        // DI misconfiguration; we throw ArgumentNullException so the host
        // fails fast instead of dereferencing null inside the constructor.
        await Assert.ThrowsAsync<ArgumentNullException>(async () =>
        {
            await using var transport = new AzureServiceBusTransport(null!);
        });
    }

    [Fact]
    public void Name_IsExposedForLogsAndMetrics()
    {
        // ITransport.Name is consumed by the saga hosted service for log
        // scoping and metrics tagging. ASB-specific consumers and the
        // saga sample both rely on this being "AzureServiceBus".
        var options = new AzureServiceBusTransportOptions
        {
            ConnectionString = "Endpoint=sb://example/;SharedAccessKeyName=RootManage;SharedAccessKey=KEY",
        };
        var transport = new AzureServiceBusTransport(options);
        Assert.Equal("AzureServiceBus", transport.Name);
    }
}
