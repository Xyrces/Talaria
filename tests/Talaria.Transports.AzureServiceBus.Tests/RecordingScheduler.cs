// SPDX-License-Identifier: Apache-2.0

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Azure;
using Talaria.Core.Abstractions;
using Talaria.Transports.AzureServiceBus.Deferral;
using Xunit;

namespace Talaria.Transports.AzureServiceBus.Tests;

/// <summary>
/// Test-only <see cref="IServiceBusMessageScheduler"/> that captures every
/// <c>ScheduleAsync</c> call instead of talking to Azure Service Bus. Lets the
/// unit suite assert what the adapter handed off without an emulator or live
/// broker connection.
/// </summary>
internal sealed class RecordingScheduler : IServiceBusMessageScheduler
{
    public List<ScheduledCall> Calls { get; } = new();

    public Task<long> ScheduleAsync(
        string topic,
        BinaryData body,
        IReadOnlyDictionary<string, object> applicationProperties,
        DateTimeOffset scheduledEnqueueTime,
        string? partitionKey,
        CancellationToken ct = default)
    {
        Calls.Add(new ScheduledCall(topic, body, new Dictionary<string, object>(applicationProperties), scheduledEnqueueTime, partitionKey));
        return Task.FromResult((long)(Calls.Count));
    }

    public sealed record ScheduledCall(
        string Topic,
        BinaryData Body,
        IReadOnlyDictionary<string, object> ApplicationProperties,
        DateTimeOffset ScheduledEnqueueTime,
        string? PartitionKey);
}
