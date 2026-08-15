// SPDX-License-Identifier: Apache-2.0

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;

namespace Talaria.Transports.AzureServiceBus.Tests;

/// <summary>
/// Test-only <see cref="ServiceBusSender"/> that captures every
/// <see cref="ServiceBusMessage"/> passed to <c>SendMessageAsync</c> instead
/// of talking to Azure Service Bus. Lets the producer's header-stamping
/// logic be asserted in-process — the SDK exposes a protected parameterless
/// constructor on <see cref="ServiceBusSender"/> precisely so consumers
/// (and tests) can substitute a recording double.
/// </summary>
internal sealed class RecordingSender : ServiceBusSender
{
    public List<ServiceBusMessage> Sent { get; } = new();
    public List<(ServiceBusMessage Message, DateTimeOffset ScheduledEnqueueTime)> Scheduled { get; } = new();

    public override Task SendMessageAsync(ServiceBusMessage message, CancellationToken cancellationToken = default)
    {
        Sent.Add(message);
        return Task.CompletedTask;
    }

    public override Task<long> ScheduleMessageAsync(
        ServiceBusMessage message,
        DateTimeOffset scheduledEnqueueTime,
        CancellationToken cancellationToken = default)
    {
        Scheduled.Add((message, scheduledEnqueueTime));
        return Task.FromResult((long)Scheduled.Count);
    }
}
