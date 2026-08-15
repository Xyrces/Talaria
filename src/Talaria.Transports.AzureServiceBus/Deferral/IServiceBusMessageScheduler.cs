// SPDX-License-Identifier: Apache-2.0

using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Talaria.Transports.AzureServiceBus.Deferral;

/// <summary>
/// Thin abstraction over the broker-side send-and-schedule primitives that
/// <see cref="DeferralAdapter"/> depends on. Hides the sealed Azure Service Bus SDK
/// types so unit tests can substitute a recording fake without needing an emulator or
/// live broker connection.
/// </summary>
/// <remarks>
/// All members are scheduled to fire at <see cref="Azure.Messaging.ServiceBus.ServiceBusMessage.ScheduledEnqueueTime"/>;
/// this interface is intentionally narrow because the short-term path does exactly
/// one operation. The long-term deferral path does NOT use this interface \u2014
/// entries there are stored in the durable <see cref="Talaria.Core.Abstractions.IDeferralStore"/>
/// and republished by the deferral sweeper via the regular
/// <see cref="Talaria.Core.Abstractions.IProducer{T}"/>.
/// </remarks>
/// <since>1.0.0</since>
internal interface IServiceBusMessageScheduler
{
    /// <summary>
    /// Schedules a message for broker-side deferred delivery.
    /// </summary>
    /// <param name="topic">Queue or topic name to schedule the message to.</param>
    /// <param name="body">
    /// Binary payload. The adapter always wraps the JSON string from
    /// <see cref="Talaria.Core.Abstractions.DeferredMessage.PayloadJson"/> into a
    /// <see cref="BinaryData"/>; we never serialize here.
    /// </param>
    /// <param name="applicationProperties">
    /// Flattened key/value view of <see cref="Talaria.Core.Abstractions.MessageHeaders"/>. Null values
    /// are skipped so ASB does not reject a property whose value type is unsupported.
    /// </param>
    /// <param name="scheduledEnqueueTime">
    /// UTC instant at which the broker should make the message available. Past times
    /// are accepted by ASB (immediate enqueue); future times trigger broker-side
    /// holding until the deadline.
    /// </param>
    /// <param name="partitionKey">
    /// Optional partition key. Transports that support sessions map this to
    /// <see cref="Azure.Messaging.ServiceBus.ServiceBusMessage.SessionId"/> so the
    /// scheduled message is received on the same session as the original delivery.
    /// </param>
    /// <param name="ct">Cancellation token; cancels the broker call.</param>
    /// <returns>The broker-assigned sequence number.</returns>
    Task<long> ScheduleAsync(
        string topic,
        BinaryData body,
        IReadOnlyDictionary<string, object> applicationProperties,
        DateTimeOffset scheduledEnqueueTime,
        string? partitionKey,
        CancellationToken ct = default);
}
