// SPDX-License-Identifier: Apache-2.0
using Talaria.Core.Abstractions;
using Talaria.Core.Registration;

namespace Talaria.Core.Hosting;

internal sealed class MessageBus(
    ProducerCache producers,
    TopicRegistry registry,
    IMessageOutbox? outbox = null,
    IDeferralStore? deferralStore = null,
    TimeProvider? timeProvider = null) : IMessageBus
{
    public Task SendAsync<T>(T message, CancellationToken ct = default) => Produce(Resolve<T>(TopologyEntityKind.Queue), message, false, ct);
    public Task PublishAsync<T>(T message, CancellationToken ct = default) => Produce(Resolve<T>(TopologyEntityKind.Topic), message, true, ct);
    public Task SendAsync<T>(string destination, T message, CancellationToken ct = default) => Produce(destination, message, false, ct);
    public Task PublishAsync<T>(string destination, T message, CancellationToken ct = default) => Produce(destination, message, true, ct);
    public Task ScheduleSendAsync<T>(T message, TimeSpan delay, CancellationToken ct = default) => Schedule(Resolve<T>(TopologyEntityKind.Queue), message, delay, false, ct);
    public Task ScheduleSendAsync<T>(T message, DateTimeOffset dueAt, CancellationToken ct = default) => Schedule(Resolve<T>(TopologyEntityKind.Queue), message, dueAt, false, ct);
    public Task ScheduleSendAsync<T>(string destination, T message, TimeSpan delay, CancellationToken ct = default) => Schedule(destination, message, delay, false, ct);
    public Task ScheduleSendAsync<T>(string destination, T message, DateTimeOffset dueAt, CancellationToken ct = default) => Schedule(destination, message, dueAt, false, ct);
    public Task SchedulePublishAsync<T>(T message, TimeSpan delay, CancellationToken ct = default) => Schedule(Resolve<T>(TopologyEntityKind.Topic), message, delay, true, ct);
    public Task SchedulePublishAsync<T>(T message, DateTimeOffset dueAt, CancellationToken ct = default) => Schedule(Resolve<T>(TopologyEntityKind.Topic), message, dueAt, true, ct);
    public Task SchedulePublishAsync<T>(string destination, T message, TimeSpan delay, CancellationToken ct = default) => Schedule(destination, message, delay, true, ct);
    public Task SchedulePublishAsync<T>(string destination, T message, DateTimeOffset dueAt, CancellationToken ct = default) => Schedule(destination, message, dueAt, true, ct);

    private string Resolve<T>(TopologyEntityKind kind)
    {
        var routes = registry.Registrations.Where(r => r.IsMinimalEndpoint && r.MessageType == typeof(T) && r.EntityKind == kind)
            .Select(r => r.TopicName).Distinct().ToArray();
        if (routes.Length > 1) throw new InvalidOperationException($"Message {typeof(T).Name} has multiple destinations. Specify the destination explicitly.");
        return routes.SingleOrDefault() ?? MessageNames.Destination(MessageNames.Contract(typeof(T)));
    }

    private Task Schedule<T>(string destination, T message, TimeSpan delay, bool publish, CancellationToken ct)
    {
        if (delay < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(delay));
        }

        var dueAt = (timeProvider ?? TimeProvider.System).GetUtcNow().Add(delay);
        return Schedule(destination, message, dueAt, publish, ct);
    }

    private async Task Schedule<T>(string destination, T message, DateTimeOffset dueAt, bool publish, CancellationToken ct)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(destination);
        ArgumentNullException.ThrowIfNull(message);
        if (outbox is null && deferralStore is null)
        {
            throw new InvalidOperationException(
                "Scheduled messaging requires an IDeferralStore or transactional IMessageOutbox. " +
                "Register persistence via UseInMemory(), UseRedisPersistence(), UseSqlServerPersistence<TDb>(), or a deferral store extension.");
        }

        var now = (timeProvider ?? TimeProvider.System).GetUtcNow();
        var contract = MessageNames.Contract(typeof(T));
        var headers = new MessageHeaders
        {
            MessageId = Guid.NewGuid().ToString("N"),
            ["talaria.intent"] = publish ? "event" : "command",
        };
        headers[MessageHeaders.MessageTypeKey] = contract;
        headers["talaria.scheduled_due_at"] = dueAt.ToString("O");
        if (System.Diagnostics.Activity.Current is { } activity)
        {
            headers.TraceParent = activity.Id;
            headers.TraceState = activity.TraceStateString;
        }

        var payloadJson = System.Text.Json.JsonSerializer.Serialize(message);
        if (outbox is not null)
        {
            await outbox.StageAsync(
                new OutboxMessage(Guid.NewGuid(), destination, contract, payloadJson, headers, now, null, ScheduledAt: dueAt),
                ct);
        }
        else
        {
            await deferralStore!.EnqueueAsync(
                new DeferredMessage(Guid.NewGuid(), destination, contract, payloadJson, headers, CorrelationId: null, Attempt: 1, DueAt: dueAt, PartitionKey: null),
                ct);
        }

        Diagnostics.TalariaDiagnostics.MessagesScheduled.Add(
            1,
            new KeyValuePair<string, object?>("messaging.destination.name", destination));
    }

    private async Task Produce<T>(string destination, T message, bool publish, CancellationToken ct)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(destination);
        ArgumentNullException.ThrowIfNull(message);
        var headers = new MessageHeaders { MessageId = Guid.NewGuid().ToString("N"), ["talaria.intent"] = publish ? "event" : "command" };
        headers[MessageHeaders.MessageTypeKey] = MessageNames.Contract(typeof(T));
        if (System.Diagnostics.Activity.Current is { } activity)
        {
            headers.TraceParent = activity.Id;
            headers.TraceState = activity.TraceStateString;
        }
        if (outbox is not null)
        {
            await outbox.StageAsync(new OutboxMessage(Guid.NewGuid(), destination, MessageNames.Contract(typeof(T)),
                System.Text.Json.JsonSerializer.Serialize(message), headers, DateTimeOffset.UtcNow, null), ct);
            return;
        }
        var producer = await producers.GetOrCreateAsync(destination, typeof(T), ct);
        await producer.Produce(message, headers, null, ct);
    }
}
