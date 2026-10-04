// SPDX-License-Identifier: Apache-2.0
using Talaria.Core.Abstractions;
using Talaria.Core.Registration;

namespace Talaria.Core.Hosting;

internal sealed class MessageBus(ProducerCache producers, TopicRegistry registry, IMessageOutbox? outbox = null) : IMessageBus
{
    public Task SendAsync<T>(T message, CancellationToken ct = default) => Produce(Resolve<T>(TopologyEntityKind.Queue), message, false, ct);
    public Task PublishAsync<T>(T message, CancellationToken ct = default) => Produce(Resolve<T>(TopologyEntityKind.Topic), message, true, ct);
    public Task SendAsync<T>(string destination, T message, CancellationToken ct = default) => Produce(destination, message, false, ct);
    public Task PublishAsync<T>(string destination, T message, CancellationToken ct = default) => Produce(destination, message, true, ct);

    private string Resolve<T>(TopologyEntityKind kind)
    {
        var routes = registry.Registrations.Where(r => r.IsMinimalEndpoint && r.MessageType == typeof(T) && r.EntityKind == kind)
            .Select(r => r.TopicName).Distinct().ToArray();
        if (routes.Length > 1) throw new InvalidOperationException($"Message {typeof(T).Name} has multiple destinations. Specify the destination explicitly.");
        return routes.SingleOrDefault() ?? MessageNames.Destination(MessageNames.Contract(typeof(T)));
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
