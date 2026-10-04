// SPDX-License-Identifier: Apache-2.0
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Talaria.Core.Abstractions;

namespace Talaria.Core.Registration;

public static class MinimalMessagingExtensions
{
    public static MessageEndpointBuilder MapCommand<T>(this IHost host, Delegate handler) => host.Services.MapCommand<T>(handler);
    public static MessageEndpointBuilder MapEvent<T>(this IHost host, Delegate handler) => host.Services.MapEvent<T>(handler);
    public static MessageEndpointBuilder MapCommand<T>(this IServiceProvider services, Delegate handler) => Map<T>(services, handler, TopologyEntityKind.Queue);
    public static MessageEndpointBuilder MapEvent<T>(this IServiceProvider services, Delegate handler) => Map<T>(services, handler, TopologyEntityKind.Topic);

    private static MessageEndpointBuilder Map<T>(IServiceProvider services, Delegate handler, TopologyEntityKind kind)
    {
        var registry = services.GetRequiredService<TopicRegistry>();
        var invoke = HandlerBinder.Bind<T>(handler, services);
        var contract = MessageNames.Contract(typeof(T));
        var destination = MessageNames.Destination(contract);
        var options = services.GetRequiredService<TalariaOptions>();
        var group = kind == TopologyEntityKind.Queue ? "command." + destination
            : MessageNames.Destination(options.ApplicationName + "." + contract);
        var registration = new TopicRegistration
        {
            TopicName = destination,
            MessageType = typeof(T),
            ConsumerGroup = group,
            EntityKind = kind,
            IsMinimalEndpoint = true,
            ScopedHandler = (payload, headers, metadata, scopedServices, ct) => invoke(new ConsumeContext<T>
            {
                Envelope = new MessageEnvelope<T>
                {
                    Payload = (T)payload, Headers = headers, SourceTopic = metadata.SourceTopic,
                    PartitionKey = metadata.PartitionKey, Partition = metadata.Partition,
                    Offset = metadata.Offset, Timestamp = metadata.Timestamp, CorrelationId = metadata.CorrelationId,
                },
                CancellationToken = ct,
                Services = scopedServices,
            }),
        };
        registry.Add(registration);
        return new MessageEndpointBuilder(registry, registration);
    }
}
