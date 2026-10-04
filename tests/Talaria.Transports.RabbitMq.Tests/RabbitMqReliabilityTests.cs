using Talaria.Core.Abstractions;
using Talaria.Transports.RabbitMq;
using Testcontainers.RabbitMq;

namespace Talaria.Transports.RabbitMq.Tests;

public sealed class RabbitMqReliabilityTests : IAsyncLifetime
{
    private RabbitMqContainer? _container;
    private RabbitMqTransport _transport = null!;

    public async Task InitializeAsync()
    {
        if (!DockerFactAttribute.IsDockerRunning())
        {
            if (Environment.GetEnvironmentVariable("TALARIA_REQUIRE_DOCKER") == "1")
                throw new InvalidOperationException("TALARIA_REQUIRE_DOCKER=1 but Docker is unavailable.");
            return;
        }

        _container = new RabbitMqBuilder("rabbitmq:3.13-management-alpine").Build();
        await _container.StartAsync();
        _transport = new RabbitMqTransport(new RabbitMqOptions { ConnectionString = _container.GetConnectionString() });
    }

    public async Task DisposeAsync()
    {
        if (_transport is not null) await _transport.DisposeAsync();
        if (_container is not null) await _container.DisposeAsync();
    }

    [DockerFact]
    public async Task Commands_Compete_While_Event_Subscriptions_FanOut_And_Confirm()
    {
        var queue = Name("commands");
        var topic = Name("events");
        var groupA = Name("group-a");
        var groupB = Name("group-b");
        await _transport.ProvisionAsync([
            new(TopologyEntityKind.Queue, queue),
            new(TopologyEntityKind.Topic, topic),
            new(TopologyEntityKind.Subscription, groupA, topic),
            new(TopologyEntityKind.Subscription, groupB, topic)]);

        var commandConsumerA = await _transport.CreateConsumerAsync<string>(queue,
            new ConsumerOptions { EntityKind = TopologyEntityKind.Queue });
        var commandConsumerB = await _transport.CreateConsumerAsync<string>(queue,
            new ConsumerOptions { EntityKind = TopologyEntityKind.Queue });
        var commandProducer = await _transport.CreateProducerAsync<string>(queue, new ProducerOptions());
        await commandProducer.ProduceAsync("one command"); // returns only after publisher confirm

        var commandA = TryNextAsync(commandConsumerA, TimeSpan.FromSeconds(5));
        var commandB = TryNextAsync(commandConsumerB, TimeSpan.FromSeconds(5));
        var command = await Task.WhenAny(commandA, commandB);
        Assert.Equal("one command", (await command)!.Payload);
        Assert.Null(await (command == commandA ? commandB : commandA));

        await commandConsumerA.DisposeAsync();
        await commandConsumerB.DisposeAsync();

        var eventA = await _transport.CreateConsumerAsync<string>(topic,
            new ConsumerOptions { EntityKind = TopologyEntityKind.Subscription, ConsumerGroup = groupA });
        var eventB = await _transport.CreateConsumerAsync<string>(topic,
            new ConsumerOptions { EntityKind = TopologyEntityKind.Subscription, ConsumerGroup = groupB });
        var eventProducer = await _transport.CreateProducerAsync<string>(topic, new ProducerOptions());
        await eventProducer.ProduceAsync("one event");

        var receivedA = await TryNextAsync(eventA, TimeSpan.FromSeconds(5));
        var receivedB = await TryNextAsync(eventB, TimeSpan.FromSeconds(5));
        Assert.Equal("one event", receivedA!.Payload);
        Assert.Equal("one event", receivedB!.Payload);
        await eventA.DisposeAsync();
        await eventB.DisposeAsync();
    }

    [DockerFact]
    public async Task DisposedConsumer_RedeliversUnackedMessage_AndPoisonedPayloadGoesToDlq()
    {
        var queue = Name("redelivery");
        await _transport.ProvisionAsync([new(TopologyEntityKind.Queue, queue)]);
        var producer = await _transport.CreateProducerAsync<string>(queue, new ProducerOptions());
        await producer.ProduceAsync("redeliver me");

        var firstConsumer = await _transport.CreateConsumerAsync<string>(queue,
            new ConsumerOptions { EntityKind = TopologyEntityKind.Queue });
        var first = await TryNextAsync(firstConsumer, TimeSpan.FromSeconds(5));
        Assert.Equal("redeliver me", first!.Payload);
        await firstConsumer.DisposeAsync(); // no ack: the broker must make it available again

        var secondConsumer = await _transport.CreateConsumerAsync<string>(queue,
            new ConsumerOptions { EntityKind = TopologyEntityKind.Queue });
        var redelivered = await TryNextAsync(secondConsumer, TimeSpan.FromSeconds(5));
        Assert.Equal("redeliver me", redelivered!.Payload);
        await secondConsumer.CommitAsync(redelivered);
        await secondConsumer.DisposeAsync();

        var poisonConsumer = await _transport.CreateConsumerAsync<int>(queue,
            new ConsumerOptions { EntityKind = TopologyEntityKind.Queue });
        var dlq = await _transport.CreateConsumerAsync<string>(queue + ".dlq",
            new ConsumerOptions { EntityKind = TopologyEntityKind.Queue });
        var poisonRead = TryNextAsync(poisonConsumer, TimeSpan.FromSeconds(5));
        var deadLetterRead = TryNextAsync(dlq, TimeSpan.FromSeconds(10));
        await ((IConsumerReadiness)poisonConsumer).Ready.WaitAsync(TimeSpan.FromSeconds(5));
        await ((IConsumerReadiness)dlq).Ready.WaitAsync(TimeSpan.FromSeconds(5));
        await producer.ProduceAsync("not-an-integer");
        Assert.Null(await poisonRead);
        var deadLetter = await deadLetterRead;
        Assert.NotNull(deadLetter);
        Assert.Equal("not-an-integer", deadLetter!.Payload);
        await dlq.CommitAsync(deadLetter);
        await poisonConsumer.DisposeAsync();
        await dlq.DisposeAsync();
    }

    private static string Name(string suffix) => $"talaria-{suffix}-{Guid.NewGuid():N}";

    private static async Task<MessageEnvelope<T>?> TryNextAsync<T>(IConsumer<T> consumer, TimeSpan timeout)
    {
        using var cts = new CancellationTokenSource(timeout);
        try
        {
            await foreach (var message in consumer.ConsumeAsync(cts.Token)) return message;
        }
        catch (OperationCanceledException) when (cts.IsCancellationRequested) { }
        return null;
    }
}
