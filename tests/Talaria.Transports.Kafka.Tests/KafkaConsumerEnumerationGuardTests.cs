// SPDX-License-Identifier: Apache-2.0

using Microsoft.Extensions.DependencyInjection;
using Talaria.Core.Abstractions;
using Talaria.Core.Registration;
using Testcontainers.Kafka;
using Xunit;

namespace Talaria.Transports.Kafka.Tests;

/// <summary>
/// Verifies the single-enumeration contract of <see cref="IConsumer{T}.ConsumeAsync"/>
/// against a real Kafka broker. The guard must trip on a second enumeration while still
/// allowing the legitimate single-enumeration path used by the hosted service.
/// </summary>
public class KafkaConsumerEnumerationGuardTests : IAsyncLifetime
{
    private KafkaContainer? _kafkaContainer;
    private IServiceProvider _serviceProvider = null!;

    public async Task InitializeAsync()
    {
        if (!DockerFactAttribute.IsDockerRunning()) return;

        _kafkaContainer = new KafkaBuilder("confluentinc/cp-kafka:7.4.0")
            .Build();

        await _kafkaContainer.StartAsync();

        var services = new ServiceCollection();
        var builder = services.AddTalaria();
        builder.UseKafkaTransport(opts =>
        {
            opts.BootstrapServers = _kafkaContainer!.GetBootstrapAddress();
            opts.BaseConsumerConfig.AutoOffsetReset = Confluent.Kafka.AutoOffsetReset.Earliest;
        });

        _serviceProvider = services.BuildServiceProvider();
    }

    public async Task DisposeAsync()
    {
        if (_kafkaContainer != null)
        {
            await _kafkaContainer.DisposeAsync();
        }
    }

    [DockerFact]
    public async Task FirstEnumerationWorks_SecondThrows()
    {
        var transport = _serviceProvider.GetRequiredService<ITransport>();
        string topic = $"test-guard-{Guid.NewGuid():N}";

        var producer = await transport.CreateProducerAsync<string>(topic, new ProducerOptions());
        await using var consumer = await transport.CreateConsumerAsync<string>(
            topic,
            new ConsumerOptions { ConsumerGroup = $"guard-group-{Guid.NewGuid():N}" });

        await producer.ProduceAsync("guard-message", new MessageHeaders { MessageId = "g-1" });

        // First enumeration yields the message normally.
        MessageEnvelope<string>? received = null;
        await foreach (var env in consumer.ConsumeAsync(CancellationToken.None))
        {
            received = env;
            await consumer.CommitAsync(env);
            break;
        }
        Assert.NotNull(received);
        Assert.Equal("guard-message", received!.Payload);

        // A second enumeration on the same instance is forbidden by contract.
        var ex = Assert.Throws<InvalidOperationException>(() => consumer.ConsumeAsync().GetAsyncEnumerator());
        Assert.Equal(
            "ConsumeAsync may only be enumerated once per consumer instance. Create a new consumer to restart consumption.",
            ex.Message);
    }

    [DockerFact]
    public async Task SecondEnumerationBeforeFirstMoveNext_Throws()
    {
        var transport = _serviceProvider.GetRequiredService<ITransport>();
        string topic = $"test-guard-pre-{Guid.NewGuid():N}";

        var producer = await transport.CreateProducerAsync<string>(topic, new ProducerOptions());
        await producer.ProduceAsync("pre-message", new MessageHeaders { MessageId = "g-pre-1" });

        await using var consumer = await transport.CreateConsumerAsync<string>(
            topic,
            new ConsumerOptions { ConsumerGroup = $"guard-pre-group-{Guid.NewGuid():N}" });

        // Starting the first enumeration is allowed...
        var first = consumer.ConsumeAsync().GetAsyncEnumerator();
        Assert.NotNull(first);

        // ...but starting a second before the first is advanced is not.
        var ex = Assert.Throws<InvalidOperationException>(() => consumer.ConsumeAsync().GetAsyncEnumerator());
        Assert.Equal(
            "ConsumeAsync may only be enumerated once per consumer instance. Create a new consumer to restart consumption.",
            ex.Message);
    }

    [DockerFact]
    public async Task ReEnumerateReturnedInstance_Throws()
    {
        var transport = _serviceProvider.GetRequiredService<ITransport>();
        string topic = $"test-guard-reenum-{Guid.NewGuid():N}";

        var producer = await transport.CreateProducerAsync<string>(topic, new ProducerOptions());
        await using var consumer = await transport.CreateConsumerAsync<string>(
            topic,
            new ConsumerOptions { ConsumerGroup = $"guard-reenum-group-{Guid.NewGuid():N}" });

        await producer.ProduceAsync("guard-message", new MessageHeaders { MessageId = "g-re-1" });

        // Capture the single IAsyncEnumerable returned by ConsumeAsync.
        var enumerable = consumer.ConsumeAsync();

        // First enumeration yields the message normally.
        MessageEnvelope<string>? received = null;
        await foreach (var env in enumerable)
        {
            received = env;
            await consumer.CommitAsync(env);
            break;
        }
        Assert.NotNull(received);
        Assert.Equal("guard-message", received!.Payload);

        // Re-enumerating the SAME returned instance is also forbidden. For async
        // iterators the guard runs when MoveNextAsync first advances the enumerator.
        var second = enumerable.GetAsyncEnumerator();
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => second.MoveNextAsync().AsTask());
        Assert.Equal(
            "ConsumeAsync may only be enumerated once per consumer instance. Create a new consumer to restart consumption.",
            ex.Message);
    }
}
