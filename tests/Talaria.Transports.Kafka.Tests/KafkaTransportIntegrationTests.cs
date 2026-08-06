using System.Threading.Channels;
using Microsoft.Extensions.DependencyInjection;
using Talaria.Core.Abstractions;
using Talaria.Core.Registration;
using Testcontainers.Kafka;
using Xunit;

namespace Talaria.Transports.Kafka.Tests;

public class KafkaTransportIntegrationTests : IAsyncLifetime
{
    private KafkaContainer _kafkaContainer;
    private IServiceProvider _serviceProvider;

    public async Task InitializeAsync()
    {
        if (!DockerFactAttribute.IsDockerRunning()) return;

        _kafkaContainer = new KafkaBuilder()
            .WithImage("confluentinc/cp-kafka:7.4.0")
            .Build();

        await _kafkaContainer.StartAsync();

        var services = new ServiceCollection();
        var builder = services.AddTalaria();
        builder.UseKafkaTransport(opts =>
        {
            opts.BootstrapServers = _kafkaContainer.GetBootstrapAddress();
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
    public async Task ProducerAndConsumer_RoundtripMessage_Successfully()
    {
        var transport = _serviceProvider.GetRequiredService<ITransport>();
        
        string topic = $"test-topic-{Guid.NewGuid():N}";
        
        var producer = await transport.CreateProducerAsync<string>(topic, new ProducerOptions());
        var consumer = await transport.CreateConsumerAsync<string>(topic, new ConsumerOptions { ConsumerGroup = "test-group" });

        var testMessage = "Hello Kafka";
        
        // Setup tracing explicitly to see if W3C carries through
        var headers = new MessageHeaders
        {
            TraceParent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
        };
        
        await producer.ProduceAsync(testMessage, headers);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        
        MessageEnvelope<string>? received = null;
        
        await foreach (var env in consumer.ConsumeAsync(cts.Token))
        {
            received = env;
            await consumer.CommitAsync(env);
            break;
        }

        Assert.NotNull(received);
        Assert.Equal(testMessage, received.Payload);
        Assert.Equal("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", received.Headers.TraceParent);
        Assert.Equal(topic, received.SourceTopic);
    }
}
