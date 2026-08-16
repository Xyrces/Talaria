// SPDX-License-Identifier: Apache-2.0

using Microsoft.Extensions.DependencyInjection;
using Talaria.Core;
using Talaria.Core.Abstractions;
using Talaria.Core.Hosting;
using Talaria.Core.Registration;
using Talaria.Core.Requesting;
using Talaria.Core.Sagas;
using Testcontainers.Kafka;
using Xunit;

namespace Talaria.Transports.Kafka.Tests;

/// <summary>
/// Request/response integration tests exercising a <c>MapRequest</c> responder over a real Kafka broker.
/// </summary>
public class RequestResponseIntegrationTests : IAsyncLifetime
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
        public async Task Request_Response_RoundTrip_Successfully()
        {
            var transport = _serviceProvider.GetRequiredService<ITransport>();
            var options = new TalariaOptions { ApplicationName = "kafka-rr-test" };

            var requestTopic = $"rr-req-{Guid.NewGuid():N}";
            var registry = new TopicRegistry();
            registry.MapRequest<Ping, Pong>(requestTopic, async (msg, _, _, ct) => new Pong(msg.Value));

            var listener = new TalariaListener(
                transport,
                registry,
                new Talaria.Core.Sagas.SagaRegistry(),
                options,
                Microsoft.Extensions.Logging.Abstractions.NullLogger<TalariaListener>.Instance);

            var loggerFactory = Microsoft.Extensions.Logging.Abstractions.NullLoggerFactory.Instance;
            var factory = new Talaria.Core.Requesting.RequestClientFactory(transport, options, loggerFactory);

            await using (factory.ConfigureAwait(false))
            {
                var client = factory.CreateClient<Ping>(requestTopic);
                await listener.StartAsync();

                var response = await client.GetResponseAsync<Pong>(new Ping("kafka"));

                Assert.Equal("kafka", response.Echo);
                await listener.StopAsync();
            }
        }

        private sealed record Ping(string Value);
        private sealed record Pong(string Echo);
    }
