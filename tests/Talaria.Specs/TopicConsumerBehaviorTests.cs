// SPDX-License-Identifier: Apache-2.0

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Talaria.Core;
using Talaria.Core.Abstractions;
using Talaria.Core.Hosting;
using Talaria.Core.Registration;
using Talaria.Core.Sagas;
using Talaria.Transports.InMemory;

namespace Talaria.Specs.Tests;

/// <summary>
/// End-to-end behavior tests for class-based topic consumers driven through a
/// manually-started <see cref="TalariaListener"/>.
/// </summary>
public class TopicConsumerBehaviorTests
{
    private class ConsumerMessage { public string Id { get; set; } = ""; }

    private class ScopedCounter
    {
        public int InstanceId { get; }

        public ScopedCounter()
        {
            InstanceId = Interlocked.Increment(ref _nextId);
        }

        private static int _nextId;
    }

    private class RecordingConsumer : ITopicConsumer<ConsumerMessage>
    {
        private readonly ScopedCounter _counter;
        private readonly List<(string Id, int InstanceId)> _received;

        public RecordingConsumer(ScopedCounter counter, List<(string, int)> received)
        {
            _counter = counter;
            _received = received;
        }

        public Task ConsumeAsync(ConsumeContext<ConsumerMessage> context)
        {
            _received.Add((context.Message.Id, _counter.InstanceId));
            return Task.CompletedTask;
        }
    }

    private class ThrowingConsumer : ITopicConsumer<ConsumerMessage>
    {
        public Task ConsumeAsync(ConsumeContext<ConsumerMessage> context)
        {
            throw new InvalidOperationException("handler failure");
        }
    }

    [Fact]
    public async Task Class_Consumer_Processes_Message_EndToEnd()
    {
        var transport = new InMemoryTransport();
        var received = new List<(string Id, int InstanceId)>();

        var services = new ServiceCollection()
            .AddSingleton(transport)
            .AddSingleton(received)
            .AddScoped<ScopedCounter>()
            .AddScoped<RecordingConsumer>()
            .BuildServiceProvider();

        var topicReg = new TopicRegistry();
        topicReg.MapTopic<ConsumerMessage, RecordingConsumer>("class-consumer.topic");

        var listener = new TalariaListener(
            transport,
            topicReg,
            new SagaRegistry(),
            new TalariaOptions { ApplicationName = "class-consumer-app" },
            NullLogger<TalariaListener>.Instance,
            services);

        await listener.StartAsync();

        var producer = await transport.CreateProducerAsync<ConsumerMessage>("class-consumer.topic", new ProducerOptions());
        await producer.ProduceAsync(new ConsumerMessage { Id = "class-1" });

        var processed = await TestAsyncHelpers.PollUntilAsync(
            () => Task.FromResult(received.Count == 1),
            TimeSpan.FromSeconds(5));
        Assert.True(processed, "Message was not processed by the class consumer.");
        Assert.Equal("class-1", received[0].Id);

        await listener.StopAsync();
    }

    [Fact]
    public async Task Class_Consumer_Creates_Per_Message_Scope()
    {
        var transport = new InMemoryTransport();
        var received = new List<(string Id, int InstanceId)>();

        var services = new ServiceCollection()
            .AddSingleton(transport)
            .AddSingleton(received)
            .AddScoped<ScopedCounter>()
            .AddScoped<RecordingConsumer>()
            .BuildServiceProvider();

        var topicReg = new TopicRegistry();
        topicReg.MapTopic<ConsumerMessage, RecordingConsumer>("class-scope.topic");

        var listener = new TalariaListener(
            transport,
            topicReg,
            new SagaRegistry(),
            new TalariaOptions { ApplicationName = "class-consumer-app" },
            NullLogger<TalariaListener>.Instance,
            services);

        await listener.StartAsync();

        var producer = await transport.CreateProducerAsync<ConsumerMessage>("class-scope.topic", new ProducerOptions());
        for (int i = 0; i < 3; i++)
        {
            await producer.ProduceAsync(new ConsumerMessage { Id = $"scope-{i}" });
        }

        var processed = await TestAsyncHelpers.PollUntilAsync(
            () => Task.FromResult(received.Count == 3),
            TimeSpan.FromSeconds(5));
        Assert.True(processed, "Not all messages were processed.");

        var instanceIds = received.Select(r => r.InstanceId).Distinct().ToList();
        Assert.Equal(3, instanceIds.Count);

        await listener.StopAsync();
    }

    [Fact]
    public async Task Class_Consumer_Without_ServiceProvider_Throws_At_StartAsync()
    {
        var transport = new InMemoryTransport();

        var topicReg = new TopicRegistry();
        topicReg.MapTopic<ConsumerMessage, RecordingConsumer>("class-no-sp.topic");

        var listener = new TalariaListener(
            transport,
            topicReg,
            new SagaRegistry(),
            new TalariaOptions { ApplicationName = "class-consumer-app" },
            NullLogger<TalariaListener>.Instance);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => listener.StartAsync());
        Assert.Contains("IServiceProvider", ex.Message);
    }

    [Fact]
    public async Task Class_Consumer_Exception_Retries_Then_DeadLetters_With_Retries_Exhausted()
    {
        var transport = new InMemoryTransport();
        var deferralStore = new InMemoryDeferralStore();

        var services = new ServiceCollection()
            .AddSingleton(transport)
            .AddScoped<ThrowingConsumer>()
            .BuildServiceProvider();

        var topicReg = new TopicRegistry();
        topicReg.MapTopic<ConsumerMessage, ThrowingConsumer>("class-retry.topic", new RetryPolicy
        {
            MaxRetryAttempts = 1,
            RetryInterval = TimeSpan.FromMilliseconds(10),
        });

        var listener = new TalariaListener(
            transport,
            topicReg,
            new SagaRegistry(),
            new TalariaOptions
            {
                ApplicationName = "class-consumer-app",
                DeferralBackoff = TimeSpan.FromMilliseconds(10),
            },
            NullLogger<TalariaListener>.Instance,
            services,
            new TalariaListenerStores(DeferralStore: deferralStore));

        await listener.StartAsync();

        var producer = await transport.CreateProducerAsync<ConsumerMessage>("class-retry.topic", new ProducerOptions());
        await producer.ProduceAsync(new ConsumerMessage { Id = "retry-1" });

        var dlqMessages = await TestAsyncHelpers.ReadUntilAsync<ConsumerMessage>(
            transport,
            "class-retry.topic.dlq",
            1,
            TimeSpan.FromSeconds(10));

        var envelope = Assert.Single(dlqMessages);
        Assert.Equal("retry-1", envelope.Payload!.Id);
        Assert.Equal("retries_exhausted", envelope.Headers.DlqReason);

        await listener.StopAsync();
    }
}
