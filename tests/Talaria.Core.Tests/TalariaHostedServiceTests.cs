using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Talaria.Core;
using Talaria.Core.Abstractions;
using Talaria.Core.Hosting;
using Talaria.Core.Registration;
using Talaria.Transports.InMemory;

namespace Talaria.Core.Tests;

public class TalariaHostedServiceTests
{
    [Fact]
    public async Task HopCount_Exceeded_Routes_To_DLQ()
    {
        // Arrange
        var transport = new InMemoryTransport();
        var received = new List<string>();

        var builder = Host.CreateDefaultBuilder();
        builder.ConfigureServices(services =>
        {
            services.AddTalaria(opts =>
            {
                opts.MaxHopCount = 3;
                opts.ApplicationName = "test-app";
            }).UseInMemoryTransport(transport);
        });

        var host = builder.Build();

        host.Services.MapTopic<TestMessage>("test.topic", (msg, ct) =>
        {
            received.Add(msg.Id);
            return Task.CompletedTask;
        });

        // Publish a message with hop count = 3 (>= MaxHopCount of 3)
        var producer = await transport.CreateProducerAsync<TestMessage>(
            "test.topic", new ProducerOptions());
        var headers = new MessageHeaders { HopCount = 3 };
        await producer.ProduceAsync(new TestMessage("MSG-HOP"), headers);

        // Act
        await host.StartAsync();
        await Task.Delay(1000);

        // Assert — handler should NOT have been called
        Assert.Empty(received);

        // DLQ should have the message
        var dlqMessages = await transport.ReadAllFromTopicAsync<TestMessage>("test.topic.dlq");
        Assert.Single(dlqMessages);
        Assert.Equal("MSG-HOP", dlqMessages[0].Payload.Id);

        await host.StopAsync();
        host.Dispose();
    }

    [Fact]
    public async Task Handler_Failure_Routes_To_DLQ()
    {
        var transport = new InMemoryTransport();

        var builder = Host.CreateDefaultBuilder();
        builder.ConfigureServices(services =>
        {
            services.AddTalaria(opts => opts.ApplicationName = "test-app")
                    .UseInMemoryTransport(transport);
        });

        var host = builder.Build();

        host.Services.MapTopic<TestMessage>("test.topic", (msg, ct) =>
            throw new InvalidOperationException("boom"));

        var producer = await transport.CreateProducerAsync<TestMessage>(
            "test.topic", new ProducerOptions());
        await producer.ProduceAsync(new TestMessage("MSG-FAIL"));

        await host.StartAsync();
        await Task.Delay(1000);

        var dlqMessages = await transport.ReadAllFromTopicAsync<TestMessage>("test.topic.dlq");
        Assert.Single(dlqMessages);

        await host.StopAsync();
        host.Dispose();
    }

    [Fact]
    public async Task Successful_Handler_Processes_Message()
    {
        var transport = new InMemoryTransport();
        var received = new List<string>();

        var builder = Host.CreateDefaultBuilder();
        builder.ConfigureServices(services =>
        {
            services.AddTalaria(opts => opts.ApplicationName = "test-app")
                    .UseInMemoryTransport(transport);
        });

        var host = builder.Build();

        host.Services.MapTopic<TestMessage>("test.topic", (msg, ct) =>
        {
            received.Add(msg.Id);
            return Task.CompletedTask;
        });

        var producer = await transport.CreateProducerAsync<TestMessage>(
            "test.topic", new ProducerOptions());
        await producer.ProduceAsync(new TestMessage("MSG-OK"));

        await host.StartAsync();
        await Task.Delay(500);

        Assert.Single(received);
        Assert.Equal("MSG-OK", received[0]);

        await host.StopAsync();
        host.Dispose();
    }

    [Fact]
    public async Task MapTopic_AfterStart_ThrowsInvalidOperationException()
    {
        var transport = new InMemoryTransport();

        var builder = Host.CreateDefaultBuilder();
        builder.ConfigureServices(services =>
        {
            services.AddTalaria(opts => opts.ApplicationName = "test-app")
                    .UseInMemoryTransport(transport);
        });

        var host = builder.Build();

        await host.StartAsync();

        var ex = Assert.Throws<InvalidOperationException>(() =>
            host.Services.MapTopic<TestMessage>("test.topic", (msg, ct) => Task.CompletedTask));

        Assert.Contains("MapTopic", ex.Message);
        Assert.Contains("before the host runs", ex.Message);

        await host.StopAsync();
        host.Dispose();
    }

    [Fact]
    public async Task MapSaga_AfterStart_ThrowsInvalidOperationException()
    {
        var transport = new InMemoryTransport();

        var builder = Host.CreateDefaultBuilder();
        builder.ConfigureServices(services =>
        {
            services.AddTalaria(opts => opts.ApplicationName = "test-app")
                    .UseInMemoryTransport(transport);
        });

        var host = builder.Build();

        await host.StartAsync();

        var ex = Assert.Throws<InvalidOperationException>(() =>
            host.Services.MapSaga<AfterStartSagaState>(s =>
                s.StartedBy<AfterStartSagaMessage>("after-start.saga",
                    (msg, ctx) => Task.FromResult(ctx.Transition(new AfterStartSagaState { Id = msg.Id })),
                    m => m.Id)));

        Assert.Contains("MapSaga", ex.Message);
        Assert.Contains("before the host runs", ex.Message);

        await host.StopAsync();
        host.Dispose();
    }

    [Fact]
    public async Task HostedService_Adapters_Forward_Lifecycle_To_Listener()
    {
        var transport = new InMemoryTransport();

        var builder = Host.CreateDefaultBuilder();
        builder.ConfigureServices(services =>
        {
            services.AddTalaria(opts => opts.ApplicationName = "test-app")
                    .UseInMemoryTransport(transport);
        });

        var host = builder.Build();

        host.Services.MapTopic<TestMessage>("adapter.topic", (msg, ct) => Task.CompletedTask);

        var listener = host.Services.GetRequiredService<TalariaListener>();
        Assert.False(listener.IsRunning);

        await host.StartAsync();
        Assert.True(listener.IsRunning);

        await host.StopAsync();
        Assert.False(listener.IsRunning);

        host.Dispose();
    }
}

public record TestMessage(string Id);

public class AfterStartSagaState
{
    public string Id { get; set; } = "";
}

public class AfterStartSagaMessage
{
    public string Id { get; set; } = "";
}
