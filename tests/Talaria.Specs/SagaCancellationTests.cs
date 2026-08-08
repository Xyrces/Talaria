using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Talaria.Core;
using Talaria.Core.Abstractions;
using Talaria.Core.Hosting;
using Talaria.Core.Registration;
using Talaria.Transports.InMemory;
using Xunit;

namespace Talaria.Specs.Tests;

public class SagaCancellationTests
{
    private class CancelMessage { public string Id { get; set; } = ""; }

    [Fact]
    public async Task StopAsync_CompletesCleanly_WhenBlockingHandlerIsSignalledDuringShutdown()
    {
        var transport = new InMemoryTransport();

        var handlerEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handlerRelease = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var topicReg = new TopicRegistry();
        topicReg.Add(new TopicRegistration
        {
            TopicName = "cancel-topic",
            MessageType = typeof(CancelMessage),
            Handler = async (msg, headers, ct) =>
            {
                // Block mid-handler until the test signals completion.
                handlerEntered.TrySetResult();
                await handlerRelease.Task;
            }
        });

        var services = new ServiceCollection().BuildServiceProvider();
        var hostedService = new TalariaHostedService(
            transport,
            topicReg,
            Options.Create(new TalariaOptions { ApplicationName = "test-app" }),
            NullLogger<TalariaHostedService>.Instance,
            services);

        using var cts = new CancellationTokenSource();
        await hostedService.StartAsync(cts.Token);

        var producer = await transport.CreateProducerAsync<CancelMessage>("cancel-topic", new ProducerOptions());
        await producer.ProduceAsync(new CancelMessage { Id = "m1" });

        // Wait until the handler is genuinely blocked mid-processing.
        var entered = await Task.WhenAny(handlerEntered.Task, Task.Delay(TimeSpan.FromSeconds(10))) == handlerEntered.Task;
        Assert.True(entered, "The handler was never entered.");

        // Signal the handler to complete, then stop: shutdown must finish quickly
        // and must not throw (a hung consumer loop would trip the timeout CTS).
        handlerRelease.TrySetResult();

        var sw = Stopwatch.StartNew();
        using var stopCts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await hostedService.StopAsync(stopCts.Token);
        sw.Stop();

        Assert.True(sw.Elapsed < TimeSpan.FromSeconds(5), $"StopAsync took too long: {sw.Elapsed}.");
    }
}
