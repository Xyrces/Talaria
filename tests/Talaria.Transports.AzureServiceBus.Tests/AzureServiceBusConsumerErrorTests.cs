// SPDX-License-Identifier: Apache-2.0

using System.Reflection;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Logging.Abstractions;
using Talaria.Core.Abstractions;
using Xunit;

namespace Talaria.Transports.AzureServiceBus.Tests;

/// <summary>
/// Unit-level coverage for fatal processor error propagation. These tests do not
/// require the ASB emulator: they construct a <see cref="AzureServiceBusConsumer{T}"/>
/// with a fake connection string, simulate the SDK raising a processor error via
/// reflection (the SDK event is public but raising it from code is only possible
/// via a test helper or a real processor), and assert the channel is completed
/// with the exception so the host's supervised loop can restart with backoff.
/// </summary>
public class AzureServiceBusConsumerErrorTests
{
    private const string FakeConnectionString =
        "Endpoint=sb://fake.servicebus.windows.net/;SharedAccessKeyName=x;SharedAccessKey=eQ==";

    private static AzureServiceBusConsumer<string> CreateConsumer()
    {
        var client = new ServiceBusClient(FakeConnectionString);
        var processor = client.CreateProcessor("topic", "subscription");
        var sender = client.CreateSender("topic-dlq");

        return new AzureServiceBusConsumer<string>(
            processor,
            sender,
            topic: "topic",
            dlqEntity: "topic-dlq",
            bufferCapacity: 10,
            includeDlqExceptionDetails: false,
            logger: NullLogger<AzureServiceBusConsumer<string>>.Instance);
    }

    /// <summary>
    /// Simulates raising <see cref="ServiceBusProcessor.ProcessErrorAsync"/> on
    /// behalf of the SDK. The event args type is internal, but its constructor is
    /// public; we use reflection to create it and invoke the registered handler.
    /// </summary>
    private static async Task RaiseProcessorErrorAsync(AzureServiceBusConsumer<string> consumer, Exception exception, string errorSource = "Receive")
    {
        var processor = GetProcessor(consumer);

        // ProcessErrorAsync uses ProcessErrorEventArgs with a public .ctor:
        // public ProcessErrorEventArgs(Exception exception, ServiceBusErrorSource errorSource,
        //     string fullyQualifiedNamespace, string entityPath)
        var argsType = typeof(ProcessErrorEventArgs);
        var args = Activator.CreateInstance(
            argsType,
            BindingFlags.Instance | BindingFlags.Public,
            binder: null,
            args: new object[] { exception, Enum.Parse(typeof(ServiceBusErrorSource), errorSource), "sb://fake.servicebus.windows.net", "topic", "identifier", CancellationToken.None },
            culture: null)!;

        // The handler field is private; retrieve it from ServiceBusProcessor.
        var errorEventField = typeof(ServiceBusProcessor).GetField(
            "_processErrorAsync",
            BindingFlags.Instance | BindingFlags.NonPublic)!;
        var handler = (Func<ProcessErrorEventArgs, Task>)errorEventField.GetValue(processor)!;

        await handler((ProcessErrorEventArgs)args);
    }

    private static ServiceBusProcessor GetProcessor(AzureServiceBusConsumer<string> consumer)
    {
        var field = typeof(AzureServiceBusConsumer<string>).GetField("_processor", BindingFlags.Instance | BindingFlags.NonPublic)!;
        return (ServiceBusProcessor)field.GetValue(consumer)!;
    }

    [Fact]
    public async Task NonTransient_ServiceBusException_CompletesChannel_WithException()
    {
        // Arrange
        var consumer = CreateConsumer();
        using var cts = new CancellationTokenSource();
        var enumerable = consumer.ConsumeAsync(cts.Token);
        var enumerator = enumerable.GetAsyncEnumerator();
        var moveTask = enumerator.MoveNextAsync().AsTask();

        // Act: simulate a fatal error (GeneralError is non-transient).
        var fatal = new ServiceBusException("link detached", ServiceBusFailureReason.GeneralError);
        await RaiseProcessorErrorAsync(consumer, fatal);

        // Assert: the enumeration should fault with the fatal exception.
        var ex = await Assert.ThrowsAsync<ServiceBusException>(() => moveTask);
        Assert.False(ex.IsTransient);

        // Clean up.
        cts.Cancel();
        try { await consumer.DisposeAsync(); } catch { /* best effort */ }
    }

    [Fact]
    public async Task Transient_ServiceBusException_DoesNotCompleteChannel()
    {
        // Arrange
        var consumer = CreateConsumer();
        using var cts = new CancellationTokenSource();
        var enumerable = consumer.ConsumeAsync(cts.Token);
        var enumerator = enumerable.GetAsyncEnumerator();
        var moveTask = enumerator.MoveNextAsync();

        // Act: simulate a transient broker error.
        var transient = new ServiceBusException("server busy", ServiceBusFailureReason.ServiceBusy);
        await RaiseProcessorErrorAsync(consumer, transient);

        // Assert: the channel is still open and the enumeration is still waiting.
        Assert.False(moveTask.IsCompleted);

        // Clean up.
        cts.Cancel();
        try { await consumer.DisposeAsync(); } catch { /* best effort */ }
    }

    [Fact]
    public async Task NonServiceBusException_CompletesChannel_WithException()
    {
        // Arrange
        var consumer = CreateConsumer();
        using var cts = new CancellationTokenSource();
        var enumerable = consumer.ConsumeAsync(cts.Token);
        var enumerator = enumerable.GetAsyncEnumerator();
        var moveTask = enumerator.MoveNextAsync().AsTask();

        // Act: simulate an unexpected non-ServiceBus exception.
        var fatal = new InvalidOperationException("unexpected SDK failure");
        await RaiseProcessorErrorAsync(consumer, fatal);

        // Assert: the enumeration should fault with the wrapped exception.
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => moveTask);
        Assert.Equal("unexpected SDK failure", ex.Message);

        // Clean up.
        cts.Cancel();
        try { await consumer.DisposeAsync(); } catch { /* best effort */ }
    }
}
