using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Reqnroll;
using Talaria.Core;
using Talaria.Core.Abstractions;
using Talaria.Core.Registration;
using Talaria.Specs.Messages;
using Talaria.Transports.InMemory;

namespace Talaria.Specs.Steps;

/// <summary>
/// Shared step definitions for all Talaria BDD features.
/// Uses a single InMemoryTransport + IHost per scenario.
/// </summary>
[Binding]
public sealed class TalariaSteps : IAsyncDisposable
{
    private InMemoryTransport _transport = null!;
    private IHost? _host;
    private bool _hostStarted;
    private int _maxHopCount = 32;

    // Tracking
    private readonly List<object> _receivedMessages = new();
    private readonly Dictionary<string, List<object>> _receivedByTopic = new();
    private MessageHeaders? _receivedHeaders;
    private bool _handlerThrows;
    private string? _throwOnTopic;

    // ─── GIVEN ────────────────────────────────────────────────────────

    [Given(@"a Talaria host with an in-memory transport")]
    public void GivenATalariaHostWithAnInMemoryTransport()
    {
        _transport = new InMemoryTransport();
    }

    [Given(@"a Talaria host with an in-memory transport and max hop count of (\d+)")]
    public void GivenATalariaHostWithMaxHopCount(int maxHopCount)
    {
        _transport = new InMemoryTransport();
        _maxHopCount = maxHopCount;
    }

    [Given(@"a handler registered for topic ""(.*)""")]
    public void GivenAHandlerRegisteredForTopic(string topic)
    {
        _receivedByTopic.TryAdd(topic, new List<object>());
    }

    [Given(@"an envelope-aware handler registered for topic ""(.*)""")]
    public void GivenAnEnvelopeAwareHandlerRegisteredForTopic(string topic)
    {
        _receivedByTopic.TryAdd(topic, new List<object>());
    }

    [Given(@"a handler for ""(.*)"" that always throws")]
    public void GivenAHandlerThatAlwaysThrows(string topic)
    {
        _receivedByTopic.TryAdd(topic, new List<object>());
        _handlerThrows = true;
        _throwOnTopic = topic;
    }

    // ─── HOST BUILDING ────────────────────────────────────────────────

    private static readonly System.Diagnostics.ActivityListener _listener = new()
    {
        ShouldListenTo = source => source.Name == "Talaria.Core",
        Sample = (ref System.Diagnostics.ActivityCreationOptions<System.Diagnostics.ActivityContext> _) => System.Diagnostics.ActivitySamplingResult.AllData
    };

    static TalariaSteps()
    {
        System.Diagnostics.ActivitySource.AddActivityListener(_listener);
    }

    private void EnsureHostBuilt()
    {
        if (_host is not null) return;

        var builder = Host.CreateDefaultBuilder();
        builder.ConfigureServices(services =>
        {
            services.AddTalaria(opts =>
            {
                opts.MaxHopCount = _maxHopCount;
                opts.ApplicationName = "test-app";
            }).UseInMemoryTransport(_transport);
        });

        _host = builder.Build();

        // Register all topic handlers
        foreach (var topic in _receivedByTopic.Keys)
        {
            var topicCopy = topic;

            if (_handlerThrows && topicCopy == _throwOnTopic)
            {
                _host.Services.MapTopic<OrderPlaced>(topicCopy, (msg, ct) =>
                    throw new InvalidOperationException("Simulated handler failure"));
            }
            else
            {
                _host.Services.MapTopic<OrderPlaced>(topicCopy, (msg, ct) =>
                {
                    _receivedMessages.Add(msg);
                    _receivedByTopic[topicCopy].Add(msg);
                    return Task.CompletedTask;
                });
            }
        }
    }

    private async Task EnsureHostStarted()
    {
        EnsureHostBuilt();
        if (!_hostStarted)
        {
            await _host!.StartAsync();
            _hostStarted = true;
        }
    }

    // ─── WHEN ─────────────────────────────────────────────────────────

    private async Task WaitUntilAsync(Func<Task<bool>> condition, string description)
    {
        var deadline = DateTime.UtcNow.AddSeconds(10);
        while (!await condition())
        {
            if (DateTime.UtcNow >= deadline)
            {
                throw new TimeoutException($"Timed out waiting for: {description}");
            }

            await Task.Delay(25);
        }
    }

    private Task WaitUntilAsync(Func<bool> condition, string description)
        => WaitUntilAsync(() => Task.FromResult(condition()), description);

    [When(@"a message of type OrderPlaced is published to ""(.*)""")]
    public async Task WhenAMessageOfTypeOrderPlacedIsPublishedTo(string topic)
    {
        EnsureHostBuilt();
        var producer = await _transport.CreateProducerAsync<OrderPlaced>(
            topic, new ProducerOptions());
        await producer.ProduceAsync(new OrderPlaced("ORD-001", 99.99m));
        await EnsureHostStarted();
        await WaitUntilAsync(() => _receivedMessages.Count >= 1, "handler to receive the message");
    }

    [When(@"a message with trace context is published to ""(.*)""")]
    public async Task WhenAMessageWithTraceContextIsPublished(string topic)
    {
        // Build a special host with envelope handler
        if (_host is null)
        {
            var builder = Host.CreateDefaultBuilder();
            builder.ConfigureServices(services =>
            {
                services.AddTalaria(opts => opts.ApplicationName = "test-app")
                        .UseInMemoryTransport(_transport);
            });
            _host = builder.Build();

            _host.Services.MapTopicWithEnvelope<OrderPlaced>(topic, (envelope, ct) =>
            {
                _receivedMessages.Add(envelope.Payload);
                _receivedHeaders = envelope.Headers;
                return Task.CompletedTask;
            });
        }

        var producer = await _transport.CreateProducerAsync<OrderPlaced>(
            topic, new ProducerOptions());

        var headers = new MessageHeaders
        {
            TraceParent = "00-abcdef1234567890abcdef1234567890-1234567890abcdef-01",
            TraceState = "vendor=test",
        };

        await producer.ProduceAsync(new OrderPlaced("ORD-002", 49.99m), headers);
        await EnsureHostStarted();
        await WaitUntilAsync(() => _receivedHeaders is not null, "handler to receive the traced message");
    }

    [When(@"a message is published to ""(.*)""")]
    public async Task WhenAMessageIsPublishedTo(string topic)
    {
        EnsureHostBuilt();
        var producer = await _transport.CreateProducerAsync<OrderPlaced>(
            topic, new ProducerOptions());
        await producer.ProduceAsync(new OrderPlaced($"ORD-{topic}", 10m));
    }

    [When(@"a message with hop count (\d+) is published to ""(.*)""")]
    public async Task WhenAMessageWithHopCountIsPublished(int hopCount, string topic)
    {
        EnsureHostBuilt();
        var producer = await _transport.CreateProducerAsync<OrderPlaced>(
            topic, new ProducerOptions());
        var headers = new MessageHeaders { HopCount = hopCount };
        await producer.ProduceAsync(new OrderPlaced("ORD-HOP", 1m), headers);
    }

    [When(@"the handler has attempted to process the message")]
    public async Task WhenTheHandlerHasAttemptedToProcess()
    {
        await EnsureHostStarted();
        await WaitUntilAsync(async () =>
            _receivedMessages.Count >= 1 ||
            (_throwOnTopic is not null &&
             (await _transport.ReadAllFromTopicAsync<OrderPlaced>(_throwOnTopic + ".dlq")).Count >= 1),
            "handler to attempt processing the message");
    }

    // ─── THEN ─────────────────────────────────────────────────────────

    [Then(@"the handler should be invoked with the OrderPlaced message")]
    public void ThenTheHandlerShouldBeInvoked()
    {
        Assert.Single(_receivedMessages);
        var msg = Assert.IsType<OrderPlaced>(_receivedMessages[0]);
        Assert.Equal("ORD-001", msg.OrderId);
    }

    [Then(@"the handler should receive the message with trace headers")]
    public void ThenTheHandlerShouldReceiveTraceHeaders()
    {
        Assert.NotNull(_receivedHeaders);
        Assert.Equal(
            "00-abcdef1234567890abcdef1234567890-1234567890abcdef-01",
            _receivedHeaders!.TraceParent);
        Assert.Equal("vendor=test", _receivedHeaders.TraceState);
    }

    [Then(@"each handler should process only its own topic messages")]
    public async Task ThenEachHandlerShouldProcessOnlyItsOwnTopicMessages()
    {
        await EnsureHostStarted();
        await WaitUntilAsync(
            () => _receivedByTopic.Values.All(messages => messages.Count >= 1),
            "each handler to receive its topic message");

        foreach (var (topic, messages) in _receivedByTopic)
        {
            var msg = Assert.IsType<OrderPlaced>(Assert.Single(messages));
            Assert.Equal($"ORD-{topic}", msg.OrderId);
        }
    }

    [Then(@"the message should appear in ""(.*)""")]
    public async Task ThenTheMessageShouldAppearIn(string dlqTopic)
    {
        var dlqMessages = await _transport.ReadAllFromTopicAsync<OrderPlaced>(dlqTopic);
        Assert.NotEmpty(dlqMessages);
    }

    [Then(@"the message should appear in the application-wide DLQ")]
    public async Task ThenTheMessageShouldAppearInAppDlq()
    {
        var dlqMessages = await _transport.ReadAllFromTopicAsync<OrderPlaced>("__app.dlq");
        Assert.NotEmpty(dlqMessages);
    }

    [Then(@"the handler should not have been invoked")]
    public void ThenTheHandlerShouldNotHaveBeenInvoked()
    {
        Assert.Empty(_receivedMessages);
    }

    // ─── CLEANUP ──────────────────────────────────────────────────────

    public async ValueTask DisposeAsync()
    {
        if (_host is not null)
        {
            try { await _host.StopAsync(TimeSpan.FromSeconds(2)); } catch { }
            _host.Dispose();
        }
    }
}
