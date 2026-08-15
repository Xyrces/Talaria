using Microsoft.Extensions.Logging;
using Talaria.Core.Abstractions;
using Talaria.Core.Hosting;

namespace Talaria.Core.Tests;

public class MessageHeadersTests
{
    [Fact]
    public void HopCount_RoundTrips()
    {
        var headers = new MessageHeaders { HopCount = 5 };
        Assert.Equal(5, headers.HopCount);
    }

    [Fact]
    public void HopCount_DefaultsToZero()
    {
        var headers = new MessageHeaders();
        Assert.Equal(0, headers.HopCount);
    }

    [Fact]
    public void HopCount_Preserved_After_Copy()
    {
        var original = new MessageHeaders { HopCount = 3 };
        var copy = new MessageHeaders(original);
        Assert.Equal(3, copy.HopCount);
    }

    [Theory]
    [InlineData("abc")]
    [InlineData("2147483648")]
    public void HopCount_Malformed_ReturnsZero(string rawValue)
    {
        var headers = new MessageHeaders { [MessageHeaders.HopCountKey] = rawValue };
        Assert.Equal(0, headers.HopCount);
    }

    [Fact]
    public void IsHopCountExceeded_MalformedHopCount_LogsWarning_AndDoesNotDLQ()
    {
        var logger = new CollectingLogger();
        var pipeline = new MessageProcessingPipeline(null, new TalariaOptions { ApplicationName = "test", MaxHopCount = 5 }, logger);
        var envelope = new MessageEnvelope<string>
        {
            Payload = "x",
            Headers = new MessageHeaders { [MessageHeaders.HopCountKey] = "not-a-number" },
        };

        var exceeded = pipeline.IsHopCountExceeded(envelope, "test.topic");

        Assert.False(exceeded);
        Assert.Contains(logger.Entries, e =>
            e.LogLevel == LogLevel.Warning &&
            e.Message.Contains("malformed hop count header"));
    }

    [Fact]
    public void TraceParent_RoundTrips()
    {
        var headers = new MessageHeaders
        {
            TraceParent = "00-abc-def-01"
        };
        Assert.Equal("00-abc-def-01", headers.TraceParent);
    }

    private sealed class CollectingLogger : ILogger
    {
        public List<LogEntry> Entries { get; } = new();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            Entries.Add(new LogEntry(logLevel, formatter(state, exception)));
        }
    }

    private sealed record LogEntry(LogLevel LogLevel, string Message);
}
