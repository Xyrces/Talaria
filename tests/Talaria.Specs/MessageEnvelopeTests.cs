using Talaria.Core.Abstractions;

namespace Talaria.Specs;

public class MessageEnvelopeTests
{
    [Fact]
    public void Properties_CanBeSetAndRetrieved()
    {
        var headers = new MessageHeaders();
        var ts = DateTimeOffset.UtcNow;

        var envelope = new MessageEnvelope<string>
        {
            Payload = "test",
            Headers = headers,
            CorrelationId = "corr",
            SourceTopic = "src",
            PartitionKey = "pk",
            Timestamp = ts,
            Offset = 123
        };

        Assert.Equal("test", envelope.Payload);
        Assert.Same(headers, envelope.Headers);
        Assert.Equal("corr", envelope.CorrelationId);
        Assert.Equal("src", envelope.SourceTopic);
        Assert.Equal("pk", envelope.PartitionKey);
        Assert.Equal(ts, envelope.Timestamp);
        Assert.Equal(123, envelope.Offset);
    }
}
