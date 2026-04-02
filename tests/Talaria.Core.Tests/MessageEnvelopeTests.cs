using Talaria.Core.Abstractions;
using Xunit;

namespace Talaria.Core.Tests;

public class MessageEnvelopeTests
{
    [Fact]
    public void Construct_ShouldAssignProperties()
    {
        var headers = new MessageHeaders();
        headers["foo"] = "bar";
        var payload = "message";

        var envelope = new MessageEnvelope<string>
        {
            Payload = payload,
            Headers = headers,
            PartitionKey = "part-1"
        };

        Assert.Equal(payload, envelope.Payload);
        Assert.Same(headers, envelope.Headers);
        Assert.Equal("part-1", envelope.PartitionKey);
    }

    [Fact]
    public void Construct_WithoutOptionalArgs_ShouldAssignDefaults()
    {
        var payload = "message";
        var envelope = new MessageEnvelope<string> { Payload = payload };

        Assert.Equal(payload, envelope.Payload);
        Assert.NotNull(envelope.Headers); // Default is new()
        Assert.Null(envelope.PartitionKey);
    }
}
