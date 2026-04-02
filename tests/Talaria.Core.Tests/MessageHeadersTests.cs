using Talaria.Core.Abstractions;

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

    [Fact]
    public void TraceParent_RoundTrips()
    {
        var headers = new MessageHeaders
        {
            TraceParent = "00-abc-def-01"
        };
        Assert.Equal("00-abc-def-01", headers.TraceParent);
    }
}
