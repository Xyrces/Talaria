// SPDX-License-Identifier: Apache-2.0

using Talaria.Core.Abstractions;

namespace Talaria.Core.Tests;

public class RequestResponseHeadersTests
{
    [Fact]
    public void RequestId_RoundTrips()
    {
        var headers = new MessageHeaders { RequestId = "abc-123" };
        Assert.Equal("abc-123", headers.RequestId);
    }

    [Fact]
    public void RequestId_SetNull_RemovesKey()
    {
        var headers = new MessageHeaders { RequestId = "abc-123" };
        headers.RequestId = null;
        Assert.Null(headers.RequestId);
        Assert.False(headers.ContainsKey(MessageHeaders.RequestIdKey));
    }

    [Fact]
    public void ReplyTo_RoundTrips()
    {
        var headers = new MessageHeaders { ReplyTo = "replies.topic" };
        Assert.Equal("replies.topic", headers.ReplyTo);
    }

    [Fact]
    public void RequestFault_True_RoundTrips()
    {
        var headers = new MessageHeaders { RequestFault = true };
        Assert.True(headers.RequestFault);
    }

    [Theory]
    [InlineData("not-a-bool")]
    [InlineData("false")]
    [InlineData(null)]
    public void RequestFault_MalformedOrFalse_ReturnsFalse(string? rawValue)
    {
        var headers = new MessageHeaders();
        if (rawValue is not null)
        {
            headers[MessageHeaders.RequestFaultKey] = rawValue;
        }

        Assert.False(headers.RequestFault);
    }
}
