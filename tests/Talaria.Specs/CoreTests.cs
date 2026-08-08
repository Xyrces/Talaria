using System.Collections.Generic;
using Talaria.Core;
using Talaria.Core.Abstractions;
using Xunit;

namespace Talaria.Specs.Tests;

public class CoreTests
{
    [Fact]
    public void MessageHeaders_NullCopy_IsEmpty()
    {
        var headers = new MessageHeaders(null);
        Assert.Empty(headers);
    }

    [Fact]
    public void MessageHeaders_TryGet_Missing_ReturnsFalse()
    {
        var headers = new MessageHeaders();
        Assert.False(headers.TryGetValue("missing", out var val));
        Assert.Null(val);
    }
}
