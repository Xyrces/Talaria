using Talaria.Core.Attributes;
using Xunit;

namespace Talaria.Core.Tests;

public class AttributeTests
{
    [Fact]
    public void MessageVersionAttribute_ShouldAssignVersion()
    {
        var attr = new MessageVersionAttribute(2);
        Assert.Equal(2, attr.Version);
    }

    [Fact]
    public void TalariaHandlerAttribute_ShouldAssignTopic()
    {
        var attr = new TalariaHandlerAttribute("test-topic")
        {
            ConsumerGroup = "group",
        };
        
        Assert.Equal("test-topic", attr.Topic);
        Assert.Equal("group", attr.ConsumerGroup);
    }
}
