using Talaria.Core.Attributes;

namespace Talaria.Specs;

public class AttributeTests
{
    [Fact]
    public void MessageVersionAttribute_HasProperties()
    {
        var attr = new MessageVersionAttribute(2);
        Assert.Equal(2, attr.Version);
    }
    
    [Fact]
    public void TalariaHandlerAttribute_HasProperties()
    {
        var attr = new TalariaHandlerAttribute("topic.test");
        Assert.Equal("topic.test", attr.Topic);
    }
}
