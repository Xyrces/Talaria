using Talaria.Core.Abstractions;

namespace Talaria.Specs;

public class MessageHeadersTests
{
    [Fact]
    public void SettingNull_RemovesHeader()
    {
        var headers = new MessageHeaders
        {
            TraceParent = "abc",
            TraceState = "def",
            DlqReason = "err",
            DlqException = "ex"
        };

        headers.TraceParent = null;
        headers.TraceState = null;
        headers.DlqReason = null;
        headers.DlqException = null;

        Assert.Null(headers.TraceParent);
        Assert.Null(headers.TraceState);
        Assert.Null(headers.DlqReason);
        Assert.Null(headers.DlqException);
        
        Assert.Empty(headers);
    }
    
    [Fact]
    public void DefaultHopCount_IsZero()
    {
         var headers = new MessageHeaders();
         Assert.Equal(0, headers.HopCount);
         
         // Unparsable
         headers[MessageHeaders.HopCountKey] = "abc";
         Assert.Equal(0, headers.HopCount);
    }
    
    [Fact]
    public void DefaultSchemaVersion_IsOne()
    {
         var headers = new MessageHeaders();
         Assert.Equal(1, headers.SchemaVersion);
         
         // Unparsable
         headers[MessageHeaders.SchemaVersionKey] = "abc";
         Assert.Equal(1, headers.SchemaVersion);
    }
}
