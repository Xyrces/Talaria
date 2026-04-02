using System;
using Talaria.Core.Abstractions;
using Talaria.Core.Attributes;
using Talaria.Core.Sagas;
using Xunit;

namespace Talaria.Specs.Tests;

public class CorrelationResolverTests
{
    private class NoIdMessage { }

    private class HeaderMessage { }

    private class AttributeMessage 
    { 
        [SagaCorrelation] 
        public string SpecialKey { get; set; } = "attr-123"; 
    }

    private class CorrelationIdMessage 
    { 
        public string CorrelationId { get; set; } = "corr-123"; 
    }

    private class IdMessage 
    { 
        public string Id { get; set; } = "id-123"; 
    }

    private class SuffixIdMessage 
    { 
        public string PaymentId { get; set; } = "pay-123"; 
    }

    [Fact]
    public void Resolves_From_Header_First()
    {
        var headers = new MessageHeaders { { MessageHeaders.CorrelationIdKey, "head-123" } };
        var msg = new CorrelationIdMessage { CorrelationId = "corr-123" }; // Header should override property
        
        var resolved = CorrelationResolver.Resolve(msg, headers);
        Assert.Equal("head-123", resolved);
    }

    [Fact]
    public void Resolves_From_SagaCorrelationAttribute()
    {
        var msg = new AttributeMessage();
        var resolved = CorrelationResolver.Resolve(msg, new MessageHeaders());
        Assert.Equal("attr-123", resolved);
    }

    [Fact]
    public void Resolves_From_CorrelationId_Property()
    {
        var msg = new CorrelationIdMessage();
        var resolved = CorrelationResolver.Resolve(msg, new MessageHeaders());
        Assert.Equal("corr-123", resolved);
    }

    [Fact]
    public void Resolves_From_Id_Property()
    {
        var msg = new IdMessage();
        var resolved = CorrelationResolver.Resolve(msg, new MessageHeaders());
        Assert.Equal("id-123", resolved);
    }

    [Fact]
    public void Resolves_From_SuffixId_Property()
    {
        var msg = new SuffixIdMessage();
        var resolved = CorrelationResolver.Resolve(msg, new MessageHeaders());
        Assert.Equal("pay-123", resolved);
    }

    [Fact]
    public void Returns_Null_When_Not_Found()
    {
        var msg = new NoIdMessage();
        var resolved = CorrelationResolver.Resolve(msg, new MessageHeaders());
        Assert.Null(resolved);
    }

    [Fact]
    public void Resolves_Returns_Null_When_Matching_Property_Is_Null()
    {
        // 1. [SagaCorrelation] is null
        var attrMsg = new AttributeMessage { SpecialKey = null! };
        Assert.Null(CorrelationResolver.Resolve(attrMsg, new MessageHeaders()));
        
        // 2. CorrelationId is null
        var corrMsg = new CorrelationIdMessage { CorrelationId = null! };
        Assert.Null(CorrelationResolver.Resolve(corrMsg, new MessageHeaders()));
        
        // 3. Id is null
        var idMsg = new IdMessage { Id = null! };
        Assert.Null(CorrelationResolver.Resolve(idMsg, new MessageHeaders()));
        
        // 4. SuffixId is null
        var sfxMsg = new SuffixIdMessage { PaymentId = null! };
        Assert.Null(CorrelationResolver.Resolve(sfxMsg, new MessageHeaders()));
    }
}
