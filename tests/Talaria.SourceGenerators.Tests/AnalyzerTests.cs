using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Diagnostics;

namespace Talaria.SourceGenerators.Tests;

public class AnalyzerTests
{
    [Fact]
    public async Task TopologyAnalyzer_ReportsWarning_WhenDirectCycleDetected()
    {
        var source = @"
using System.Threading.Tasks;
using Talaria.Core.Attributes;
using Talaria.Core.Abstractions;

namespace TestNamespace;

public class OrderMessage { }

public class OrderHandler
{
    private readonly IProducer<OrderMessage> _producer;

    // Injects producer for OrderMessage
    public OrderHandler(IProducer<OrderMessage> producer)
    {
        _producer = producer;
    }

    // Handles OrderMessage
    [TalariaHandler(""orders.placed"")]
    public Task HandleOrder(OrderMessage message)
    {
        return _producer.ProduceAsync(message);
    }
}
";

        var diagnostics = await RunAnalyzerAsync(source);

        Assert.Single(diagnostics);
        var diagnostic = diagnostics[0];
        Assert.Equal("TALA001", diagnostic.Id);
        Assert.Equal(DiagnosticSeverity.Warning, diagnostic.Severity);
        Assert.Contains("OrderHandler", diagnostic.GetMessage());
        Assert.Contains("OrderMessage", diagnostic.GetMessage());
    }

    [Fact]
    public async Task TopologyAnalyzer_NoWarning_WhenNoCycle()
    {
        var source = @"
using System.Threading.Tasks;
using Talaria.Core.Attributes;
using Talaria.Core.Abstractions;

namespace TestNamespace;

public class OrderMessage { }
public class OtherMessage { }

public class OrderHandler
{
    private readonly IProducer<OtherMessage> _producer;

    // Produces OtherMessage
    public OrderHandler(IProducer<OtherMessage> producer)
    {
        _producer = producer;
    }

    // Handles OrderMessage
    [TalariaHandler(""orders.placed"")]
    public Task HandleOrder(OrderMessage message)
    {
        return Task.CompletedTask;
    }
}
";

        var diagnostics = await RunAnalyzerAsync(source);

        Assert.Empty(diagnostics);
    }

    [Fact]
    public async Task TopologyAnalyzer_ReportsWarning_WhenMultiHopCycleDetected()
    {
        var source = @"
using System.Threading.Tasks;
using Talaria.Core.Attributes;
using Talaria.Core.Abstractions;

namespace TestNamespace;

public class Msg1 { }
public class Msg2 { }

public class HandlerA
{
    private readonly IProducer<Msg2> _producer;
    public HandlerA(IProducer<Msg2> producer) { _producer = producer; }

    [TalariaHandler(""topic.one"")]
    public Task Handle(Msg1 msg) => _producer.ProduceAsync(new Msg2());
}

public class HandlerB
{
    private readonly IProducer<Msg1> _producer;
    public HandlerB(IProducer<Msg1> producer) { _producer = producer; }

    [TalariaHandler(""topic.two"")]
    public Task Handle(Msg2 msg) => _producer.ProduceAsync(new Msg1());
}
";

        var diagnostics = await RunAnalyzerAsync(source);

        Assert.Equal(2, diagnostics.Length);
        Assert.All(diagnostics, d => Assert.Equal("TALA001", d.Id));
    }

    private static async Task<ImmutableArray<Diagnostic>> RunAnalyzerAsync(string source)
    {
        // Force load Talaria.Core
        _ = typeof(Talaria.Core.Attributes.TalariaHandlerAttribute);
        _ = typeof(Talaria.Core.Abstractions.MessageEnvelope<>);

        var references = new[]
        {
            MetadataReference.CreateFromFile(typeof(object).Assembly.Location),
            MetadataReference.CreateFromFile(typeof(Task).Assembly.Location),
            MetadataReference.CreateFromFile(typeof(System.Threading.CancellationToken).Assembly.Location),
            MetadataReference.CreateFromFile(typeof(Talaria.Core.Attributes.TalariaHandlerAttribute).Assembly.Location),
            MetadataReference.CreateFromFile(AppDomain.CurrentDomain.GetAssemblies().Single(a => a.GetName().Name == "netstandard").Location),
            MetadataReference.CreateFromFile(AppDomain.CurrentDomain.GetAssemblies().Single(a => a.GetName().Name == "System.Runtime").Location),
            MetadataReference.CreateFromFile(AppDomain.CurrentDomain.GetAssemblies().Single(a => a.GetName().Name == "System.Collections").Location)
        };

        var compilation = CSharpCompilation.Create("TopologyAnalysisTest",
            new[] { CSharpSyntaxTree.ParseText(source) },
            references,
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));

        var compilerDiagnostics = compilation.GetDiagnostics();
        foreach (var d in compilerDiagnostics)
        {
            if (d.Severity == DiagnosticSeverity.Error)
            {
                throw new InvalidOperationException($"Compilation error setup: {d}");
            }
        }

        var analyzer = new TopologyAnalyzer();
        var compilationWithAnalyzers = compilation.WithAnalyzers(ImmutableArray.Create<DiagnosticAnalyzer>(analyzer));

        return await compilationWithAnalyzers.GetAnalyzerDiagnosticsAsync();
    }
}
