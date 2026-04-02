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
