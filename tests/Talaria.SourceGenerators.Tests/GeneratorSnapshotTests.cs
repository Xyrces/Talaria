using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using VerifyXunit;

namespace Talaria.SourceGenerators.Tests;

public class GeneratorSnapshotTests
{
    [Fact]
    public Task GeneratesHandlerRegistration_ForValidHandler()
    {
        var source = @"
using System.Threading.Tasks;
using Talaria.Core.Attributes;
using Talaria.Core.Abstractions;

namespace TestNamespace;

public class MyMessage { }

public class MyHandler
{
    [TalariaHandler(""orders.placed"", ConsumerGroup = ""custom-group"")]
    public Task HandleOrder(MyMessage message)
    {
        return Task.CompletedTask;
    }

    [TalariaHandler(""inventory.updated"")]
    public void HandleSyc(MessageEnvelope<MyMessage> envelope, System.Threading.CancellationToken ct)
    {
    }
}
";

        return VerifyGenerator(source);
    }

    private static Task VerifyGenerator(string source)
    {
        // Force load Talaria.Core so it's included in GetAssemblies()
        _ = typeof(Talaria.Core.Attributes.TalariaHandlerAttribute);
        _ = typeof(Talaria.Core.Abstractions.MessageEnvelope<>);

        var references = new[]
        {
            MetadataReference.CreateFromFile(typeof(object).Assembly.Location),
            MetadataReference.CreateFromFile(typeof(Task).Assembly.Location),
            MetadataReference.CreateFromFile(typeof(System.Threading.CancellationToken).Assembly.Location),
            MetadataReference.CreateFromFile(typeof(Talaria.Core.Attributes.TalariaHandlerAttribute).Assembly.Location),
            
            // Add netstandard and system.runtime to make sure basic types resolve
            MetadataReference.CreateFromFile(AppDomain.CurrentDomain.GetAssemblies().Single(a => a.GetName().Name == "netstandard").Location),
            MetadataReference.CreateFromFile(AppDomain.CurrentDomain.GetAssemblies().Single(a => a.GetName().Name == "System.Runtime").Location)
        };

        var compilation = CSharpCompilation.Create("Talaria.SourceGenerators.Tests.Compilation",
            new[] { CSharpSyntaxTree.ParseText(source) },
            references,
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));

        var diagnostics = compilation.GetDiagnostics();
        foreach (var d in diagnostics)
        {
            if (d.Severity == DiagnosticSeverity.Error)
            {
                throw new InvalidOperationException($"Compilation error before generation: {d}");
            }
        }

        var generator = new TalariaIncrementalGenerator();
        GeneratorDriver driver = CSharpGeneratorDriver.Create(generator);

        driver = driver.RunGenerators(compilation);
        var runResult = driver.GetRunResult();

        if (runResult.Diagnostics.Length > 0)
            throw new InvalidOperationException("Generator diagnostics: " + string.Join(", ", runResult.Diagnostics));

        if (runResult.GeneratedTrees.Length == 0)
            throw new InvalidOperationException("Generator produced NO output trees!");

        return Verifier.Verify(driver).UseDirectory("Snapshots");
    }
}
