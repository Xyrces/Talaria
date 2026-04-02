using System.Collections.Concurrent;
using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Diagnostics;
using System.Linq;

namespace Talaria.SourceGenerators
{
    [DiagnosticAnalyzer(LanguageNames.CSharp)]
    public class TopologyAnalyzer : DiagnosticAnalyzer
    {
        public const string DiagnosticId = "TALA001";
        private const string Category = "Design";

        private static readonly DiagnosticDescriptor Rule = new(
            DiagnosticId,
            "Static DAG cycle detected in Talaria handlers",
            "Handler '{0}' creates an infinite message loop by consuming and producing '{1}' within the topology",
            Category,
            DiagnosticSeverity.Warning,
            isEnabledByDefault: true,
            description: "A cycle was detected in the message topology, which could lead to an infinite loop.",
            customTags: new[] { WellKnownDiagnosticTags.CompilationEnd });

        public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics => ImmutableArray.Create(Rule);

        public override void Initialize(AnalysisContext context)
        {
            context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
            context.EnableConcurrentExecution();

            // We register a CompilationAction because we need to see the entire messaging topology
            // to detect multi-hop cycles across different handlers in the assembly.
            context.RegisterCompilationAction(AnalyzeTopology);
        }

        private static void AnalyzeTopology(CompilationAnalysisContext context)
        {
            // Find TalariaHandlerAttribute
            var handlerAttr = context.Compilation.GetTypeByMetadataName("Talaria.Core.Attributes.TalariaHandlerAttribute");
            if (handlerAttr == null) return;

            // Find IProducer<T>
            var producerInterface = context.Compilation.GetTypeByMetadataName("Talaria.Core.Abstractions.IProducer`1");
            if (producerInterface == null) return;

            // Find MessageEnvelope<T>
            var envelopeType = context.Compilation.GetTypeByMetadataName("Talaria.Core.Abstractions.MessageEnvelope`1");
            
            var handlers = new System.Collections.Generic.List<HandlerNode>();

            // Inspect all named types in the assembly
            foreach (var type in GetAllClasses(context.Compilation.Assembly.GlobalNamespace))
            {
                var consumed = new System.Collections.Generic.HashSet<ITypeSymbol>(SymbolEqualityComparer.Default);
                var produced = new System.Collections.Generic.HashSet<ITypeSymbol>(SymbolEqualityComparer.Default);

                bool isHandler = false;

                // 1. Find consumed messages (methods with [TalariaHandler])
                foreach (var method in type.GetMembers().OfType<IMethodSymbol>())
                {
                    if (method.GetAttributes().Any(a => SymbolEqualityComparer.Default.Equals(a.AttributeClass, handlerAttr)))
                    {
                        isHandler = true;
                        if (method.Parameters.Length > 0)
                        {
                            var paramType = method.Parameters[0].Type;
                            
                            if (paramType is INamedTypeSymbol named && named.IsGenericType && 
                                SymbolEqualityComparer.Default.Equals(named.ConstructedFrom, envelopeType))
                            {
                                consumed.Add(named.TypeArguments[0]);
                            }
                            else
                            {
                                consumed.Add(paramType);
                            }
                        }
                    }
                }

                if (!isHandler) continue;

                // 2. Find produced messages (constructor injection of IProducer<T> or IEnumerable<IProducer<T>>)
                foreach (var constructor in type.Constructors)
                {
                    foreach (var param in constructor.Parameters)
                    {
                        if (param.Type is INamedTypeSymbol namedParam && namedParam.IsGenericType)
                        {
                            if (SymbolEqualityComparer.Default.Equals(namedParam.ConstructedFrom, producerInterface))
                            {
                                produced.Add(namedParam.TypeArguments[0]);
                            }
                            else if (namedParam.ConstructedFrom.ToDisplayString() == "System.Collections.Generic.IEnumerable<T>" ||
                                     namedParam.ConstructedFrom.ToDisplayString() == "System.Collections.Generic.IEnumerable`1")
                            {
                                var innerArg = namedParam.TypeArguments[0] as INamedTypeSymbol;
                                if (innerArg != null && innerArg.IsGenericType && SymbolEqualityComparer.Default.Equals(innerArg.ConstructedFrom, producerInterface))
                                {
                                    produced.Add(innerArg.TypeArguments[0]);
                                }
                            }
                        }
                    }
                }

                handlers.Add(new HandlerNode(type, consumed, produced));
            }

            // 3. Cycle Detection over the gathered topology
            foreach (var handler in handlers)
            {
                // Simple 1-hop check (consumes X and produces X directly)
                foreach (var c in handler.Consumed)
                {
                    if (handler.Produced.Contains(c))
                    {
                        var location = handler.Symbol.Locations.FirstOrDefault();
                        if (location != null)
                        {
                            var diagnostic = Diagnostic.Create(Rule, location, handler.Symbol.Name, c.Name);
                            context.ReportDiagnostic(diagnostic);
                        }
                    }
                }
            }
        }

        private static System.Collections.Generic.IEnumerable<INamedTypeSymbol> GetAllClasses(INamespaceSymbol root)
        {
            foreach (var member in root.GetMembers())
            {
                if (member is INamespaceSymbol ns)
                {
                    foreach (var c in GetAllClasses(ns)) yield return c;
                }
                else if (member is INamedTypeSymbol type && type.TypeKind == TypeKind.Class)
                {
                    yield return type;
                }
            }
        }

        private class HandlerNode
        {
            public INamedTypeSymbol Symbol { get; }
            public System.Collections.Generic.HashSet<ITypeSymbol> Consumed { get; }
            public System.Collections.Generic.HashSet<ITypeSymbol> Produced { get; }

            public HandlerNode(INamedTypeSymbol symbol, System.Collections.Generic.HashSet<ITypeSymbol> consumed, System.Collections.Generic.HashSet<ITypeSymbol> produced)
            {
                Symbol = symbol;
                Consumed = consumed;
                Produced = produced;
            }
        }
    }
}
