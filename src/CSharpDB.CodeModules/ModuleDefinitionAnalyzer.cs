using CSharpDB.DevOps;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace CSharpDB.CodeModules;

/// <summary>Creates a syntax/semantic model only. No assembly emission, loading, trust changes, or module invocation.</summary>
public sealed class ModuleDefinitionAnalyzer : IDefinitionModuleAnalyzer
{
    private static readonly Lazy<MetadataReference[]> References = new(() =>
    {
        var paths = ((string?)AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES") ?? "").Split(Path.PathSeparator, StringSplitOptions.RemoveEmptyEntries)
            .Concat(new[] { typeof(ModuleDefinitionAnalyzer).Assembly.Location, typeof(Client.ICSharpDbClient).Assembly.Location, typeof(Primitives.DbValue).Assembly.Location });
        return paths.Distinct(StringComparer.OrdinalIgnoreCase).Where(File.Exists).Select(p => MetadataReference.CreateFromFile(p)).ToArray();
    });

    public ModuleDefinitionInspection Inspect(string source, CancellationToken ct = default)
    {
        var tree = CSharpSyntaxTree.ParseText(source, cancellationToken: ct);
        var root = tree.GetRoot(ct);
        var found = new List<ModuleDefinitionReference>();
        var diagnostics = tree.GetDiagnostics(ct).Where(d => d.Severity == DiagnosticSeverity.Error).Select(d => d.ToString()).ToList();
        var compilation = CSharpCompilation.Create("DefinitionInspection", [tree], References.Value,
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));
        var model = compilation.GetSemanticModel(tree, ignoreAccessibility: true);
        foreach (var invocation in root.DescendantNodes().OfType<InvocationExpressionSyntax>())
        {
            ct.ThrowIfCancellationRequested();
            string name = invocation.Expression switch
            { MemberAccessExpressionSyntax member => member.Name.Identifier.ValueText, IdentifierNameSyntax identifier => identifier.Identifier.ValueText, _ => "" };
            string? kind = name switch
            {
                "ExecuteSqlAsync" or "RunSqlAsync" or "ExecuteReadAsync" => "sql",
                "ExecuteProcedureAsync" or "RunProcedureAsync" => "procedure",
                "GetSavedQueryAsync" => "savedQuery",
                "SetFieldAsync" => "field",
                "OpenFormAsync" => "form",
                _ => null,
            };
            if (kind is null)
            {
                var called = model.GetSymbolInfo(invocation, ct).Symbol as IMethodSymbol;
                if (name is "RunHostCommandAsync" or "RunActionSequenceAsync" or "ExecuteCommandAsync"
                    || called?.MethodKind == MethodKind.DelegateInvoke || called?.ContainingNamespace.ToDisplayString().StartsWith("System.Reflection", StringComparison.Ordinal) == true)
                    diagnostics.Add($"Line {invocation.GetLocation().GetLineSpan().StartLinePosition.Line + 1}: host, reflection, or indirect call may have additional runtime dependencies.");
                continue;
            }
            var argument = invocation.ArgumentList.Arguments.FirstOrDefault()?.Expression;
            var constant = argument is null ? default : model.GetConstantValue(argument, ct);
            int line = invocation.GetLocation().GetLineSpan().StartLinePosition.Line + 1;
            var symbol = model.GetSymbolInfo(invocation, ct).Symbol as IMethodSymbol;
            string? owner = symbol?.ContainingType.ToDisplayString();
            bool confirmed = owner is "CSharpDB.Client.ICSharpDbClient" or "CSharpDB.Client.CSharpDbClient" or "CSharpDB.CodeModules.Runtime.IFormCommandApi";
            if (constant.HasValue && constant.Value is string text)
            {
                found.Add(new(kind, text, line, confirmed));
                if (!confirmed) diagnostics.Add($"Line {line}: call receiver could not be bound to a supported CSharpDB API; references are possible.");
            }
            else diagnostics.Add($"Line {line}: {name} uses a runtime-computed argument; dependencies require review.");
        }
        foreach (var member in root.DescendantNodes().OfType<MemberAccessExpressionSyntax>())
        {
            ct.ThrowIfCancellationRequested();
            if (member.Expression is not IdentifierNameSyntax { Identifier.ValueText: "Me" }) continue;
            bool confirmed = model.GetSymbolInfo(member.Expression, ct).Symbol is IPropertySymbol property
                && property.ContainingType.ToDisplayString() == "CSharpDB.CodeModules.Runtime.FormCodeModule";
            found.Add(new("field", member.Name.Identifier.ValueText, member.GetLocation().GetLineSpan().StartLinePosition.Line + 1, confirmed));
        }
        foreach (var element in root.DescendantNodes().OfType<ElementAccessExpressionSyntax>())
        {
            if (element.Expression is not IdentifierNameSyntax { Identifier.ValueText: "Me" }) continue;
            var arg = element.ArgumentList.Arguments.FirstOrDefault()?.Expression;
            var constant = arg is null ? default : model.GetConstantValue(arg, ct);
            if (constant.HasValue && constant.Value is string text) found.Add(new("field", text, element.GetLocation().GetLineSpan().StartLinePosition.Line + 1));
            else diagnostics.Add("A form field name is computed at runtime; its dependency requires review.");
        }
        // Arbitrary module code can call external helpers; this is deliberately not a whole-program proof.
        if (root.DescendantNodes().OfType<InvocationExpressionSyntax>().Any(i => model.GetSymbolInfo(i, ct).Symbol is null))
            diagnostics.Add("Some C# calls cannot be resolved from this stored module alone; external helper dependencies may be missing.");
        return new(found.Distinct().ToArray(), diagnostics.Distinct().ToArray());
    }
}
