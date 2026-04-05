using System.Reflection;
using CSharpDB.Engine;
using CSharpDB.Generators;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;

namespace CSharpDB.Tests;

public sealed class CollectionModelGeneratorTests
{
    [Fact]
    public void CollectionModelGenerator_ReportsUnsupportedMembers()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        CSharpParseOptions parseOptions = CSharpParseOptions.Default;
        Compilation compilation = CreateCompilation(
            """
            using System.Collections.Generic;
            using System.Text.Json.Serialization;
            using CSharpDB.Engine;

            namespace Demo;

            [CollectionModel(typeof(ProductJsonContext))]
            public sealed partial record Product(string Email, decimal Price, List<List<string>> TagMatrix);

            [JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
            [JsonSerializable(typeof(Product))]
            internal sealed partial class ProductJsonContext : JsonSerializerContext;
            """,
            parseOptions);

        GeneratorDriver driver = CSharpGeneratorDriver.Create(new ISourceGenerator[]
        {
            new CollectionModelGenerator().AsSourceGenerator(),
        }, parseOptions: parseOptions);

        driver = driver.RunGeneratorsAndUpdateCompilation(compilation, out _, out _, ct);
        GeneratorDriverRunResult runResult = driver.GetRunResult();

        Diagnostic[] warnings = runResult.Diagnostics
            .Where(static diagnostic => diagnostic.Id == "CDBGEN007")
            .OrderBy(static diagnostic => diagnostic.GetMessage(), StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(2, warnings.Length);
        Assert.Contains(warnings, diagnostic => diagnostic.GetMessage().Contains("member 'Price'", StringComparison.Ordinal));
        Assert.Contains(warnings, diagnostic => diagnostic.GetMessage().Contains("member 'TagMatrix'", StringComparison.Ordinal));

        string generatedSource = GetGeneratedSourceContaining(runResult, "partial record Product");
        Assert.Contains(" Email ", generatedSource, StringComparison.Ordinal);
        Assert.DoesNotContain(" Price ", generatedSource, StringComparison.Ordinal);
        Assert.DoesNotContain(" TagMatrix ", generatedSource, StringComparison.Ordinal);
    }

    [Fact]
    public void CollectionModelGenerator_ReportsRecursiveMembers()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        CSharpParseOptions parseOptions = CSharpParseOptions.Default;
        Compilation compilation = CreateCompilation(
            """
            using System.Text.Json.Serialization;
            using CSharpDB.Engine;

            namespace Demo;

            [CollectionModel(typeof(NodeJsonContext))]
            public sealed partial record Node(string Name, Node? Next);

            [JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
            [JsonSerializable(typeof(Node))]
            internal sealed partial class NodeJsonContext : JsonSerializerContext;
            """,
            parseOptions);

        GeneratorDriver driver = CSharpGeneratorDriver.Create(new ISourceGenerator[]
        {
            new CollectionModelGenerator().AsSourceGenerator(),
        }, parseOptions: parseOptions);

        driver = driver.RunGeneratorsAndUpdateCompilation(compilation, out _, out _, ct);
        GeneratorDriverRunResult runResult = driver.GetRunResult();

        Diagnostic warning = Assert.Single(runResult.Diagnostics.Where(static diagnostic => diagnostic.Id == "CDBGEN007"));
        Assert.Contains("member 'Next'", warning.GetMessage(), StringComparison.Ordinal);
        Assert.Contains("recursive object graphs are not supported", warning.GetMessage(), StringComparison.Ordinal);

        string generatedSource = GetGeneratedSourceContaining(runResult, "partial record Node");
        Assert.Contains(" Name ", generatedSource, StringComparison.Ordinal);
        Assert.DoesNotContain(" Next ", generatedSource, StringComparison.Ordinal);
    }

    private static Compilation CreateCompilation(string source, CSharpParseOptions parseOptions)
    {
        SyntaxTree syntaxTree = CSharpSyntaxTree.ParseText(source, parseOptions);

        MetadataReference[] references = GetMetadataReferences().ToArray();
        return CSharpCompilation.Create(
            assemblyName: "CollectionModelGeneratorTests_" + Guid.NewGuid().ToString("N"),
            syntaxTrees: [syntaxTree],
            references: references,
            options: new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));
    }

    private static IEnumerable<MetadataReference> GetMetadataReferences()
    {
        string trustedPlatformAssemblies = (string?)AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES")
            ?? throw new InvalidOperationException("Trusted platform assemblies were not available.");

        foreach (string path in trustedPlatformAssemblies.Split(Path.PathSeparator))
            yield return MetadataReference.CreateFromFile(path);

        yield return MetadataReference.CreateFromFile(typeof(CollectionModelAttribute).Assembly.Location);
        yield return MetadataReference.CreateFromFile(typeof(CollectionModelGenerator).Assembly.Location);
    }

    private static string GetGeneratedSourceContaining(GeneratorDriverRunResult runResult, string marker)
    {
        foreach (GeneratorRunResult result in runResult.Results)
        {
            foreach (GeneratedSourceResult generatedSource in result.GeneratedSources)
            {
                string source = generatedSource.SourceText.ToString();
                if (source.Contains(marker, StringComparison.Ordinal))
                    return source;
            }
        }

        throw new InvalidOperationException($"Generated source containing '{marker}' was not found.");
    }
}
