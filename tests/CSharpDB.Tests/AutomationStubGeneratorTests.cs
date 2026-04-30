using CSharpDB.Primitives;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;

namespace CSharpDB.Tests;

public sealed class AutomationStubGeneratorTests
{
    [Fact]
    public void GenerateCSharp_ProducesDeterministicRegistrationStubs()
    {
        DbAutomationMetadata metadata = new(
            Commands:
            [
                new DbAutomationCommandReference("AuditOrder", "admin.forms", "form.events.BeforeInsert"),
                new DbAutomationCommandReference("auditorder", "admin.forms", "form.events.AfterUpdate"),
            ],
            ScalarFunctions:
            [
                new DbAutomationScalarFunctionReference("NormalizeName", 2, "pipelines", "transforms[0].filterExpression"),
                new DbAutomationScalarFunctionReference("normalizeName", 2, "admin.forms", "controls.name.formula"),
            ]);

        string source = AutomationStubGenerator.GenerateCSharp(
            metadata,
            new AutomationStubGenerationOptions(
                Namespace: "MyApp.CSharpDbAutomation",
                ClassName: "CSharpDbAutomationRegistration"));

        const string expected = """
            using System;
            using System.Threading.Tasks;
            using CSharpDB.Primitives;

            namespace MyApp.CSharpDbAutomation;

            public static class CSharpDbAutomationRegistration
            {
                public static void Register(DbFunctionRegistryBuilder functions, DbCommandRegistryBuilder commands)
                {
                    ArgumentNullException.ThrowIfNull(functions);
                    ArgumentNullException.ThrowIfNull(commands);

                    functions.AddScalar(
                        "NormalizeName",
                        arity: 2,
                        options: new DbScalarFunctionOptions(DbType.Text),
                        invoke: static (context, args) =>
                        {
                            // References:
                            // - admin.forms: controls.name.formula
                            // - pipelines: transforms[0].filterExpression
                            throw new NotImplementedException("Implement trusted scalar function 'NormalizeName'.");
                        });

                    commands.AddAsyncCommand(
                        "AuditOrder",
                        static async (context, ct) =>
                        {
                            // References:
                            // - admin.forms: form.events.AfterUpdate
                            // - admin.forms: form.events.BeforeInsert
                            await Task.CompletedTask;
                            throw new NotImplementedException("Implement trusted command 'AuditOrder'.");
                        });
                }
            }
            """;

        Assert.Equal(Normalize(expected), Normalize(source));
    }

    [Fact]
    public void GenerateCSharp_ProducesCompilableSource()
    {
        DbAutomationMetadata metadata = new(
            Commands:
            [
                new DbAutomationCommandReference("WriteAudit", "admin.forms", "form.events.BeforeInsert\r\n// ignored"),
            ],
            ScalarFunctions:
            [
                new DbAutomationScalarFunctionReference("FormatValue", 1, "admin.reports", "bands.detail.controls.total.expression"),
            ]);

        string source = AutomationStubGenerator.GenerateCSharp(
            metadata,
            new AutomationStubGenerationOptions(
                Namespace: "MyApp.Generated",
                ClassName: "RegistrationStubs",
                MethodName: "AddCallbacks"));

        AssertCompiles(source);
    }

    [Fact]
    public void GenerateCSharp_RejectsInvalidTypeNames()
    {
        DbAutomationMetadata metadata = new();

        Assert.Throws<ArgumentException>(() =>
            AutomationStubGenerator.GenerateCSharp(
                metadata,
                new AutomationStubGenerationOptions(
                    Namespace: "MyApp.Generated",
                    ClassName: "class")));
    }

    private static void AssertCompiles(string source)
    {
        SyntaxTree syntaxTree = CSharpSyntaxTree.ParseText(source);
        CSharpCompilation compilation = CSharpCompilation.Create(
            assemblyName: "AutomationStubGeneratorTests_" + Guid.NewGuid().ToString("N"),
            syntaxTrees: [syntaxTree],
            references: GetMetadataReferences(),
            options: new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));

        Diagnostic[] errors = compilation.GetDiagnostics()
            .Where(static diagnostic => diagnostic.Severity == DiagnosticSeverity.Error)
            .ToArray();

        Assert.Empty(errors);
    }

    private static MetadataReference[] GetMetadataReferences()
    {
        string trustedPlatformAssemblies = (string?)AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES")
            ?? throw new InvalidOperationException("Trusted platform assemblies were not available.");

        return trustedPlatformAssemblies
            .Split(Path.PathSeparator)
            .Select(static path => MetadataReference.CreateFromFile(path))
            .Append(MetadataReference.CreateFromFile(typeof(DbAutomationMetadata).Assembly.Location))
            .ToArray();
    }

    private static string Normalize(string source)
        => source.ReplaceLineEndings("\n").TrimEnd('\n');
}
