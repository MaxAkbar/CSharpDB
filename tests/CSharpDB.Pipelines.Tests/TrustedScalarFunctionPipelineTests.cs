using CSharpDB.Pipelines.Models;
using CSharpDB.Pipelines.Runtime.BuiltIns;
using CSharpDB.Primitives;

namespace CSharpDB.Pipelines.Tests;

public sealed class TrustedScalarFunctionPipelineTests
{
    [Fact]
    public async Task FilterAndDeriveTransforms_InvokeRegisteredFunctions()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        var registry = DbFunctionRegistry.Create(functions =>
        {
            functions.AddScalar(
                "Slugify",
                1,
                new DbScalarFunctionOptions(DbType.Text, IsDeterministic: true, NullPropagating: true),
                static (_, args) => DbValue.FromText(args[0].AsText.ToLowerInvariant().Replace(' ', '-')));
            functions.AddScalar(
                "StartsWithA",
                1,
                new DbScalarFunctionOptions(DbType.Integer, IsDeterministic: true, NullPropagating: true),
                static (_, args) => DbValue.FromInteger(args[0].AsText.StartsWith("A", StringComparison.OrdinalIgnoreCase) ? 1 : 0));
        });

        var filter = new FilterPipelineTransform(new PipelineTransformDefinition
        {
            Kind = PipelineTransformKind.Filter,
            FilterExpression = "StartsWithA(name) == 1",
        }, registry);
        var derive = new DerivePipelineTransform(new PipelineTransformDefinition
        {
            Kind = PipelineTransformKind.Derive,
            DerivedColumns =
            [
                new PipelineDerivedColumn { Name = "slug", Expression = "Slugify(name)" },
            ],
        }, registry);
        var context = CreateContext();
        var batch = new PipelineRowBatch
        {
            BatchNumber = 1,
            StartingRowNumber = 1,
            Rows =
            [
                new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
                {
                    ["name"] = "Alice Smith",
                },
                new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
                {
                    ["name"] = "Bob Smith",
                },
            ],
        };

        PipelineRowBatch filtered = await filter.TransformAsync(batch, context, ct);
        PipelineRowBatch derived = await derive.TransformAsync(filtered, context, ct);

        Assert.Single(derived.Rows);
        Assert.Equal("alice-smith", derived.Rows[0]["slug"]);
    }

    [Fact]
    public async Task MissingRegisteredFunction_FailsAtRuntime()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        var transform = new DerivePipelineTransform(new PipelineTransformDefinition
        {
            Kind = PipelineTransformKind.Derive,
            DerivedColumns =
            [
                new PipelineDerivedColumn { Name = "slug", Expression = "MissingFunction(name)" },
            ],
        });
        var batch = new PipelineRowBatch
        {
            BatchNumber = 1,
            StartingRowNumber = 1,
            Rows =
            [
                new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
                {
                    ["name"] = "Alice",
                },
            ],
        };

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await transform.TransformAsync(batch, CreateContext(), ct));

        Assert.Contains("Unknown scalar function", ex.Message);
    }

    private static PipelineExecutionContext CreateContext() => new()
    {
        RunId = "functions-test",
        Mode = PipelineExecutionMode.DryRun,
        Package = new PipelinePackageDefinition
        {
            Name = "functions",
            Version = "1.0",
            Source = new PipelineSourceDefinition { Kind = PipelineSourceKind.JsonFile, Path = "input.json" },
            Destination = new PipelineDestinationDefinition { Kind = PipelineDestinationKind.JsonFile, Path = "output.json" },
            Options = new PipelineExecutionOptions(),
        },
    };
}
