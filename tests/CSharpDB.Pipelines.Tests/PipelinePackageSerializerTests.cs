using CSharpDB.Pipelines.Models;
using CSharpDB.Pipelines.Serialization;
using CSharpDB.Primitives;

namespace CSharpDB.Pipelines.Tests;

public sealed class PipelinePackageSerializerTests
{
    [Fact]
    public void Serialize_UsesCamelCaseAndEnumStrings()
    {
        var package = CreatePackage();

        string json = PipelinePackageSerializer.Serialize(package);

        Assert.Contains("\"name\"", json);
        Assert.Contains("\"errorMode\": \"skipBadRows\"", json);
        Assert.Contains("\"kind\": \"csvFile\"", json);
        Assert.Contains("\"targetType\": \"integer\"", json);
        Assert.Contains("\"event\": \"onRunSucceeded\"", json);
        Assert.Contains("\"automation\"", json);
        Assert.Contains("\"scalarFunctions\"", json);
    }

    [Fact]
    public void Deserialize_RoundTripsPackage()
    {
        var package = CreatePackage();

        string json = PipelinePackageSerializer.Serialize(package);
        PipelinePackageDefinition clone = PipelinePackageSerializer.Deserialize(json);

        Assert.Equal(package.Name, clone.Name);
        Assert.Equal(package.Version, clone.Version);
        Assert.Equal(package.Source.Kind, clone.Source.Kind);
        Assert.Equal(package.Source.Path, clone.Source.Path);
        Assert.Equal(package.Destination.Kind, clone.Destination.Kind);
        Assert.Equal(package.Destination.TableName, clone.Destination.TableName);
        Assert.Equal(package.Options.ErrorMode, clone.Options.ErrorMode);
        Assert.Equal(package.Transforms.Count, clone.Transforms.Count);
        Assert.Equal(package.Incremental?.WatermarkColumn, clone.Incremental?.WatermarkColumn);
        PipelineCommandHookDefinition hook = Assert.Single(clone.Hooks);
        Assert.Equal(PipelineCommandHookEvent.OnRunSucceeded, hook.Event);
        Assert.Equal("NotifyImport", hook.CommandName);
        Assert.Equal("ops", Assert.IsType<string>(hook.Arguments!["channel"]));
        Assert.Equal(3, DbCommandArguments.FromObject(hook.Arguments["priority"]).AsInteger);
        Assert.NotNull(clone.Automation);
        Assert.Contains(clone.Automation!.Commands!, command => command.Name == "NotifyImport");
        Assert.Contains(clone.Automation.ScalarFunctions!, function => function.Name == "NormalizeStatus" && function.Arity == 1);
        Assert.Contains(clone.Automation.ScalarFunctions!, function => function.Name == "Slugify" && function.Arity == 1);
    }

    [Fact]
    public async Task SaveToFileAsync_AndLoadFromFileAsync_RoundTripPackage()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        var package = CreatePackage();
        string path = Path.Combine(Path.GetTempPath(), $"pipeline-{Guid.NewGuid():N}.json");

        try
        {
            await PipelinePackageSerializer.SaveToFileAsync(package, path, ct);
            PipelinePackageDefinition loaded = await PipelinePackageSerializer.LoadFromFileAsync(path, ct);

        Assert.Equal(package.Name, loaded.Name);
        Assert.Equal(package.Transforms.Count, loaded.Transforms.Count);
        Assert.Equal(package.Options.BatchSize, loaded.Options.BatchSize);
        Assert.Single(loaded.Hooks);
        }
        finally
        {
            if (File.Exists(path))
            {
                File.Delete(path);
            }
        }
    }

    private static PipelinePackageDefinition CreatePackage() => new()
    {
        Name = "import_customers",
        Version = "1.0.0",
        Description = "Imports customers from csv.",
        Source = new PipelineSourceDefinition
        {
            Kind = PipelineSourceKind.CsvFile,
            Path = "data/customers.csv",
            HasHeaderRow = true,
        },
        Destination = new PipelineDestinationDefinition
        {
            Kind = PipelineDestinationKind.CSharpDbTable,
            TableName = "customers",
        },
        Options = new PipelineExecutionOptions
        {
            BatchSize = 100,
            CheckpointInterval = 10,
            ErrorMode = PipelineErrorMode.SkipBadRows,
            MaxRejects = 25,
        },
        Transforms =
        [
            new PipelineTransformDefinition
            {
                Kind = PipelineTransformKind.Rename,
                RenameMappings =
                [
                    new PipelineRenameMapping
                    {
                        Source = "customer_id",
                        Target = "id",
                    },
                ],
            },
            new PipelineTransformDefinition
            {
                Kind = PipelineTransformKind.Cast,
                CastMappings =
                [
                    new PipelineCastMapping
                    {
                        Column = "id",
                        TargetType = DbType.Integer,
                    },
                ],
            },
            new PipelineTransformDefinition
            {
                Kind = PipelineTransformKind.Filter,
                FilterExpression = "NormalizeStatus(status) == 'active'",
            },
            new PipelineTransformDefinition
            {
                Kind = PipelineTransformKind.Derive,
                DerivedColumns =
                [
                    new PipelineDerivedColumn
                    {
                        Name = "slug",
                        Expression = "Slugify(name)",
                    },
                ],
            },
        ],
        Incremental = new PipelineIncrementalOptions
        {
            WatermarkColumn = "updated_at",
            LastProcessedValue = "2026-01-01T00:00:00Z",
        },
        Hooks =
        [
            new PipelineCommandHookDefinition
            {
                Event = PipelineCommandHookEvent.OnRunSucceeded,
                CommandName = "NotifyImport",
                Arguments = new Dictionary<string, object?>
                {
                    ["channel"] = "ops",
                    ["priority"] = 3,
                },
            },
        ],
    };
}
