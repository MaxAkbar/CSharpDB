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
    }

    [Fact]
    public async Task SaveToFileAsync_AndLoadFromFileAsync_RoundTripPackage()
    {
        var package = CreatePackage();
        string path = Path.Combine(Path.GetTempPath(), $"pipeline-{Guid.NewGuid():N}.json");

        try
        {
            await PipelinePackageSerializer.SaveToFileAsync(package, path);
            PipelinePackageDefinition loaded = await PipelinePackageSerializer.LoadFromFileAsync(path);

            Assert.Equal(package.Name, loaded.Name);
            Assert.Equal(package.Transforms.Count, loaded.Transforms.Count);
            Assert.Equal(package.Options.BatchSize, loaded.Options.BatchSize);
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
        ],
        Incremental = new PipelineIncrementalOptions
        {
            WatermarkColumn = "updated_at",
            LastProcessedValue = "2026-01-01T00:00:00Z",
        },
    };
}
