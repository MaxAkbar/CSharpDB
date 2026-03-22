using CSharpDB.Pipelines.Models;
using CSharpDB.Pipelines.Validation;
using CSharpDB.Primitives;

namespace CSharpDB.Pipelines.Tests;

public sealed class PipelinePackageValidatorTests
{
    [Fact]
    public void Validate_ReturnsSuccess_ForMinimalValidPipeline()
    {
        var package = CreateValidPackage();

        PipelineValidationResult result = PipelinePackageValidator.Validate(package);

        Assert.True(result.IsValid);
        Assert.Empty(result.Errors);
    }

    [Fact]
    public void Validate_ReturnsError_WhenNameIsMissing()
    {
        var package = new PipelinePackageDefinition
        {
            Name = " ",
            Version = "1.0.0",
            Description = CreateValidPackage().Description,
            Source = CreateValidPackage().Source,
            Destination = CreateValidPackage().Destination,
            Options = CreateValidPackage().Options,
            Transforms = CreateValidPackage().Transforms,
            Incremental = CreateValidPackage().Incremental,
        };

        PipelineValidationResult result = PipelinePackageValidator.Validate(package);

        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, e => e.Code == "pipeline.name.required");
    }

    [Fact]
    public void Validate_ReturnsError_WhenFileSourcePathIsMissing()
    {
        var validPackage = CreateValidPackage();
        var package = new PipelinePackageDefinition
        {
            Name = validPackage.Name,
            Version = validPackage.Version,
            Source = new PipelineSourceDefinition
            {
                Kind = PipelineSourceKind.CsvFile,
            },
            Destination = validPackage.Destination,
            Options = validPackage.Options,
            Transforms = validPackage.Transforms,
            Incremental = validPackage.Incremental,
        };

        PipelineValidationResult result = PipelinePackageValidator.Validate(package);

        Assert.Contains(result.Errors, e => e.Code == "pipeline.source.path.required");
    }

    [Fact]
    public void Validate_ReturnsError_WhenDestinationTableNameIsMissing()
    {
        var validPackage = CreateValidPackage();
        var package = new PipelinePackageDefinition
        {
            Name = validPackage.Name,
            Version = validPackage.Version,
            Source = validPackage.Source,
            Destination = new PipelineDestinationDefinition
            {
                Kind = PipelineDestinationKind.CSharpDbTable,
            },
            Options = validPackage.Options,
            Transforms = validPackage.Transforms,
            Incremental = validPackage.Incremental,
        };

        PipelineValidationResult result = PipelinePackageValidator.Validate(package);

        Assert.Contains(result.Errors, e => e.Code == "pipeline.destination.table.required");
    }

    [Fact]
    public void Validate_ReturnsError_WhenSelectTransformHasNoColumns()
    {
        var validPackage = CreateValidPackage();
        var package = new PipelinePackageDefinition
        {
            Name = validPackage.Name,
            Version = validPackage.Version,
            Source = validPackage.Source,
            Destination = validPackage.Destination,
            Options = validPackage.Options,
            Transforms =
            [
                new PipelineTransformDefinition
                {
                    Kind = PipelineTransformKind.Select,
                },
            ],
            Incremental = validPackage.Incremental,
        };

        PipelineValidationResult result = PipelinePackageValidator.Validate(package);

        Assert.Contains(result.Errors, e => e.Code == "pipeline.transform.select.columns.required");
    }

    [Fact]
    public void Validate_ReturnsError_WhenFailFastUsesMaxRejects()
    {
        var validPackage = CreateValidPackage();
        var package = new PipelinePackageDefinition
        {
            Name = validPackage.Name,
            Version = validPackage.Version,
            Source = validPackage.Source,
            Destination = validPackage.Destination,
            Options = new PipelineExecutionOptions
            {
                BatchSize = 250,
                CheckpointInterval = 50,
                ErrorMode = PipelineErrorMode.FailFast,
                MaxRejects = 10,
            },
            Transforms = validPackage.Transforms,
            Incremental = validPackage.Incremental,
        };

        PipelineValidationResult result = PipelinePackageValidator.Validate(package);

        Assert.Contains(result.Errors, e => e.Code == "pipeline.options.maxRejects.unsupported");
    }

    [Fact]
    public void Validate_ReturnsError_WhenIncrementalWatermarkIsMissing()
    {
        var validPackage = CreateValidPackage();
        var package = new PipelinePackageDefinition
        {
            Name = validPackage.Name,
            Version = validPackage.Version,
            Source = validPackage.Source,
            Destination = validPackage.Destination,
            Options = validPackage.Options,
            Transforms = validPackage.Transforms,
            Incremental = new PipelineIncrementalOptions(),
        };

        PipelineValidationResult result = PipelinePackageValidator.Validate(package);

        Assert.Contains(result.Errors, e => e.Code == "pipeline.incremental.watermark.required");
    }

    [Fact]
    public void Validate_ReturnsMultipleErrors_ForCompoundInvalidPackage()
    {
        var package = new PipelinePackageDefinition
        {
            Source = new PipelineSourceDefinition
            {
                Kind = PipelineSourceKind.SqlQuery,
            },
            Destination = new PipelineDestinationDefinition
            {
                Kind = PipelineDestinationKind.JsonFile,
            },
            Options = new PipelineExecutionOptions
            {
                BatchSize = 0,
                CheckpointInterval = 0,
                ErrorMode = PipelineErrorMode.SkipBadRows,
                MaxRejects = -1,
            },
            Transforms =
            [
                new PipelineTransformDefinition
                {
                    Kind = PipelineTransformKind.Filter,
                },
                new PipelineTransformDefinition
                {
                    Kind = PipelineTransformKind.Cast,
                    CastMappings =
                    [
                        new PipelineCastMapping
                        {
                            Column = "",
                            TargetType = DbType.Integer,
                        },
                    ],
                },
            ],
        };

        PipelineValidationResult result = PipelinePackageValidator.Validate(package);

        Assert.False(result.IsValid);
        Assert.True(result.Errors.Count >= 7);
        Assert.Contains(result.Errors, e => e.Code == "pipeline.version.required");
        Assert.Contains(result.Errors, e => e.Code == "pipeline.source.query.required");
        Assert.Contains(result.Errors, e => e.Code == "pipeline.destination.path.required");
        Assert.Contains(result.Errors, e => e.Code == "pipeline.options.batchSize.invalid");
        Assert.Contains(result.Errors, e => e.Code == "pipeline.options.checkpointInterval.invalid");
        Assert.Contains(result.Errors, e => e.Code == "pipeline.options.maxRejects.invalid");
        Assert.Contains(result.Errors, e => e.Code == "pipeline.transform.filter.expression.required");
        Assert.Contains(result.Errors, e => e.Code == "pipeline.transform.cast.column.required");
    }

    private static PipelinePackageDefinition CreateValidPackage() => new()
    {
        Name = "import_customers",
        Version = "1.0.0",
        Source = new PipelineSourceDefinition
        {
            Kind = PipelineSourceKind.CsvFile,
            Path = "data/customers.csv",
        },
        Destination = new PipelineDestinationDefinition
        {
            Kind = PipelineDestinationKind.CSharpDbTable,
            TableName = "customers",
        },
        Options = new PipelineExecutionOptions
        {
            BatchSize = 500,
            CheckpointInterval = 100,
            ErrorMode = PipelineErrorMode.SkipBadRows,
            MaxRejects = 100,
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
