using System.Text.Json;
using CSharpDB.Pipelines.Models;
using CSharpDB.Pipelines.Runtime;
using CSharpDB.Pipelines.Runtime.BuiltIns;
using CSharpDB.Primitives;

namespace CSharpDB.Pipelines.Tests;

public sealed class BuiltInPipelineComponentsTests
{
    [Fact]
    public async Task CsvPipelineSource_ReadsRowsInBatches()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string path = Path.Combine(Path.GetTempPath(), $"csv-source-{Guid.NewGuid():N}.csv");
        await File.WriteAllLinesAsync(path,
        [
            "id,name",
            "1,Alice",
            "2,Bob",
            "3,Carol",
        ], ct);

        try
        {
            var source = new CsvPipelineSource(new PipelineSourceDefinition
            {
                Kind = PipelineSourceKind.CsvFile,
                Path = path,
                HasHeaderRow = true,
            });

            var context = CreateContext(batchSize: 2);
            await source.OpenAsync(context, ct);
            List<PipelineRowBatch> batches = await source.ReadBatchesAsync(context, ct).ToListAsync(ct);

            Assert.Equal(2, batches.Count);
            Assert.Equal("Alice", batches[0].Rows[0]["name"]);
            Assert.Equal("3", batches[1].Rows[0]["id"]);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task CsvPipelineSource_ResolvesRelativePathsFromAppBaseDirectory()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string dataDirectory = Path.Combine(AppContext.BaseDirectory, "data");
        string path = Path.Combine(dataDirectory, $"csv-source-{Guid.NewGuid():N}.csv");
        string relativePath = Path.GetRelativePath(AppContext.BaseDirectory, path);
        Directory.CreateDirectory(dataDirectory);
        await File.WriteAllLinesAsync(path,
        [
            "id,name",
            "1,Alice",
        ], ct);

        string originalCurrentDirectory = Directory.GetCurrentDirectory();
        string isolatedCurrentDirectory = Path.Combine(Path.GetTempPath(), $"csv-cwd-{Guid.NewGuid():N}");
        Directory.CreateDirectory(isolatedCurrentDirectory);

        try
        {
            Directory.SetCurrentDirectory(isolatedCurrentDirectory);

            var source = new CsvPipelineSource(new PipelineSourceDefinition
            {
                Kind = PipelineSourceKind.CsvFile,
                Path = relativePath,
                HasHeaderRow = true,
            });

            var context = CreateContext(batchSize: 10);
            await source.OpenAsync(context, ct);
            List<PipelineRowBatch> batches = await source.ReadBatchesAsync(context, ct).ToListAsync(ct);

            Assert.Single(batches);
            Assert.Equal("Alice", batches[0].Rows[0]["name"]);
        }
        finally
        {
            Directory.SetCurrentDirectory(originalCurrentDirectory);
            File.Delete(path);
            Directory.Delete(isolatedCurrentDirectory);
        }
    }

    [Fact]
    public async Task JsonPipelineSource_ReadsObjectArray()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string path = Path.Combine(Path.GetTempPath(), $"json-source-{Guid.NewGuid():N}.json");
        await File.WriteAllTextAsync(path, """
[
  { "id": 1, "name": "Alice" },
  { "id": 2, "name": "Bob" }
]
""", ct);

        try
        {
            var source = new JsonPipelineSource(new PipelineSourceDefinition
            {
                Kind = PipelineSourceKind.JsonFile,
                Path = path,
            });

            var context = CreateContext(batchSize: 10);
            await source.OpenAsync(context, ct);
            List<PipelineRowBatch> batches = await source.ReadBatchesAsync(context, ct).ToListAsync(ct);

            Assert.Single(batches);
            Assert.Equal(2L, Convert.ToInt64(batches[0].Rows[1]["id"]));
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task JsonPipelineSource_ResolvesRelativePathsFromAppBaseDirectory()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string dataDirectory = Path.Combine(AppContext.BaseDirectory, "data");
        string path = Path.Combine(dataDirectory, $"json-source-{Guid.NewGuid():N}.json");
        string relativePath = Path.GetRelativePath(AppContext.BaseDirectory, path);
        Directory.CreateDirectory(dataDirectory);
        await File.WriteAllTextAsync(path, """
[
  { "id": 1, "name": "Alice" }
]
""", ct);

        string originalCurrentDirectory = Directory.GetCurrentDirectory();
        string isolatedCurrentDirectory = Path.Combine(Path.GetTempPath(), $"json-cwd-{Guid.NewGuid():N}");
        Directory.CreateDirectory(isolatedCurrentDirectory);

        try
        {
            Directory.SetCurrentDirectory(isolatedCurrentDirectory);

            var source = new JsonPipelineSource(new PipelineSourceDefinition
            {
                Kind = PipelineSourceKind.JsonFile,
                Path = relativePath,
            });

            var context = CreateContext(batchSize: 10);
            await source.OpenAsync(context, ct);
            List<PipelineRowBatch> batches = await source.ReadBatchesAsync(context, ct).ToListAsync(ct);

            Assert.Single(batches);
            Assert.Equal("Alice", batches[0].Rows[0]["name"]);
        }
        finally
        {
            Directory.SetCurrentDirectory(originalCurrentDirectory);
            File.Delete(path);
            Directory.Delete(isolatedCurrentDirectory);
        }
    }

    [Fact]
    public async Task CsvPipelineSource_ResumeMode_SkipsCompletedBatches()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string path = Path.Combine(Path.GetTempPath(), $"csv-resume-source-{Guid.NewGuid():N}.csv");
        await File.WriteAllLinesAsync(path,
        [
            "id,name",
            "1,Alice",
            "2,Bob",
            "3,Carol",
        ], ct);

        try
        {
            var source = new CsvPipelineSource(new PipelineSourceDefinition
            {
                Kind = PipelineSourceKind.CsvFile,
                Path = path,
                HasHeaderRow = true,
            });

            var context = CreateContext(batchSize: 2);
            context = new PipelineExecutionContext
            {
                RunId = context.RunId,
                Package = context.Package,
                Mode = PipelineExecutionMode.Resume,
                Checkpoint = new PipelineCheckpointState
                {
                    BatchNumber = 1,
                    StepName = "destination-write",
                    UpdatedUtc = DateTimeOffset.UtcNow,
                },
            };

            List<PipelineRowBatch> batches = await source.ReadBatchesAsync(context, ct).ToListAsync(ct);

            Assert.Single(batches);
            Assert.Equal(2L, batches[0].BatchNumber);
            Assert.Equal("3", batches[0].Rows[0]["id"]);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task CsvPipelineDestination_WritesHeaderAndRows()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string path = Path.Combine(Path.GetTempPath(), $"csv-destination-{Guid.NewGuid():N}.csv");
        var destination = new CsvPipelineDestination(new PipelineDestinationDefinition
        {
            Kind = PipelineDestinationKind.CsvFile,
            Path = path,
            Overwrite = true,
        });

        try
        {
            var context = CreateContext();
            await destination.InitializeAsync(context, ct);
            await destination.WriteBatchAsync(CreateBatch(
                new Dictionary<string, object?> { ["id"] = 1, ["name"] = "Alice" },
                new Dictionary<string, object?> { ["id"] = 2, ["name"] = "Bob" }), context, ct);
            await destination.CompleteAsync(context, ct);

            string[] lines = await File.ReadAllLinesAsync(path, ct);
            Assert.Equal("id,name", lines[0]);
            Assert.Contains("Alice", lines[1]);
            Assert.Contains("Bob", lines[2]);
        }
        finally
        {
            if (File.Exists(path))
            {
                File.Delete(path);
            }
        }
    }

    [Fact]
    public async Task JsonPipelineDestination_WritesObjectArray()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        string path = Path.Combine(Path.GetTempPath(), $"json-destination-{Guid.NewGuid():N}.json");
        var destination = new JsonPipelineDestination(new PipelineDestinationDefinition
        {
            Kind = PipelineDestinationKind.JsonFile,
            Path = path,
        });

        try
        {
            var context = CreateContext();
            await destination.InitializeAsync(context, ct);
            await destination.WriteBatchAsync(CreateBatch(
                new Dictionary<string, object?> { ["id"] = 1L, ["name"] = "Alice" }), context, ct);
            await destination.CompleteAsync(context, ct);

            using var document = JsonDocument.Parse(await File.ReadAllTextAsync(path, ct));
            Assert.Equal(JsonValueKind.Array, document.RootElement.ValueKind);
            Assert.Equal("Alice", document.RootElement[0].GetProperty("name").GetString());
        }
        finally
        {
            if (File.Exists(path))
            {
                File.Delete(path);
            }
        }
    }

    [Fact]
    public async Task BuiltInTransforms_ProjectRenameCastFilterDeriveAndDeduplicate()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        var transforms = new IPipelineTransform[]
        {
            new SelectPipelineTransform(new PipelineTransformDefinition
            {
                Kind = PipelineTransformKind.Select,
                SelectColumns = ["id", "name", "status"],
            }),
            new RenamePipelineTransform(new PipelineTransformDefinition
            {
                Kind = PipelineTransformKind.Rename,
                RenameMappings =
                [
                    new PipelineRenameMapping { Source = "name", Target = "full_name" },
                ],
            }),
            new CastPipelineTransform(new PipelineTransformDefinition
            {
                Kind = PipelineTransformKind.Cast,
                CastMappings =
                [
                    new PipelineCastMapping { Column = "id", TargetType = DbType.Integer },
                ],
            }),
            new FilterPipelineTransform(new PipelineTransformDefinition
            {
                Kind = PipelineTransformKind.Filter,
                FilterExpression = "status == 'active'",
            }),
            new DerivePipelineTransform(new PipelineTransformDefinition
            {
                Kind = PipelineTransformKind.Derive,
                DerivedColumns =
                [
                    new PipelineDerivedColumn { Name = "name_copy", Expression = "full_name" },
                ],
            }),
            new DeduplicatePipelineTransform(new PipelineTransformDefinition
            {
                Kind = PipelineTransformKind.Deduplicate,
                DeduplicateKeys = ["id"],
            }),
        };

        var batch = CreateBatch(
            new Dictionary<string, object?> { ["id"] = "1", ["name"] = "Alice", ["status"] = "active" },
            new Dictionary<string, object?> { ["id"] = "1", ["name"] = "Alice", ["status"] = "active" },
            new Dictionary<string, object?> { ["id"] = "2", ["name"] = "Bob", ["status"] = "inactive" });

        var context = CreateContext();
        PipelineRowBatch current = batch;
        foreach (var transform in transforms)
        {
            current = await transform.TransformAsync(current, context, ct);
        }

        Assert.Single(current.Rows);
        Assert.Equal(1L, current.Rows[0]["id"]);
        Assert.False(current.Rows[0].ContainsKey("name"));
        Assert.Equal("Alice", current.Rows[0]["full_name"]);
        Assert.Equal("Alice", current.Rows[0]["name_copy"]);
    }

    [Fact]
    public void DefaultPipelineComponentFactory_SupportsFileComponents()
    {
        var factory = new DefaultPipelineComponentFactory();

        IPipelineSource source = factory.CreateSource(new PipelineSourceDefinition
        {
            Kind = PipelineSourceKind.CsvFile,
            Path = "input.csv",
        });
        IPipelineDestination destination = factory.CreateDestination(new PipelineDestinationDefinition
        {
            Kind = PipelineDestinationKind.JsonFile,
            Path = "output.json",
        });
        IReadOnlyList<IPipelineTransform> transforms =
        [
            .. factory.CreateTransforms(
            [
                new PipelineTransformDefinition
                {
                    Kind = PipelineTransformKind.Filter,
                    FilterExpression = "id == 1",
                },
            ]),
        ];

        Assert.IsType<CsvPipelineSource>(source);
        Assert.IsType<JsonPipelineDestination>(destination);
        Assert.Single(transforms);
        Assert.IsType<FilterPipelineTransform>(transforms[0]);
    }

    private static PipelineExecutionContext CreateContext(int batchSize = 100) => new()
    {
        RunId = Guid.NewGuid().ToString("N"),
        Mode = PipelineExecutionMode.Run,
        Package = new PipelinePackageDefinition
        {
            Name = "test",
            Version = "1.0.0",
            Source = new PipelineSourceDefinition
            {
                Kind = PipelineSourceKind.CsvFile,
                Path = "input.csv",
            },
            Destination = new PipelineDestinationDefinition
            {
                Kind = PipelineDestinationKind.JsonFile,
                Path = "output.json",
            },
            Options = new PipelineExecutionOptions
            {
                BatchSize = batchSize,
                CheckpointInterval = 1,
                ErrorMode = PipelineErrorMode.SkipBadRows,
                MaxRejects = 10,
            },
        },
    };

    private static PipelineRowBatch CreateBatch(params Dictionary<string, object?>[] rows) => new()
    {
        BatchNumber = 1,
        StartingRowNumber = 1,
        Rows = rows,
    };
}
