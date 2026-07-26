using System.Text;
using System.Text.Json;
using CSharpDB.Migration;
using CSharpDB.Migration.Files.Csv;
using CSharpDB.Migration.Files.Json;

namespace CSharpDB.Cli.Tests;

[Collection("CliConsole")]
public sealed class MigrationSourcePreviewReleaseGateTests
{
    private const string PrivateMarker =
        "phase1-preview-private-source-marker";

    private static readonly string[] ExpectedPreviewProperties =
    [
        "blockingDiagnosticIds",
        "diagnostics",
        "excludedObjects",
        "format",
        "mappingProfile",
        "mappings",
        "objects",
        "pendingDiagnosticIds",
        "pendingExclusionObjectIds",
        "status",
        "targetCSharpDbVersion",
    ];

    private static CancellationToken Cancellation =>
        TestContext.Current.CancellationToken;

    [Fact]
    public async Task RetainedCsvPlan_DefaultJsonPreviewIsStableAndSafe()
    {
        using var directory = new PreviewTestDirectory("csv");
        string sourcePath = directory.PathFor(
            PrivateMarker + "-rows.csv");
        string packagePath = directory.PathFor("retained.csdbcsv");
        string catalogPath = directory.PathFor("catalog.json");
        string planPath = directory.PathFor("plan.json");
        await File.WriteAllTextAsync(
            sourcePath,
            "id,name\n1,alpha\n2,\"bravo, incorporated\"\n",
            new UTF8Encoding(false, true),
            Cancellation);

        await InspectAsync(
            [
                "migrate", "inspect",
                "--source", "csv",
                "--input", sourcePath,
                "--package", packagePath,
                "--out", catalogPath,
                "--delimiter", "comma",
                "--source-id", PrivateMarker,
                "--workspace", directory.Root,
                "--max-source-bytes", "1048576",
            ]);

        MigrationCatalog catalog = await ReadCatalogAsync(catalogPath);
        AssertSourceBinding(
            catalog,
            MigrationSourceKind.Csv,
            "csharpdb-csv-adapter-v1");
        Assert.Equal(
            CsvSchemaInferenceResult.AlgorithmId,
            TableFacet(catalog, "csvSchemaAlgorithm"));
        Assert.True(File.Exists(packagePath));
        Assert.True(new FileInfo(packagePath).Length > 0);

        MigrationPlan plan = await PlanAsync(catalogPath, planPath, catalog);
        AssertPlanSource(plan, catalog);
        await AssertStableSafePreviewAsync(
            planPath,
            catalogPath,
            directory);
    }

    [Theory]
    [InlineData("root-array")]
    [InlineData("ndjson")]
    public async Task RetainedJsonPlan_DefaultJsonPreviewIsStableAndSafe(
        string framing)
    {
        using var directory = new PreviewTestDirectory(
            "json-" + framing);
        string sourcePath = directory.PathFor(
            PrivateMarker +
            (framing == "root-array" ? "-rows.json" : "-rows.ndjson"));
        string packagePath = directory.PathFor("retained.csdbjson");
        string catalogPath = directory.PathFor("catalog.json");
        string planPath = directory.PathFor("plan.json");
        await WriteJsonSourceAsync(sourcePath, framing);

        await InspectAsync(
            [
                "migrate", "inspect",
                "--source", "json",
                "--input", sourcePath,
                "--package", packagePath,
                "--out", catalogPath,
                "--framing", framing,
                "--source-id", PrivateMarker,
                "--workspace", directory.Root,
                "--max-source-bytes", "1048576",
            ]);

        MigrationCatalog catalog = await ReadCatalogAsync(catalogPath);
        AssertSourceBinding(
            catalog,
            MigrationSourceKind.Json,
            "csharpdb-json-adapter-v1");
        Assert.Equal(
            JsonTableSchemaInferenceResult.AlgorithmId,
            TableFacet(catalog, "jsonSchemaAlgorithm"));
        Assert.Equal(
            framing == "root-array" ? "root-array" : "multiple-values",
            TableFacet(catalog, "jsonInputFraming"));
        Assert.DoesNotContain(
            catalog.Objects.SelectMany(item => item.Facets),
            facet => facet.Name.StartsWith(
                "jsonTyped",
                StringComparison.Ordinal));
        Assert.True(File.Exists(packagePath));
        Assert.True(new FileInfo(packagePath).Length > 0);

        MigrationPlan plan = await PlanAsync(catalogPath, planPath, catalog);
        AssertPlanSource(plan, catalog);
        await AssertStableSafePreviewAsync(
            planPath,
            catalogPath,
            directory);
    }

    [Theory]
    [InlineData("root-array")]
    [InlineData("ndjson")]
    public async Task RetainedTypedJsonPlan_DefaultJsonPreviewIsStableAndSafe(
        string framing)
    {
        using var directory = new PreviewTestDirectory(
            "typed-json-" + framing);
        string sourcePath = directory.PathFor(
            PrivateMarker +
            (framing == "root-array" ? "-typed.json" : "-typed.ndjson"));
        string sidecarPath = directory.PathFor(
            "typed-intent.csdbjson-intent.json");
        string packagePath = directory.PathFor("retained-typed.csdbjson");
        string catalogPath = directory.PathFor("typed-catalog.json");
        string planPath = directory.PathFor("typed-plan.json");
        await WriteJsonSourceAsync(sourcePath, framing);
        string intentDigest = await WriteTypedIntentAsync(
            sourcePath,
            sidecarPath,
            framing,
            directory.Root);

        await InspectAsync(
            [
                "migrate", "inspect",
                "--source", "json",
                "--input", sourcePath,
                "--typed-intent", sidecarPath,
                "--expected-intent-manifest-digest", intentDigest,
                "--package", packagePath,
                "--out", catalogPath,
                "--framing", framing,
                "--source-id", PrivateMarker,
                "--workspace", directory.Root,
                "--max-source-bytes", "1048576",
            ]);

        MigrationCatalog catalog = await ReadCatalogAsync(catalogPath);
        AssertSourceBinding(
            catalog,
            MigrationSourceKind.Json,
            "csharpdb-json-adapter-v1");
        Assert.Equal(
            JsonTypedTableSchemaInferenceResult.AlgorithmId,
            TableFacet(catalog, "jsonSchemaAlgorithm"));
        Assert.Equal(
            JsonTypedIntentSidecar.Format,
            TableFacet(catalog, "jsonTypedIntentFormat"));
        Assert.Equal(
            intentDigest,
            TableFacet(catalog, "jsonTypedIntentManifestDigest"));
        Assert.Equal(
            framing == "root-array" ? "root-array" : "multiple-values",
            TableFacet(catalog, "jsonInputFraming"));
        Assert.True(File.Exists(packagePath));
        Assert.True(new FileInfo(packagePath).Length > 0);

        MigrationPlan plan = await PlanAsync(catalogPath, planPath, catalog);
        AssertPlanSource(plan, catalog);
        await AssertStableSafePreviewAsync(
            planPath,
            catalogPath,
            directory);
    }

    private static async ValueTask InspectAsync(string[] arguments)
    {
        var output = new StringWriter();
        var error = new StringWriter();
        int code = await MigrationCommandRunner.RunAsync(
            arguments,
            output,
            error,
            Cancellation);

        AssertAcceptableExit(code, error);
        Assert.True(
            string.IsNullOrWhiteSpace(error.ToString()),
            error.ToString());
    }

    private static async ValueTask<MigrationPlan> PlanAsync(
        string catalogPath,
        string planPath,
        MigrationCatalog catalog)
    {
        var output = new StringWriter();
        var error = new StringWriter();
        int code = await MigrationCommandRunner.RunAsync(
            [
                "migrate", "plan", catalogPath,
                "--out", planPath,
                "--accept-exclusions", "all",
            ],
            output,
            error,
            Cancellation);

        AssertAcceptableExit(code, error);
        Assert.True(
            string.IsNullOrWhiteSpace(error.ToString()),
            error.ToString());
        Assert.True(File.Exists(planPath));
        return MigrationArtifactSerializer.DeserializePlan(
            await File.ReadAllTextAsync(planPath, Cancellation),
            catalog);
    }

    private static async ValueTask AssertStableSafePreviewAsync(
        string planPath,
        string catalogPath,
        PreviewTestDirectory directory)
    {
        (int firstCode, string firstOutput, string firstError) =
            await PreviewAsync(planPath, catalogPath);
        (int repeatedCode, string repeatedOutput, string repeatedError) =
            await PreviewAsync(planPath, catalogPath);

        AssertAcceptableExit(firstCode, firstError);
        Assert.Equal(firstCode, repeatedCode);
        Assert.Equal(firstOutput, repeatedOutput);
        Assert.True(string.IsNullOrWhiteSpace(firstError), firstError);
        Assert.True(string.IsNullOrWhiteSpace(repeatedError), repeatedError);

        using JsonDocument document = JsonDocument.Parse(firstOutput);
        JsonElement root = document.RootElement;
        Assert.Equal(JsonValueKind.Object, root.ValueKind);
        Assert.Equal(
            "csharpdb-migration-preview/v1",
            root.GetProperty("format").GetString());
        Assert.Equal(
            CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
            root.GetProperty("targetCSharpDbVersion").GetString());
        Assert.Contains(
            root.GetProperty("status").GetString(),
            new[] { "ready", "review-required", "blocked" });
        Assert.Equal(
            ExpectedPreviewProperties,
            root.EnumerateObject()
                .Select(property => property.Name)
                .Order(StringComparer.Ordinal)
                .ToArray());

        JsonElement objects = root.GetProperty("objects");
        int totalObjects = objects.GetProperty("total").GetInt32();
        int includedObjects = objects.GetProperty("included").GetInt32();
        int excludedObjects = objects.GetProperty("excluded").GetInt32();
        Assert.True(totalObjects > 0);
        Assert.Equal(totalObjects, includedObjects + excludedObjects);
        Assert.Equal(
            JsonValueKind.Array,
            root.GetProperty("diagnostics")
                .GetProperty("items")
                .ValueKind);
        Assert.Equal(
            JsonValueKind.Array,
            root.GetProperty("pendingDiagnosticIds").ValueKind);
        Assert.Equal(
            JsonValueKind.Array,
            root.GetProperty("pendingExclusionObjectIds").ValueKind);
        Assert.Equal(
            JsonValueKind.Array,
            root.GetProperty("blockingDiagnosticIds").ValueKind);

        Assert.False(root.TryGetProperty("generatedDdlDigest", out _));
        Assert.False(root.TryGetProperty("stages", out _));
        Assert.DoesNotContain(
            "CREATE TABLE",
            firstOutput,
            StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(
            "ALTER TABLE",
            firstOutput,
            StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(
            "DROP TABLE",
            firstOutput,
            StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(
            PrivateMarker,
            firstOutput,
            StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(
            directory.Root,
            firstOutput,
            StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(
            JsonEncodedPath(directory.Root),
            firstOutput,
            StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(
            Path.GetFileName(planPath),
            firstOutput,
            StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(
            Path.GetFileName(catalogPath),
            firstOutput,
            StringComparison.OrdinalIgnoreCase);
    }

    private static async ValueTask<(
        int Code,
        string Output,
        string Error)> PreviewAsync(
        string planPath,
        string catalogPath)
    {
        var output = new StringWriter();
        var error = new StringWriter();
        int code = await MigrationCommandRunner.RunAsync(
            [
                "migrate", "preview", planPath,
                "--catalog", catalogPath,
                "--format", "json",
            ],
            output,
            error,
            Cancellation);
        return (code, output.ToString(), error.ToString());
    }

    private static async ValueTask<MigrationCatalog> ReadCatalogAsync(
        string catalogPath) =>
        MigrationArtifactSerializer.DeserializeCatalog(
            await File.ReadAllTextAsync(catalogPath, Cancellation));

    private static void AssertSourceBinding(
        MigrationCatalog catalog,
        MigrationSourceKind expectedKind,
        string expectedProviderVersion)
    {
        Assert.Equal(
            CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
            catalog.TargetCSharpDbVersion);
        Assert.Equal(expectedKind, catalog.Source.Kind);
        Assert.Equal(
            expectedProviderVersion,
            catalog.Source.ProviderVersion);
        Assert.Null(catalog.Source.SourceVersion);
        Assert.Equal(
            MigrationConsistencyKind.Snapshot,
            catalog.Source.Consistency.Kind);
        Assert.DoesNotContain(
            PrivateMarker,
            catalog.Source.Identity,
            StringComparison.OrdinalIgnoreCase);
    }

    private static void AssertPlanSource(
        MigrationPlan plan,
        MigrationCatalog catalog)
    {
        Assert.Equal(catalog.Source, plan.Source);
        Assert.Equal(
            catalog.TargetCSharpDbVersion,
            plan.TargetCSharpDbVersion);
        Assert.Equal(
            MigrationArtifactSerializer.ComputeCatalogDigest(catalog),
            plan.CatalogDigest);
    }

    private static string TableFacet(
        MigrationCatalog catalog,
        string name)
    {
        MigrationCatalogObject table = Assert.Single(
            catalog.Objects,
            item => item.Kind == MigrationObjectKind.Table);
        MigrationCatalogFacet facet = Assert.Single(
            table.Facets,
            facet => string.Equals(
                facet.Name,
                name,
                StringComparison.Ordinal));
        return Assert.IsType<string>(facet.Value);
    }

    private static async ValueTask WriteJsonSourceAsync(
        string sourcePath,
        string framing)
    {
        string contents = framing switch
        {
            "root-array" =>
                """
                [
                  {"value":"42","name":"alpha"},
                  {"value":"43","name":"bravo"}
                ]
                """,
            "ndjson" =>
                """
                {"value":"42","name":"alpha"}
                {"value":"43","name":"bravo"}

                """,
            _ => throw new ArgumentOutOfRangeException(nameof(framing)),
        };
        await File.WriteAllTextAsync(
            sourcePath,
            contents,
            new UTF8Encoding(false, true),
            Cancellation);
    }

    private static async ValueTask<string> WriteTypedIntentAsync(
        string sourcePath,
        string sidecarPath,
        string framing,
        string workspacePath)
    {
        await using JsonSourceSnapshot snapshot =
            await JsonSourceSnapshot.CreateFromFileAsync(
                sourcePath,
                new JsonSourceSnapshotOptions
                {
                    WorkspacePath = workspacePath,
                    MaxSourceBytes = 1024 * 1024,
                },
                Cancellation);
        JsonSourceBinding binding = await JsonSourceBinding.CreateAsync(
            snapshot,
            new JsonStreamingReaderOptions
            {
                Framing = framing == "root-array"
                    ? JsonInputFraming.RootArray
                    : JsonInputFraming.MultipleValues,
            },
            PrivateMarker,
            Cancellation);
        JsonTypedIntentManifest manifest =
            await JsonTypedIntentSidecar.WriteAsync(
                sidecarPath,
                binding,
                new JsonTypedIntentOptions
                {
                    Columns =
                    [
                        new JsonTypedColumnIntent
                        {
                            ColumnIndex = 0,
                            ExpectedPropertyName = "value",
                            Codec = JsonTypedValueCodec.Int64String,
                            Nullable = false,
                            MissingPolicy =
                                JsonMissingPropertyPolicy.Reject,
                        },
                    ],
                },
                Cancellation);
        return manifest.ManifestDigest;
    }

    private static void AssertAcceptableExit(
        int code,
        object error)
    {
        Assert.True(
            code is InspectorCommandRunner.ExitOk or
                InspectorCommandRunner.ExitWarn,
            error.ToString());
    }

    private static string JsonEncodedPath(string path) =>
        JsonSerializer.Serialize(path).Trim('"');

    private sealed class PreviewTestDirectory : IDisposable
    {
        internal PreviewTestDirectory(string scenario)
        {
            Root = Path.Combine(
                Path.GetTempPath(),
                $"csharpdb-{PrivateMarker}-{scenario}-{Guid.NewGuid():N}");
            Directory.CreateDirectory(Root);
        }

        internal string Root { get; }

        internal string PathFor(string leafName) =>
            Path.Combine(Root, leafName);

        public void Dispose()
        {
            if (Directory.Exists(Root))
                Directory.Delete(Root, recursive: true);
        }
    }
}
