using System.Text.Json;
using System.Text.RegularExpressions;
using CSharpDB.Migration;
using Microsoft.Data.Sqlite;

namespace CSharpDB.Cli.Tests;

public sealed partial class SqliteMigrationCommandRunnerTests
{
    private static CancellationToken Cancellation =>
        TestContext.Current.CancellationToken;

    [Fact]
    public async Task InspectPlanAndPreview_UseRetainedBackupWithoutDisclosingInputPath()
    {
        using var workspace = new TemporaryDirectory();
        string sourcePath = workspace.PathFor("private-live-source.sqlite");
        string packagePath = workspace.PathFor("retained.csdbsqlite");
        string catalogPath = workspace.PathFor("catalog.json");
        string planPath = workspace.PathFor("plan.json");
        await CreateDatabaseAsync(sourcePath);
        byte[] sourceBefore = await File.ReadAllBytesAsync(
            sourcePath,
            Cancellation);

        var inspectOutput = new StringWriter();
        var inspectError = new StringWriter();
        int inspectCode = await MigrationCommandRunner.RunAsync(
            [
                "migrate", "inspect",
                "--source", "sqlite",
                "--input", sourcePath,
                "--package", packagePath,
                "--out", catalogPath,
                "--profile-sample-size", "2",
            ],
            inspectOutput,
            inspectError,
            Cancellation);

        Assert.True(
            inspectCode is InspectorCommandRunner.ExitOk or
                InspectorCommandRunner.ExitWarn,
            inspectError.ToString());
        Assert.True(string.IsNullOrWhiteSpace(inspectError.ToString()));
        Assert.True(File.Exists(packagePath));
        Assert.True(File.Exists(catalogPath));
        Assert.Equal(
            sourceBefore,
            await File.ReadAllBytesAsync(sourcePath, Cancellation));
        string catalogJson = await File.ReadAllTextAsync(
            catalogPath,
            Cancellation);
        Assert.DoesNotContain(sourcePath, inspectOutput.ToString(), StringComparison.Ordinal);
        Assert.DoesNotContain(sourcePath, catalogJson, StringComparison.Ordinal);
        string packageDigest = ReadManifestDigest(inspectOutput.ToString());
        Assert.Matches(
            "^sha256:[0-9a-f]{64}$",
            packageDigest);

        MigrationCatalog catalog =
            MigrationArtifactSerializer.DeserializeCatalog(catalogJson);
        Assert.Equal(MigrationSourceKind.Sqlite, catalog.Source.Kind);
        Assert.Contains(
            catalog.Objects
                .Where(item => item.Kind == MigrationObjectKind.Column)
                .SelectMany(item => item.Facets),
            facet =>
                facet.Name == "profileValuesExamined" &&
                facet.Value == "2");

        var planOutput = new StringWriter();
        var planError = new StringWriter();
        int planCode = await MigrationCommandRunner.RunAsync(
            [
                "migrate", "plan", catalogPath,
                "--out", planPath,
                "--accept-exclusions", "all",
            ],
            planOutput,
            planError,
            Cancellation);

        Assert.True(
            planCode is InspectorCommandRunner.ExitOk or
                InspectorCommandRunner.ExitWarn,
            planError.ToString());
        Assert.True(File.Exists(planPath));

        var previewOutput = new StringWriter();
        var previewError = new StringWriter();
        int previewCode = await MigrationCommandRunner.RunAsync(
            [
                "migrate", "preview", planPath,
                "--catalog", catalogPath,
                "--format", "json",
            ],
            previewOutput,
            previewError,
            Cancellation);

        Assert.True(
            previewCode is InspectorCommandRunner.ExitOk or
                InspectorCommandRunner.ExitWarn,
            previewError.ToString());
        using JsonDocument preview = JsonDocument.Parse(previewOutput.ToString());
        Assert.Equal(
            "csharpdb-migration-preview/v1",
            preview.RootElement.GetProperty("format").GetString());
    }

    [Fact]
    public async Task Inspect_RefusesCollisionsExistingOutputsAndUnknownOptions()
    {
        using var workspace = new TemporaryDirectory();
        string sourcePath = workspace.PathFor("source.sqlite");
        string packagePath = workspace.PathFor("retained.csdbsqlite");
        string catalogPath = workspace.PathFor("catalog.json");
        await CreateDatabaseAsync(sourcePath);
        byte[] sourceBefore = await File.ReadAllBytesAsync(
            sourcePath,
            Cancellation);

        (string Package, string Catalog)[] collisions =
        [
            (sourcePath, catalogPath),
            (packagePath, sourcePath),
            (packagePath, packagePath),
        ];
        foreach ((string package, string catalog) in collisions)
        {
            int code = await RunInspectAsync(
                sourcePath,
                package,
                catalog);
            Assert.Equal(InspectorCommandRunner.ExitUsage, code);
            Assert.Equal(
                sourceBefore,
                await File.ReadAllBytesAsync(sourcePath, Cancellation));
        }

        await File.WriteAllBytesAsync(packagePath, [0x01, 0x02], Cancellation);
        int existingPackageCode = await RunInspectAsync(
            sourcePath,
            packagePath,
            catalogPath);
        Assert.Equal(
            InspectorCommandRunner.ExitUsage,
            existingPackageCode);
        Assert.Equal(
            new byte[] { 0x01, 0x02 },
            await File.ReadAllBytesAsync(packagePath, Cancellation));
        File.Delete(packagePath);

        await File.WriteAllTextAsync(
            catalogPath,
            "existing catalog",
            Cancellation);
        int existingCatalogCode = await RunInspectAsync(
            sourcePath,
            packagePath,
            catalogPath);
        Assert.Equal(
            InspectorCommandRunner.ExitUsage,
            existingCatalogCode);
        Assert.False(File.Exists(packagePath));
        Assert.Equal(
            "existing catalog",
            await File.ReadAllTextAsync(catalogPath, Cancellation));

        var output = new StringWriter();
        var error = new StringWriter();
        int unknownOptionCode = await MigrationCommandRunner.RunAsync(
            [
                "migrate", "inspect",
                "--source", "sqlite",
                "--input", sourcePath,
                "--package", packagePath,
                "--out", workspace.PathFor("new-catalog.json"),
                "--connection", "Data Source=do-not-print;Password=secret",
            ],
            output,
            error,
            Cancellation);
        Assert.Equal(InspectorCommandRunner.ExitUsage, unknownOptionCode);
        Assert.DoesNotContain(
            "Password=secret",
            error.ToString(),
            StringComparison.Ordinal);
        Assert.False(File.Exists(packagePath));
    }

    [Fact]
    public async Task PlanAndPreview_RejectFutureSqliteCatalogContract()
    {
        using var workspace = new TemporaryDirectory();
        string sourcePath = workspace.PathFor("source.sqlite");
        string packagePath = workspace.PathFor("retained.csdbsqlite");
        string catalogPath = workspace.PathFor("catalog.json");
        string planPath = workspace.PathFor("plan.json");
        string futureCatalogPath = workspace.PathFor("future-catalog.json");
        await CreateDatabaseAsync(sourcePath);

        int inspectCode = await RunInspectAsync(
            sourcePath,
            packagePath,
            catalogPath);
        Assert.True(
            inspectCode is InspectorCommandRunner.ExitOk or
                InspectorCommandRunner.ExitWarn);
        int planCode = await MigrationCommandRunner.RunAsync(
            [
                "migrate", "plan", catalogPath,
                "--out", planPath,
                "--accept-exclusions", "all",
            ],
            TextWriter.Null,
            TextWriter.Null,
            Cancellation);
        Assert.True(
            planCode is InspectorCommandRunner.ExitOk or
                InspectorCommandRunner.ExitWarn);

        MigrationCatalog catalog = MigrationArtifactSerializer.DeserializeCatalog(
            await File.ReadAllTextAsync(catalogPath, Cancellation));
        MigrationCatalog future = catalog with
        {
            Objects = catalog.Objects.Select(item =>
                item.Kind != MigrationObjectKind.Namespace
                    ? item
                    : item with
                    {
                        Facets = item.Facets.Select(facet =>
                            facet.Name == "sqliteCatalogContract"
                                ? facet with
                                {
                                    Value =
                                        "csharpdb-sqlite-catalog-v2",
                                }
                                : facet).ToArray(),
                    }).ToArray(),
        };
        await File.WriteAllTextAsync(
            futureCatalogPath,
            MigrationArtifactSerializer.SerializeCatalog(future),
            Cancellation);

        var planError = new StringWriter();
        int futurePlanCode = await MigrationCommandRunner.RunAsync(
            [
                "migrate", "plan", futureCatalogPath,
                "--out", workspace.PathFor("future-plan.json"),
            ],
            TextWriter.Null,
            planError,
            Cancellation);
        Assert.Equal(InspectorCommandRunner.ExitUsage, futurePlanCode);
        Assert.Contains(
            "SQLite catalog contract v1",
            planError.ToString(),
            StringComparison.Ordinal);

        var previewError = new StringWriter();
        int futurePreviewCode = await MigrationCommandRunner.RunAsync(
            [
                "migrate", "preview", planPath,
                "--catalog", futureCatalogPath,
            ],
            TextWriter.Null,
            previewError,
            Cancellation);
        Assert.Equal(
            InspectorCommandRunner.ExitUsage,
            futurePreviewCode);
        Assert.Contains(
            "SQLite catalog contract v1",
            previewError.ToString(),
            StringComparison.Ordinal);
    }

    [Fact]
    public async Task Inspect_InvalidSourceDoesNotPrintRawInputPathOrProviderDetails()
    {
        using var workspace = new TemporaryDirectory();
        string sourcePath = workspace.PathFor(
            "private-provider-detail-do-not-print.sqlite");
        await File.WriteAllTextAsync(
            sourcePath,
            "not a sqlite database",
            Cancellation);

        var output = new StringWriter();
        var error = new StringWriter();
        int code = await MigrationCommandRunner.RunAsync(
            [
                "migrate", "inspect",
                "--source", "sqlite",
                "--input", sourcePath,
                "--package", workspace.PathFor("retained.csdbsqlite"),
                "--out", workspace.PathFor("catalog.json"),
            ],
            output,
            error,
            Cancellation);

        Assert.Equal(InspectorCommandRunner.ExitError, code);
        Assert.DoesNotContain(sourcePath, output.ToString(), StringComparison.Ordinal);
        Assert.DoesNotContain(sourcePath, error.ToString(), StringComparison.Ordinal);
        Assert.DoesNotContain(
            "private-provider-detail-do-not-print",
            error.ToString(),
            StringComparison.Ordinal);
    }

    private static async ValueTask<int> RunInspectAsync(
        string sourcePath,
        string packagePath,
        string catalogPath) =>
        await MigrationCommandRunner.RunAsync(
            [
                "migrate", "inspect",
                "--source", "sqlite",
                "--input", sourcePath,
                "--package", packagePath,
                "--out", catalogPath,
            ],
            TextWriter.Null,
            TextWriter.Null,
            Cancellation);

    private static string ReadManifestDigest(string output)
    {
        Match match = ManifestDigestPattern().Match(output);
        Assert.True(match.Success, $"No manifest digest was emitted: {output}");
        return match.Groups[1].Value;
    }

    private static async ValueTask CreateDatabaseAsync(string path)
    {
        var builder = new SqliteConnectionStringBuilder
        {
            DataSource = path,
            Mode = SqliteOpenMode.ReadWriteCreate,
            Pooling = false,
        };
        await using var connection = new SqliteConnection(
            builder.ConnectionString);
        await connection.OpenAsync(Cancellation);
        await using SqliteCommand command = connection.CreateCommand();
        command.CommandText =
            """
            CREATE TABLE items (
                id INTEGER PRIMARY KEY,
                label TEXT NOT NULL,
                amount NUMERIC
            );
            INSERT INTO items(id, label, amount) VALUES
                (1, 'one', 1),
                (2, 'two', 2),
                (3, 'three', 'mixed');
            """;
        await command.ExecuteNonQueryAsync(Cancellation);
    }

    [GeneratedRegex(
        @"(?:^|\|\s*)manifestDigest=(sha256:[0-9a-f]{64})(?:\s*\||\s*$)",
        RegexOptions.CultureInvariant)]
    private static partial Regex ManifestDigestPattern();

    private sealed class TemporaryDirectory : IDisposable
    {
        internal TemporaryDirectory()
        {
            Root = Path.Combine(
                Path.GetTempPath(),
                $"csharpdb_sqlite_cli_{Guid.NewGuid():N}");
            Directory.CreateDirectory(Root);
        }

        internal string Root { get; }

        internal string PathFor(string name) => Path.Combine(Root, name);

        public void Dispose()
        {
            try
            {
                Directory.Delete(Root, recursive: true);
            }
            catch (IOException)
            {
            }
            catch (UnauthorizedAccessException)
            {
            }
        }
    }
}
