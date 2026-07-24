using System.Text.Json;
using System.Text.RegularExpressions;
using CSharpDB.Engine;
using CSharpDB.Migration;
using CSharpDB.Migration.Validation;
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

    [Fact]
    public async Task RetainedBackup_ApplyResumeAndChecksumValidateAfterLiveSourceDeletion()
    {
        using var workspace = new TemporaryDirectory();
        SqliteArtifacts artifacts =
            await CreateApplyReadyArtifactsAsync(workspace.Root);
        byte[] retainedBytes = await File.ReadAllBytesAsync(
            artifacts.PackagePath,
            Cancellation);
        File.Delete(artifacts.SourcePath);
        string targetPath = workspace.PathFor("staged.csdb");
        string runPath = workspace.PathFor("run.json");

        var applyOutput = new StringWriter();
        var applyError = new StringWriter();
        int applyCode = await MigrationCommandRunner.RunAsync(
            ApplyArguments(
                artifacts,
                targetPath,
                runPath,
                [
                    "--source-package", artifacts.PackagePath,
                    "--expected-manifest-digest", artifacts.ManifestDigest,
                    "--workspace", workspace.Root,
                    "--format", "json",
                ]),
            applyOutput,
            applyError,
            Cancellation);

        Assert.Equal(InspectorCommandRunner.ExitWarn, applyCode);
        Assert.True(string.IsNullOrWhiteSpace(applyError.ToString()));
        Assert.False(File.Exists(artifacts.SourcePath));
        Assert.Equal(
            retainedBytes,
            await File.ReadAllBytesAsync(
                artifacts.PackagePath,
                Cancellation));
        Assert.True(File.Exists(targetPath));
        using JsonDocument applied = JsonDocument.Parse(
            applyOutput.ToString());
        JsonElement appliedReport = applied.RootElement;
        Assert.Equal(
            "csharpdb-sqlite-backup-v1",
            appliedReport.GetProperty("sourcePackageFormat").GetString());
        Assert.Equal(
            artifacts.ManifestDigest,
            appliedReport
                .GetProperty("sourcePackageManifestDigest")
                .GetString());
        Assert.Equal(3, appliedReport.GetProperty("rowsWritten").GetInt64());
        long firstBatches = appliedReport
            .GetProperty("batchesWritten")
            .GetInt64();
        Assert.True(firstBatches > 0);
        AssertNoSqliteWorkspaceDirectories(workspace.Root);

        string resumePath = workspace.PathFor("run-resume.json");
        var resumeOutput = new StringWriter();
        var resumeError = new StringWriter();
        int resumeCode = await MigrationCommandRunner.RunAsync(
            ApplyArguments(
                artifacts,
                targetPath,
                resumePath,
                [
                    "--source-package", artifacts.PackagePath,
                    "--expected-manifest-digest", artifacts.ManifestDigest,
                    "--workspace", workspace.Root,
                    "--resume",
                    "--format", "json",
                ]),
            resumeOutput,
            resumeError,
            Cancellation);

        Assert.Equal(InspectorCommandRunner.ExitWarn, resumeCode);
        Assert.True(string.IsNullOrWhiteSpace(resumeError.ToString()));
        using JsonDocument resumed = JsonDocument.Parse(
            resumeOutput.ToString());
        Assert.Equal(
            "csharpdb-sqlite-backup-v1",
            resumed.RootElement
                .GetProperty("sourcePackageFormat")
                .GetString());
        Assert.Equal(
            0,
            resumed.RootElement.GetProperty("batchesWritten").GetInt64());
        Assert.Equal(
            firstBatches,
            resumed.RootElement.GetProperty("batchesSkipped").GetInt64());
        Assert.Equal(
            3,
            resumed.RootElement.GetProperty("rowsSkipped").GetInt64());
        AssertNoSqliteWorkspaceDirectories(workspace.Root);

        string validationPath = workspace.PathFor("validation.json");
        var validationOutput = new StringWriter();
        var validationError = new StringWriter();
        int validationCode = await MigrationCommandRunner.RunAsync(
            [
                "migrate", "validate", artifacts.PlanPath,
                "--catalog", artifacts.CatalogPath,
                "--source-package", artifacts.PackagePath,
                "--expected-manifest-digest", artifacts.ManifestDigest,
                "--workspace", workspace.Root,
                "--target", targetPath,
                "--out", validationPath,
                "--level", "checksum",
                "--spill-dir", workspace.Root,
            ],
            validationOutput,
            validationError,
            Cancellation);

        Assert.Equal(InspectorCommandRunner.ExitWarn, validationCode);
        Assert.True(string.IsNullOrWhiteSpace(validationError.ToString()));
        Assert.Contains(
            "Status: PASSED",
            validationOutput.ToString(),
            StringComparison.Ordinal);
        MigrationValidationReport validation =
            MigrationValidationReportSerializer.Deserialize(
                await File.ReadAllTextAsync(
                    validationPath,
                    Cancellation));
        Assert.Equal(MigrationValidationStatus.Passed, validation.Outcome);
        Assert.Equal(MigrationValidationLevel.Checksum, validation.Level);
        Assert.Equal(
            retainedBytes,
            await File.ReadAllBytesAsync(
                artifacts.PackagePath,
                Cancellation));
        AssertNoSqliteWorkspaceDirectories(workspace.Root);

        await using var database =
            await Database.OpenAsync(targetPath, Cancellation);
        await using var query = await database.ExecuteAsync(
            "SELECT id, label FROM items ORDER BY id;",
            Cancellation);
        var rows = await query.ToListAsync(Cancellation);
        Assert.Equal([1L, 2L, 3L], rows.Select(row => row[0].AsInteger));
        Assert.Equal(
            ["one", "two", "three"],
            rows.Select(row => row[1].AsText));
    }

    [Fact]
    public async Task RetainedBackup_RequiresPackageAndDigestBeforeTargetCreation()
    {
        using var workspace = new TemporaryDirectory();
        SqliteArtifacts artifacts =
            await CreateApplyReadyArtifactsAsync(workspace.Root);
        string targetPath = workspace.PathFor("staged.csdb");

        string[][] omittedOptionArguments =
        [
            ["--expected-manifest-digest", artifacts.ManifestDigest],
            ["--source-package", artifacts.PackagePath],
        ];
        foreach (string[] suffix in omittedOptionArguments)
        {
            string reportPath = workspace.PathFor(
                $"run-{Guid.NewGuid():N}.json");
            var error = new StringWriter();
            int code = await MigrationCommandRunner.RunAsync(
                ApplyArguments(
                    artifacts,
                    targetPath,
                    reportPath,
                    suffix),
                TextWriter.Null,
                error,
                Cancellation);

            Assert.Equal(InspectorCommandRunner.ExitUsage, code);
            Assert.Contains(
                "Missing required option",
                error.ToString(),
                StringComparison.Ordinal);
            AssertTargetWasNotCreated(targetPath, reportPath);
        }
    }

    [Fact]
    public async Task RetainedBackup_IntegrityAndSizeFailuresOccurBeforeTargetCreationOrAccess()
    {
        using var workspace = new TemporaryDirectory();
        SqliteArtifacts artifacts =
            await CreateApplyReadyArtifactsAsync(workspace.Root);
        string targetPath = workspace.PathFor("missing-target.csdb");
        string wrongDigest = DifferentDigest(artifacts.ManifestDigest);
        string applyReportPath = workspace.PathFor("wrong-pin-run.json");
        var wrongPinError = new StringWriter();

        int wrongPinCode = await MigrationCommandRunner.RunAsync(
            ApplyArguments(
                artifacts,
                targetPath,
                applyReportPath,
                [
                    "--source-package", artifacts.PackagePath,
                    "--expected-manifest-digest", wrongDigest,
                    "--workspace", workspace.Root,
                ]),
            TextWriter.Null,
            wrongPinError,
            Cancellation);

        Assert.Equal(InspectorCommandRunner.ExitError, wrongPinCode);
        Assert.Contains(
            "digest",
            wrongPinError.ToString(),
            StringComparison.OrdinalIgnoreCase);
        AssertTargetWasNotCreated(targetPath, applyReportPath);
        AssertNoSqliteWorkspaceDirectories(workspace.Root);

        string validationPath = workspace.PathFor(
            "wrong-pin-validation.json");
        var validationError = new StringWriter();
        int validationCode = await MigrationCommandRunner.RunAsync(
            [
                "migrate", "validate", artifacts.PlanPath,
                "--catalog", artifacts.CatalogPath,
                "--source-package", artifacts.PackagePath,
                "--expected-manifest-digest", wrongDigest,
                "--workspace", workspace.Root,
                "--target", targetPath,
                "--out", validationPath,
                "--level", "checksum",
                "--spill-dir", workspace.Root,
            ],
            TextWriter.Null,
            validationError,
            Cancellation);

        Assert.Equal(InspectorCommandRunner.ExitError, validationCode);
        Assert.Contains(
            "digest",
            validationError.ToString(),
            StringComparison.OrdinalIgnoreCase);
        AssertTargetWasNotCreated(targetPath, validationPath);
        AssertNoSqliteWorkspaceDirectories(workspace.Root);

        string byteLimitReportPath = workspace.PathFor(
            "byte-limit-run.json");
        var byteLimitError = new StringWriter();
        int byteLimitCode = await MigrationCommandRunner.RunAsync(
            ApplyArguments(
                artifacts,
                targetPath,
                byteLimitReportPath,
                [
                    "--source-package", artifacts.PackagePath,
                    "--expected-manifest-digest", artifacts.ManifestDigest,
                    "--workspace", workspace.Root,
                    "--max-source-bytes", "1",
                ]),
            TextWriter.Null,
            byteLimitError,
            Cancellation);

        Assert.Equal(InspectorCommandRunner.ExitError, byteLimitCode);
        Assert.Contains(
            "byte limit",
            byteLimitError.ToString(),
            StringComparison.OrdinalIgnoreCase);
        AssertTargetWasNotCreated(targetPath, byteLimitReportPath);
        AssertNoSqliteWorkspaceDirectories(workspace.Root);
    }

    [Fact]
    public async Task RetainedBackup_TamperAndCatalogMismatchFailBeforeTargetCreation()
    {
        using var workspace = new TemporaryDirectory();
        string firstRoot = workspace.PathFor("first");
        string secondRoot = workspace.PathFor("second");
        Directory.CreateDirectory(firstRoot);
        Directory.CreateDirectory(secondRoot);
        SqliteArtifacts first =
            await CreateApplyReadyArtifactsAsync(firstRoot);
        SqliteArtifacts second =
            await CreateApplyReadyArtifactsAsync(
                secondRoot,
                firstId: 10);
        string targetPath = workspace.PathFor("staged.csdb");

        await using (var package = new FileStream(
            first.PackagePath,
            FileMode.Append,
            FileAccess.Write,
            FileShare.None))
        {
            await package.WriteAsync(
                new byte[] { 0x5a },
                Cancellation);
        }
        string tamperReportPath = workspace.PathFor("tamper-run.json");
        var tamperError = new StringWriter();
        int tamperCode = await MigrationCommandRunner.RunAsync(
            ApplyArguments(
                first,
                targetPath,
                tamperReportPath,
                [
                    "--source-package", first.PackagePath,
                    "--expected-manifest-digest", first.ManifestDigest,
                    "--workspace", workspace.Root,
                ]),
            TextWriter.Null,
            tamperError,
            Cancellation);

        Assert.Equal(InspectorCommandRunner.ExitError, tamperCode);
        Assert.Contains(
            "digest",
            tamperError.ToString(),
            StringComparison.OrdinalIgnoreCase);
        AssertTargetWasNotCreated(targetPath, tamperReportPath);
        AssertNoSqliteWorkspaceDirectories(workspace.Root);

        string mismatchReportPath = workspace.PathFor(
            "catalog-mismatch-run.json");
        var mismatchError = new StringWriter();
        int mismatchCode = await MigrationCommandRunner.RunAsync(
            ApplyArguments(
                first,
                targetPath,
                mismatchReportPath,
                [
                    "--source-package", second.PackagePath,
                    "--expected-manifest-digest", second.ManifestDigest,
                    "--workspace", workspace.Root,
                ]),
            TextWriter.Null,
            mismatchError,
            Cancellation);

        Assert.Equal(InspectorCommandRunner.ExitError, mismatchCode);
        Assert.Contains(
            "catalog",
            mismatchError.ToString(),
            StringComparison.OrdinalIgnoreCase);
        AssertTargetWasNotCreated(targetPath, mismatchReportPath);
        AssertNoSqliteWorkspaceDirectories(workspace.Root);
    }

    [Fact]
    public async Task Inspect_SourceByteLimitIsValidatedAndLeavesNoPartialArtifacts()
    {
        using var workspace = new TemporaryDirectory();
        string sourcePath = workspace.PathFor(
            "private-oversized-source.sqlite");
        string packagePath = workspace.PathFor("retained.csdbsqlite");
        string catalogPath = workspace.PathFor("catalog.json");
        await CreateDatabaseAsync(sourcePath);

        foreach (string invalidLimit in
                 new[] { "-1", "not-a-number", long.MaxValue.ToString() })
        {
            var invalidError = new StringWriter();
            int invalidCode = await MigrationCommandRunner.RunAsync(
                [
                    "migrate", "inspect",
                    "--source", "sqlite",
                    "--input", sourcePath,
                    "--package", packagePath,
                    "--out", catalogPath,
                    "--max-source-bytes", invalidLimit,
                ],
                TextWriter.Null,
                invalidError,
                Cancellation);

            Assert.Equal(InspectorCommandRunner.ExitUsage, invalidCode);
            Assert.Contains(
                "non-negative 64-bit integer below Int64.MaxValue",
                invalidError.ToString(),
                StringComparison.Ordinal);
            Assert.False(File.Exists(packagePath));
            Assert.False(File.Exists(catalogPath));
        }

        var limitError = new StringWriter();
        int limitCode = await MigrationCommandRunner.RunAsync(
            [
                "migrate", "inspect",
                "--source", "sqlite",
                "--input", sourcePath,
                "--package", packagePath,
                "--out", catalogPath,
                "--max-source-bytes", "1",
            ],
            TextWriter.Null,
            limitError,
            Cancellation);

        Assert.Equal(InspectorCommandRunner.ExitError, limitCode);
        Assert.Contains(
            "byte limit",
            limitError.ToString(),
            StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(
            sourcePath,
            limitError.ToString(),
            StringComparison.Ordinal);
        Assert.False(File.Exists(packagePath));
        Assert.False(File.Exists(catalogPath));
        Assert.DoesNotContain(
            Directory.EnumerateFiles(workspace.Root),
            path => Path.GetFileName(path).Contains(
                ".tmp",
                StringComparison.Ordinal));
    }

    [Fact]
    public async Task Plan_RejectsDeterministicRejectModeForSqlite()
    {
        using var workspace = new TemporaryDirectory();
        string sourcePath = workspace.PathFor("source.sqlite");
        string packagePath = workspace.PathFor("retained.csdbsqlite");
        string catalogPath = workspace.PathFor("catalog.json");
        await CreateDatabaseAsync(sourcePath);
        int inspectCode = await RunInspectAsync(
            sourcePath,
            packagePath,
            catalogPath);
        Assert.True(
            inspectCode is InspectorCommandRunner.ExitOk or
                InspectorCommandRunner.ExitWarn);
        var error = new StringWriter();

        int code = await MigrationCommandRunner.RunAsync(
            [
                "migrate", "plan", catalogPath,
                "--out", workspace.PathFor("plan.json"),
                "--accept-exclusions", "all",
                "--reject-mode", "deterministic",
                "--reject-rules", "all",
                "--max-rejected-rows-per-batch", "100",
                "--max-rejected-rows-per-run", "10000",
                "--max-reject-evidence-value-bytes", "4096",
                "--max-reject-evidence-bytes-per-batch", "65536",
                "--max-reject-evidence-bytes-per-run", "1048576",
                "--max-reject-artifact-bytes", "16777216",
            ],
            TextWriter.Null,
            error,
            Cancellation);

        Assert.Equal(InspectorCommandRunner.ExitUsage, code);
        Assert.Contains(
            "Deterministic rejects are not supported",
            error.ToString(),
            StringComparison.Ordinal);
        Assert.False(File.Exists(workspace.PathFor("plan.json")));
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

    private static async ValueTask<SqliteArtifacts>
        CreateApplyReadyArtifactsAsync(
            string directory,
            long firstId = 1)
    {
        string sourcePath = Path.Combine(directory, "source.sqlite");
        string packagePath = Path.Combine(
            directory,
            "retained.csdbsqlite");
        string catalogPath = Path.Combine(directory, "catalog.json");
        string planPath = Path.Combine(directory, "plan.json");
        await CreateDatabaseAsync(sourcePath, firstId);
        var inspectOutput = new StringWriter();
        var inspectError = new StringWriter();
        int inspectCode = await MigrationCommandRunner.RunAsync(
            [
                "migrate", "inspect",
                "--source", "sqlite",
                "--input", sourcePath,
                "--package", packagePath,
                "--out", catalogPath,
            ],
            inspectOutput,
            inspectError,
            Cancellation);
        Assert.True(
            inspectCode is InspectorCommandRunner.ExitOk or
                InspectorCommandRunner.ExitWarn,
            inspectError.ToString());
        string digest = ReadManifestDigest(inspectOutput.ToString());
        var planError = new StringWriter();
        int planCode = await MigrationCommandRunner.RunAsync(
            [
                "migrate", "plan", catalogPath,
                "--out", planPath,
                "--accept-exclusions", "all",
            ],
            TextWriter.Null,
            planError,
            Cancellation);
        Assert.True(
            planCode is InspectorCommandRunner.ExitOk or
                InspectorCommandRunner.ExitWarn,
            planError.ToString());
        var previewError = new StringWriter();
        int previewCode = await MigrationCommandRunner.RunAsync(
            [
                "migrate", "preview", planPath,
                "--catalog", catalogPath,
                "--format", "json",
            ],
            TextWriter.Null,
            previewError,
            Cancellation);
        Assert.True(
            previewCode is InspectorCommandRunner.ExitOk or
                InspectorCommandRunner.ExitWarn,
            previewError.ToString());

        return new SqliteArtifacts(
            sourcePath,
            packagePath,
            catalogPath,
            planPath,
            digest);
    }

    private static string[] ApplyArguments(
        SqliteArtifacts artifacts,
        string targetPath,
        string reportPath,
        IReadOnlyList<string> suffix)
    {
        var arguments = new List<string>
        {
            "migrate", "apply", artifacts.PlanPath,
            "--catalog", artifacts.CatalogPath,
            "--target", targetPath,
            "--out", reportPath,
        };
        arguments.AddRange(suffix);
        return arguments.ToArray();
    }

    private static string DifferentDigest(string digest)
    {
        Assert.Matches("^sha256:[0-9a-f]{64}$", digest);
        char replacement = digest[7] == '0' ? '1' : '0';
        return digest[..7] + replacement + digest[8..];
    }

    private static void AssertTargetWasNotCreated(
        string targetPath,
        string reportPath)
    {
        Assert.False(File.Exists(targetPath));
        Assert.False(File.Exists(targetPath + ".wal"));
        Assert.False(File.Exists(targetPath + ".migration.lock"));
        Assert.False(File.Exists(reportPath));
    }

    private static void AssertNoSqliteWorkspaceDirectories(
        string directory) =>
        Assert.Empty(Directory.EnumerateDirectories(
            directory,
            "csharpdb-sqlite-*"));

    private static async ValueTask CreateDatabaseAsync(
        string path,
        long firstId = 1)
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
                ($first, 'one', 1),
                ($second, 'two', 2),
                ($third, 'three', 3);
            """;
        command.Parameters.AddWithValue("$first", firstId);
        command.Parameters.AddWithValue("$second", firstId + 1);
        command.Parameters.AddWithValue("$third", firstId + 2);
        await command.ExecuteNonQueryAsync(Cancellation);
    }

    [GeneratedRegex(
        @"(?:^|\|\s*)manifestDigest=(sha256:[0-9a-f]{64})(?:\s*\||\s*$)",
        RegexOptions.CultureInvariant)]
    private static partial Regex ManifestDigestPattern();

    private sealed record SqliteArtifacts(
        string SourcePath,
        string PackagePath,
        string CatalogPath,
        string PlanPath,
        string ManifestDigest);

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
