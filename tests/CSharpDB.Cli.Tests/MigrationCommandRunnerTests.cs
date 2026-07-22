using System.Text.Json;
using CSharpDB.Migration;

namespace CSharpDB.Cli.Tests;

[Collection("CliConsole")]
public sealed class MigrationCommandRunnerTests
{
    [Fact]
    public void IsKnownCommand_RecognizesOnlyMigrateCaseInsensitively()
    {
        Assert.True(MigrationCommandRunner.IsKnownCommand("migrate"));
        Assert.True(MigrationCommandRunner.IsKnownCommand("MIGRATE"));
        Assert.False(MigrationCommandRunner.IsKnownCommand("inspect"));
        Assert.False(MigrationCommandRunner.IsKnownCommand(null));
    }

    [Fact]
    public async Task RunAsync_MissingOrUnknownVerb_ReturnsUsage()
    {
        var output = new StringWriter();
        var error = new StringWriter();

        int missing = await MigrationCommandRunner.RunAsync(
            ["migrate"],
            output,
            error,
            TestContext.Current.CancellationToken);
        int unknown = await MigrationCommandRunner.RunAsync(
            ["migrate", "launch"],
            output,
            error,
            TestContext.Current.CancellationToken);

        Assert.Equal(InspectorCommandRunner.ExitUsage, missing);
        Assert.Equal(InspectorCommandRunner.ExitUsage, unknown);
        Assert.Contains("Usage: csharpdb migrate", error.ToString(), StringComparison.Ordinal);
        Assert.Contains("Unsupported migrate command", error.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task InspectPlanAndPreview_ProduceDeterministicBoundArtifacts()
    {
        string directory = NewTempDirectory();
        string catalogPath = Path.Combine(directory, "catalog.json");
        string secondCatalogPath = Path.Combine(directory, "catalog-2.json");
        string planPath = Path.Combine(directory, "plan.json");
        string secondPlanPath = Path.Combine(directory, "plan-2.json");

        try
        {
            var output = new StringWriter();
            var error = new StringWriter();
            CancellationToken ct = TestContext.Current.CancellationToken;

            int inspectCode = await MigrationCommandRunner.RunAsync(
                ["migrate", "inspect", "--source", "synthetic", "--out", catalogPath],
                output,
                error,
                ct);
            int secondInspectCode = await MigrationCommandRunner.RunAsync(
                ["migrate", "inspect", "--source", "synthetic", "--out", secondCatalogPath],
                output,
                error,
                ct);

            Assert.Equal(InspectorCommandRunner.ExitWarn, inspectCode);
            Assert.Equal(InspectorCommandRunner.ExitWarn, secondInspectCode);
            Assert.Equal(
                await File.ReadAllTextAsync(catalogPath, ct),
                await File.ReadAllTextAsync(secondCatalogPath, ct));
            MigrationCatalog catalog = MigrationArtifactSerializer.DeserializeCatalog(
                await File.ReadAllTextAsync(catalogPath, ct));
            Assert.Contains(catalog.Objects, item => item.NativeType == "GEOGRAPHY");

            int planCode = await MigrationCommandRunner.RunAsync(
                ["migrate", "plan", catalogPath, "--out", planPath],
                output,
                error,
                ct);
            int secondPlanCode = await MigrationCommandRunner.RunAsync(
                ["migrate", "plan", catalogPath, "--out", secondPlanPath],
                output,
                error,
                ct);

            Assert.Equal(InspectorCommandRunner.ExitWarn, planCode);
            Assert.Equal(InspectorCommandRunner.ExitWarn, secondPlanCode);
            Assert.Equal(
                await File.ReadAllTextAsync(planPath, ct),
                await File.ReadAllTextAsync(secondPlanPath, ct));
            MigrationPlan plan = MigrationArtifactSerializer.DeserializePlan(
                await File.ReadAllTextAsync(planPath, ct),
                catalog);
            Assert.Equal(catalog.Objects.Count, plan.Objects.Count);

            output.GetStringBuilder().Clear();
            int previewCode = await MigrationCommandRunner.RunAsync(
                ["migrate", "preview", planPath, "--catalog", catalogPath],
                output,
                error,
                ct);

            Assert.Equal(InspectorCommandRunner.ExitWarn, previewCode);
            Assert.Contains("Status: REVIEW REQUIRED", output.ToString(), StringComparison.Ordinal);
            Assert.Contains("losslessReencoded=", output.ToString(), StringComparison.Ordinal);
            Assert.Contains("[excluded]", output.ToString(), StringComparison.Ordinal);
            Assert.Contains("MIG-TYPE-UNSUPPORTED-001", output.ToString(), StringComparison.Ordinal);
            Assert.True(string.IsNullOrWhiteSpace(error.ToString()));
        }
        finally
        {
            DeleteDirectoryIfExists(directory);
        }
    }

    [Fact]
    public async Task QueryableJsonPreview_IsPureJsonAndIncludesPendingApprovals()
    {
        string directory = NewTempDirectory();
        string catalogPath = Path.Combine(directory, "catalog.json");
        string planPath = Path.Combine(directory, "queryable-plan.json");

        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;
            await MigrationCommandRunner.RunAsync(
                ["migrate", "inspect", "--source", "synthetic", "--out", catalogPath],
                TextWriter.Null,
                TextWriter.Null,
                ct);
            await MigrationCommandRunner.RunAsync(
                ["migrate", "plan", catalogPath, "--out", planPath, "--profile", "queryable"],
                TextWriter.Null,
                TextWriter.Null,
                ct);

            var output = new StringWriter();
            var error = new StringWriter();
            int code = await MigrationCommandRunner.RunAsync(
                ["migrate", "preview", planPath, "--catalog", catalogPath, "--format", "json"],
                output,
                error,
                ct);

            Assert.Equal(InspectorCommandRunner.ExitWarn, code);
            using JsonDocument preview = JsonDocument.Parse(output.ToString());
            Assert.Equal(
                "csharpdb-migration-preview/v1",
                preview.RootElement.GetProperty("format").GetString());
            Assert.Equal("review-required", preview.RootElement.GetProperty("status").GetString());
            Assert.Equal(
                2,
                preview.RootElement.GetProperty("pendingDiagnosticIds").GetArrayLength());
            Assert.True(string.IsNullOrWhiteSpace(error.ToString()));
        }
        finally
        {
            DeleteDirectoryIfExists(directory);
        }
    }

    [Fact]
    public async Task Plan_AcceptAllExclusionsProducesApplyReadyBoundPlan()
    {
        string directory = NewTempDirectory();
        string catalogPath = Path.Combine(directory, "catalog.json");
        string planPath = Path.Combine(directory, "ready-plan.json");

        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;
            await MigrationCommandRunner.RunAsync(
                ["migrate", "inspect", "--source", "synthetic", "--out", catalogPath],
                TextWriter.Null,
                TextWriter.Null,
                ct);
            int code = await MigrationCommandRunner.RunAsync(
                [
                    "migrate", "plan", catalogPath,
                    "--out", planPath,
                    "--accept-exclusions", "all",
                ],
                TextWriter.Null,
                TextWriter.Null,
                ct);

            MigrationCatalog catalog = MigrationArtifactSerializer.DeserializeCatalog(
                await File.ReadAllTextAsync(catalogPath, ct));
            MigrationPlan plan = MigrationArtifactSerializer.DeserializePlan(
                await File.ReadAllTextAsync(planPath, ct),
                catalog);

            Assert.Equal(InspectorCommandRunner.ExitWarn, code);
            Assert.NotEmpty(plan.AcceptedExclusionObjectIds);
            Assert.Equal(
                MigrationPlanReadinessStatus.Ready,
                MigrationPlanReadinessValidator.Evaluate(plan, catalog).Status);
        }
        finally
        {
            DeleteDirectoryIfExists(directory);
        }
    }

    [Fact]
    public async Task Apply_ResumeIsAValuelessFlag()
    {
        var error = new StringWriter();

        int code = await MigrationCommandRunner.RunAsync(
            ["migrate", "apply", "plan.json", "--resume"],
            TextWriter.Null,
            error,
            TestContext.Current.CancellationToken);

        Assert.Equal(InspectorCommandRunner.ExitUsage, code);
        Assert.Contains("Missing required option --catalog", error.ToString(), StringComparison.Ordinal);
        Assert.DoesNotContain("Missing value for --resume", error.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task ApplyAndResume_ProducePureJsonReportsWithStableTargetIdentityAndNoReplayedRows()
    {
        string directory = NewTempDirectory();
        string targetPath = Path.Combine(directory, "staged.csdb");
        string runPath = Path.Combine(directory, "run.json");
        string resumeRunPath = Path.Combine(directory, "run-resume.json");

        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;
            (string catalogPath, string planPath) = await CreateApplyReadyArtifactsAsync(directory, ct);
            var applyOutput = new StringWriter();
            var applyError = new StringWriter();

            int applyCode = await MigrationCommandRunner.RunAsync(
                [
                    "migrate", "apply", planPath,
                    "--catalog", catalogPath,
                    "--target", targetPath,
                    "--out", runPath,
                    "--format", "json",
                ],
                applyOutput,
                applyError,
                ct);

            Assert.Equal(InspectorCommandRunner.ExitWarn, applyCode);
            Assert.True(string.IsNullOrWhiteSpace(applyError.ToString()));
            Assert.True(File.Exists(targetPath));
            Assert.True(File.Exists(runPath));
            Assert.False(File.Exists(targetPath + ".migration.lock"));

            using JsonDocument applyStdout = JsonDocument.Parse(applyOutput.ToString());
            using JsonDocument applyReport = JsonDocument.Parse(await File.ReadAllTextAsync(runPath, ct));
            JsonElement first = applyStdout.RootElement;
            Assert.Equal("csharpdb-migration-run/v1", first.GetProperty("format").GetString());
            Assert.Equal("awaitingValidation", first.GetProperty("status").GetString());
            Assert.Equal(first.GetRawText(), applyReport.RootElement.GetRawText());
            string targetIdentity = Assert.IsType<string>(first.GetProperty("targetIdentity").GetString());
            long batchesWritten = first.GetProperty("batchesWritten").GetInt64();
            long rowsWritten = first.GetProperty("rowsWritten").GetInt64();
            Assert.False(string.IsNullOrWhiteSpace(targetIdentity));
            Assert.True(batchesWritten > 0);
            Assert.True(rowsWritten > 0);
            Assert.Equal(0, first.GetProperty("batchesSkipped").GetInt64());
            Assert.Equal(0, first.GetProperty("rowsSkipped").GetInt64());
            Assert.Equal(0, first.GetProperty("rejectedRows").GetInt64());
            Assert.True(first.GetProperty("excludedObjects").GetInt32() > 0);

            var resumeOutput = new StringWriter();
            var resumeError = new StringWriter();
            int resumeCode = await MigrationCommandRunner.RunAsync(
                [
                    "migrate", "apply", planPath,
                    "--catalog", catalogPath,
                    "--target", targetPath,
                    "--out", resumeRunPath,
                    "--resume",
                    "--format", "json",
                ],
                resumeOutput,
                resumeError,
                ct);

            Assert.Equal(InspectorCommandRunner.ExitWarn, resumeCode);
            Assert.True(string.IsNullOrWhiteSpace(resumeError.ToString()));
            Assert.True(File.Exists(resumeRunPath));
            Assert.False(File.Exists(targetPath + ".migration.lock"));
            using JsonDocument resumeStdout = JsonDocument.Parse(resumeOutput.ToString());
            using JsonDocument resumeReport = JsonDocument.Parse(
                await File.ReadAllTextAsync(resumeRunPath, ct));
            JsonElement resumed = resumeStdout.RootElement;
            Assert.Equal(resumed.GetRawText(), resumeReport.RootElement.GetRawText());
            Assert.Equal(targetIdentity, resumed.GetProperty("targetIdentity").GetString());
            Assert.Equal(0, resumed.GetProperty("batchesWritten").GetInt64());
            Assert.Equal(batchesWritten, resumed.GetProperty("batchesSkipped").GetInt64());
            Assert.Equal(0, resumed.GetProperty("rowsWritten").GetInt64());
            Assert.Equal(rowsWritten, resumed.GetProperty("rowsSkipped").GetInt64());
        }
        finally
        {
            DeleteDirectoryIfExists(directory);
        }
    }

    [Fact]
    public async Task Apply_ExistingTargetIsRefusedWithoutChangingItAndWritesSafeFailureReport()
    {
        string directory = NewTempDirectory();
        string targetPath = Path.Combine(directory, "existing.csdb");
        string runPath = Path.Combine(directory, "run.json");
        byte[] originalTarget = [0x43, 0x53, 0x44, 0x42];

        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;
            (string catalogPath, string planPath) = await CreateApplyReadyArtifactsAsync(directory, ct);
            await File.WriteAllBytesAsync(targetPath, originalTarget, ct);
            var output = new StringWriter();
            var error = new StringWriter();

            int code = await MigrationCommandRunner.RunAsync(
                [
                    "migrate", "apply", planPath,
                    "--catalog", catalogPath,
                    "--target", targetPath,
                    "--out", runPath,
                ],
                output,
                error,
                ct);

            Assert.Equal(InspectorCommandRunner.ExitError, code);
            Assert.True(string.IsNullOrWhiteSpace(output.ToString()));
            Assert.Contains("already exists", error.ToString(), StringComparison.OrdinalIgnoreCase);
            Assert.Equal(originalTarget, await File.ReadAllBytesAsync(targetPath, ct));
            using JsonDocument report = JsonDocument.Parse(await File.ReadAllTextAsync(runPath, ct));
            AssertSafeTargetIoFailureReport(report.RootElement);
            Assert.False(File.Exists(targetPath + ".migration.lock"));
            Assert.Empty(Directory.EnumerateFiles(directory, ".csharpdb-migration-*.tmp"));
        }
        finally
        {
            DeleteDirectoryIfExists(directory);
        }
    }

    [Fact]
    public async Task Apply_OrphanWalIsRefusedWithoutCreatingTargetAndWritesSafeFailureReport()
    {
        string directory = NewTempDirectory();
        string targetPath = Path.Combine(directory, "staged.csdb");
        string walPath = targetPath + ".wal";
        string runPath = Path.Combine(directory, "run.json");
        byte[] originalWal = [0x57, 0x41, 0x4c];

        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;
            (string catalogPath, string planPath) = await CreateApplyReadyArtifactsAsync(directory, ct);
            await File.WriteAllBytesAsync(walPath, originalWal, ct);
            var output = new StringWriter();
            var error = new StringWriter();

            int code = await MigrationCommandRunner.RunAsync(
                [
                    "migrate", "apply", planPath,
                    "--catalog", catalogPath,
                    "--target", targetPath,
                    "--out", runPath,
                ],
                output,
                error,
                ct);

            Assert.Equal(InspectorCommandRunner.ExitError, code);
            Assert.True(string.IsNullOrWhiteSpace(output.ToString()));
            Assert.Contains("companion WAL", error.ToString(), StringComparison.OrdinalIgnoreCase);
            Assert.False(File.Exists(targetPath));
            Assert.Equal(originalWal, await File.ReadAllBytesAsync(walPath, ct));
            using JsonDocument report = JsonDocument.Parse(await File.ReadAllTextAsync(runPath, ct));
            AssertSafeTargetIoFailureReport(report.RootElement);
            Assert.False(File.Exists(targetPath + ".migration.lock"));
            Assert.Empty(Directory.EnumerateFiles(directory, ".csharpdb-migration-*.tmp"));
        }
        finally
        {
            DeleteDirectoryIfExists(directory);
        }
    }

    [Fact]
    public async Task Apply_MissingResumeTargetReturnsOperationalErrorWithSafeFailureReport()
    {
        string directory = NewTempDirectory();
        string targetPath = Path.Combine(directory, "missing.csdb");
        string runPath = Path.Combine(directory, "run.json");

        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;
            (string catalogPath, string planPath) = await CreateApplyReadyArtifactsAsync(directory, ct);
            var output = new StringWriter();
            var error = new StringWriter();

            int code = await MigrationCommandRunner.RunAsync(
                [
                    "migrate", "apply", planPath,
                    "--catalog", catalogPath,
                    "--target", targetPath,
                    "--out", runPath,
                    "--resume",
                ],
                output,
                error,
                ct);

            Assert.Equal(InspectorCommandRunner.ExitError, code);
            Assert.True(string.IsNullOrWhiteSpace(output.ToString()));
            Assert.Contains("does not exist for resume", error.ToString(), StringComparison.OrdinalIgnoreCase);
            Assert.False(File.Exists(targetPath));
            using JsonDocument report = JsonDocument.Parse(await File.ReadAllTextAsync(runPath, ct));
            AssertSafeTargetIoFailureReport(report.RootElement);
            Assert.False(File.Exists(targetPath + ".migration.lock"));
            Assert.Empty(Directory.EnumerateFiles(directory, ".csharpdb-migration-*.tmp"));
        }
        finally
        {
            DeleteDirectoryIfExists(directory);
        }
    }

    [Fact]
    public async Task Apply_PathCollisionsReturnUsageWithoutChangingInputsOrCreatingTarget()
    {
        string directory = NewTempDirectory();
        string targetPath = Path.Combine(directory, "staged.csdb");

        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;
            (string catalogPath, string planPath) = await CreateApplyReadyArtifactsAsync(directory, ct);
            string originalCatalog = await File.ReadAllTextAsync(catalogPath, ct);
            string originalPlan = await File.ReadAllTextAsync(planPath, ct);
            (string Target, string Report)[] collisions =
            [
                (planPath, Path.Combine(directory, "run-target-plan.json")),
                (catalogPath, Path.Combine(directory, "run-target-catalog.json")),
                (targetPath, planPath),
                (targetPath, catalogPath),
                (targetPath, targetPath),
                (targetPath, targetPath + ".wal"),
                (targetPath, targetPath + ".migration.lock"),
            ];

            foreach ((string target, string report) in collisions)
            {
                var output = new StringWriter();
                var error = new StringWriter();
                int code = await MigrationCommandRunner.RunAsync(
                    [
                        "migrate", "apply", planPath,
                        "--catalog", catalogPath,
                        "--target", target,
                        "--out", report,
                    ],
                    output,
                    error,
                    ct);

                Assert.Equal(InspectorCommandRunner.ExitUsage, code);
                Assert.True(string.IsNullOrWhiteSpace(output.ToString()));
                Assert.Contains("must use different files", error.ToString(), StringComparison.OrdinalIgnoreCase);
            }

            Assert.Equal(originalCatalog, await File.ReadAllTextAsync(catalogPath, ct));
            Assert.Equal(originalPlan, await File.ReadAllTextAsync(planPath, ct));
            Assert.False(File.Exists(targetPath));
            Assert.False(File.Exists(targetPath + ".wal"));
            Assert.False(File.Exists(targetPath + ".migration.lock"));
        }
        finally
        {
            DeleteDirectoryIfExists(directory);
        }
    }

    [Fact]
    public async Task Apply_InputArtifactsCannotCollideWithTargetCompanionFiles()
    {
        string directory = NewTempDirectory();

        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;
            (string catalogPath, string planPath) = await CreateApplyReadyArtifactsAsync(directory, ct);
            string originalCatalog = await File.ReadAllTextAsync(catalogPath, ct);
            string originalPlan = await File.ReadAllTextAsync(planPath, ct);
            (string Plan, string Catalog, string Target, string InputPath, string InputContent)[] collisions =
            [
                CreateCompanionCollision("plan-lock", ".migration.lock", originalPlan, planInput: true),
                CreateCompanionCollision("plan-wal", ".wal", originalPlan, planInput: true),
                CreateCompanionCollision("catalog-lock", ".migration.lock", originalCatalog, planInput: false),
                CreateCompanionCollision("catalog-wal", ".wal", originalCatalog, planInput: false),
            ];

            foreach ((string plan, string catalog, string target, string inputPath, string inputContent) in collisions)
            {
                await File.WriteAllTextAsync(inputPath, inputContent, ct);
                var output = new StringWriter();
                var error = new StringWriter();
                int code = await MigrationCommandRunner.RunAsync(
                    [
                        "migrate", "apply", plan,
                        "--catalog", catalog,
                        "--target", target,
                        "--out", target + ".run.json",
                    ],
                    output,
                    error,
                    ct);

                Assert.Equal(InspectorCommandRunner.ExitUsage, code);
                Assert.True(string.IsNullOrWhiteSpace(output.ToString()));
                Assert.Contains("must use different files", error.ToString(), StringComparison.OrdinalIgnoreCase);
                Assert.Equal(inputContent, await File.ReadAllTextAsync(inputPath, ct));
                Assert.False(File.Exists(target));
                Assert.False(File.Exists(target + ".run.json"));
            }

            Assert.Equal(originalCatalog, await File.ReadAllTextAsync(catalogPath, ct));
            Assert.Equal(originalPlan, await File.ReadAllTextAsync(planPath, ct));

            (string Plan, string Catalog, string Target, string InputPath, string InputContent) CreateCompanionCollision(
                string name,
                string suffix,
                string content,
                bool planInput)
            {
                string target = Path.Combine(directory, name + ".csdb");
                string input = target + suffix;
                return planInput
                    ? (input, catalogPath, target, input, content)
                    : (planPath, input, target, input, content);
            }
        }
        finally
        {
            DeleteDirectoryIfExists(directory);
        }
    }

    [Fact]
    public async Task Plan_SameInputAndOutputPath_IsRejectedWithoutChangingCatalog()
    {
        string directory = NewTempDirectory();
        string catalogPath = Path.Combine(directory, "catalog.json");

        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;
            await MigrationCommandRunner.RunAsync(
                ["migrate", "inspect", "--source", "synthetic", "--out", catalogPath],
                TextWriter.Null,
                TextWriter.Null,
                ct);
            string originalCatalog = await File.ReadAllTextAsync(catalogPath, ct);
            var error = new StringWriter();

            int code = await MigrationCommandRunner.RunAsync(
                ["migrate", "plan", catalogPath, "--out", Path.Combine(directory, ".", "catalog.json")],
                TextWriter.Null,
                error,
                ct);

            Assert.Equal(InspectorCommandRunner.ExitUsage, code);
            Assert.Contains("must be different files", error.ToString(), StringComparison.OrdinalIgnoreCase);
            Assert.Equal(originalCatalog, await File.ReadAllTextAsync(catalogPath, ct));
            _ = MigrationArtifactSerializer.DeserializeCatalog(originalCatalog);
            Assert.Single(Directory.EnumerateFiles(directory));
        }
        finally
        {
            DeleteDirectoryIfExists(directory);
        }
    }

    [Fact]
    public async Task Plan_DistinctOutputPath_WritesValidArtifactWithoutTemporaryFiles()
    {
        string directory = NewTempDirectory();
        string catalogPath = Path.Combine(directory, "catalog.json");
        string planPath = Path.Combine(directory, "plan.json");

        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;
            await MigrationCommandRunner.RunAsync(
                ["migrate", "inspect", "--source", "synthetic", "--out", catalogPath],
                TextWriter.Null,
                TextWriter.Null,
                ct);
            MigrationCatalog catalog = MigrationArtifactSerializer.DeserializeCatalog(
                await File.ReadAllTextAsync(catalogPath, ct));
            await File.WriteAllTextAsync(planPath, "stale plan content", ct);

            int code = await MigrationCommandRunner.RunAsync(
                ["migrate", "plan", catalogPath, "--out", planPath],
                TextWriter.Null,
                TextWriter.Null,
                ct);

            Assert.Equal(InspectorCommandRunner.ExitWarn, code);
            MigrationPlan plan = MigrationArtifactSerializer.DeserializePlan(
                await File.ReadAllTextAsync(planPath, ct),
                catalog);
            Assert.Equal(catalog.Objects.Count, plan.Objects.Count);
            Assert.Equal(
                new[] { Path.GetFileName(catalogPath), Path.GetFileName(planPath) },
                Directory.EnumerateFiles(directory)
                    .Select(Path.GetFileName)
                    .Order(StringComparer.Ordinal)
                    .ToArray());
        }
        finally
        {
            DeleteDirectoryIfExists(directory);
        }
    }

    [Fact]
    public async Task Preview_WithMismatchedCatalog_ReturnsOperationalError()
    {
        string directory = NewTempDirectory();
        string catalogPath = Path.Combine(directory, "catalog.json");
        string differentCatalogPath = Path.Combine(directory, "catalog-different.json");
        string planPath = Path.Combine(directory, "plan.json");

        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;
            await MigrationCommandRunner.RunAsync(
                ["migrate", "inspect", "--source", "synthetic", "--out", catalogPath],
                TextWriter.Null,
                TextWriter.Null,
                ct);
            await MigrationCommandRunner.RunAsync(
                ["migrate", "plan", catalogPath, "--out", planPath],
                TextWriter.Null,
                TextWriter.Null,
                ct);

            MigrationCatalog catalog = MigrationArtifactSerializer.DeserializeCatalog(
                await File.ReadAllTextAsync(catalogPath, ct));
            MigrationCatalog different = catalog with
            {
                Source = catalog.Source with
                {
                    Fingerprint = "sha256:" + new string('a', 64),
                },
            };
            await File.WriteAllTextAsync(
                differentCatalogPath,
                MigrationArtifactSerializer.SerializeCatalog(different),
                ct);

            var error = new StringWriter();
            int code = await MigrationCommandRunner.RunAsync(
                ["migrate", "preview", planPath, "--catalog", differentCatalogPath],
                TextWriter.Null,
                error,
                ct);

            Assert.Equal(InspectorCommandRunner.ExitError, code);
            Assert.Contains("catalog digest", error.ToString(), StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            DeleteDirectoryIfExists(directory);
        }
    }

    [Fact]
    public async Task InvalidAndDuplicateOptions_ReturnUsage()
    {
        var error = new StringWriter();
        CancellationToken ct = TestContext.Current.CancellationToken;

        int missing = await MigrationCommandRunner.RunAsync(
            ["migrate", "inspect", "--source", "synthetic"],
            TextWriter.Null,
            error,
            ct);
        int duplicate = await MigrationCommandRunner.RunAsync(
            ["migrate", "inspect", "--source", "synthetic", "--source", "synthetic", "--out", "unused.json"],
            TextWriter.Null,
            error,
            ct);
        int unknown = await MigrationCommandRunner.RunAsync(
            ["migrate", "inspect", "--source", "synthetic", "--out", "unused.json", "--mystery", "x"],
            TextWriter.Null,
            error,
            ct);

        Assert.Equal(InspectorCommandRunner.ExitUsage, missing);
        Assert.Equal(InspectorCommandRunner.ExitUsage, duplicate);
        Assert.Equal(InspectorCommandRunner.ExitUsage, unknown);
        Assert.Contains("Missing required option --out", error.ToString(), StringComparison.Ordinal);
        Assert.Contains("Duplicate option", error.ToString(), StringComparison.Ordinal);
        Assert.Contains("Unknown option", error.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task Cancellation_IsPropagated()
    {
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await MigrationCommandRunner.RunAsync(
                ["migrate", "inspect", "--source", "synthetic", "--out",
                    Path.Combine(Path.GetTempPath(), $"cancelled_migration_{Guid.NewGuid():N}.json")],
                TextWriter.Null,
                TextWriter.Null,
                cancellation.Token));
    }

    private static string NewTempDirectory()
    {
        string path = Path.Combine(Path.GetTempPath(), $"csharpdb_migration_cli_{Guid.NewGuid():N}");
        Directory.CreateDirectory(path);
        return path;
    }

    private static async Task<(string CatalogPath, string PlanPath)> CreateApplyReadyArtifactsAsync(
        string directory,
        CancellationToken ct)
    {
        string catalogPath = Path.Combine(directory, "catalog.json");
        string planPath = Path.Combine(directory, "plan.json");
        int inspectCode = await MigrationCommandRunner.RunAsync(
            ["migrate", "inspect", "--source", "synthetic", "--out", catalogPath],
            TextWriter.Null,
            TextWriter.Null,
            ct);
        int planCode = await MigrationCommandRunner.RunAsync(
            [
                "migrate", "plan", catalogPath,
                "--out", planPath,
                "--accept-exclusions", "all",
            ],
            TextWriter.Null,
            TextWriter.Null,
            ct);

        Assert.Equal(InspectorCommandRunner.ExitWarn, inspectCode);
        Assert.Equal(InspectorCommandRunner.ExitWarn, planCode);
        return (catalogPath, planPath);
    }

    private static void AssertSafeTargetIoFailureReport(JsonElement report)
    {
        Assert.Equal("csharpdb-migration-run/v1", report.GetProperty("format").GetString());
        Assert.Equal("failed", report.GetProperty("status").GetString());
        Assert.Equal("MIG-APPLY-TARGET-IO-001", report.GetProperty("errorCode").GetString());
        Assert.False(report.TryGetProperty("targetPath", out _));
        Assert.False(report.TryGetProperty("message", out _));
        Assert.False(report.TryGetProperty("error", out _));
        Assert.False(report.TryGetProperty("resumeCursor", out _));
    }

    private static void DeleteDirectoryIfExists(string path)
    {
        if (Directory.Exists(path))
            Directory.Delete(path, recursive: true);
    }
}
