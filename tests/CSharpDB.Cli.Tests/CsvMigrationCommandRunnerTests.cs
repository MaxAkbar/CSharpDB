using System.Text;
using System.Text.Json;
using CSharpDB.Engine;
using CSharpDB.Migration;
using CSharpDB.Migration.Files.Csv;
using CSharpDB.Migration.Validation;

namespace CSharpDB.Cli.Tests;

[Collection("CliConsole")]
public sealed class CsvMigrationCommandRunnerTests
{
    private const string CsvContents =
        "id,name\n" +
        "1,alpha\n" +
        "2,\"bravo\nincorporated\"\n" +
        "3,charlie\n";

    private const string MixedRejectCsvContents =
        "id,name\n" +
        "1,alpha\n" +
        "2,bravo\n" +
        "bad,broken\n" +
        "3,charlie\n";

    private const string DeterministicRejectRules =
        CsvMigrationDataRules.MissingField + "," +
        CsvMigrationDataRules.NullNotAllowed + "," +
        CsvMigrationDataRules.TypeMismatch;

    private static CancellationToken Cancellation => TestContext.Current.CancellationToken;

    [Fact]
    public async Task InspectCsv_WritesBoundCatalogAndPackageAndReportsManifestDigest()
    {
        using var workspace = new TemporaryDirectory();
        string sourcePath = Path.Combine(workspace.Root, "orders.csv");
        string packagePath = Path.Combine(workspace.Root, "orders.csdbcsv");
        string catalogPath = Path.Combine(workspace.Root, "catalog.json");
        await WriteCsvAsync(sourcePath);
        byte[] originalSource = await File.ReadAllBytesAsync(sourcePath, Cancellation);
        var output = new StringWriter();
        var error = new StringWriter();

        int code = await MigrationCommandRunner.RunAsync(
            [
                "migrate", "inspect",
                "--source", "csv",
                "--input", sourcePath,
                "--package", packagePath,
                "--out", catalogPath,
                "--workspace", workspace.Root,
            ],
            output,
            error,
            Cancellation);

        Assert.Equal(InspectorCommandRunner.ExitOk, code);
        Assert.True(string.IsNullOrWhiteSpace(error.ToString()));
        Assert.Contains("Status: OK", output.ToString(), StringComparison.Ordinal);
        Assert.Contains($"catalog={Path.GetFullPath(catalogPath)}", output.ToString(), StringComparison.Ordinal);
        Assert.Contains($"package={Path.GetFullPath(packagePath)}", output.ToString(), StringComparison.Ordinal);
        string manifestDigest = ReadStatusField(output.ToString(), "manifestDigest");
        AssertCanonicalDigest(manifestDigest);
        Assert.True(File.Exists(packagePath));
        Assert.True(File.Exists(catalogPath));
        Assert.Equal(originalSource, await File.ReadAllBytesAsync(sourcePath, Cancellation));

        MigrationCatalog catalog = MigrationArtifactSerializer.DeserializeCatalog(
            await File.ReadAllTextAsync(catalogPath, Cancellation));
        Assert.Equal(MigrationSourceKind.Csv, catalog.Source.Kind);
        await using (CsvSnapshotPackageSession session = await CsvSnapshotPackage.OpenAsync(
                         packagePath,
                         new CsvSnapshotPackageOpenOptions
                         {
                             WorkspacePath = workspace.Root,
                             MaxSourceBytes = 1024 * 1024,
                             ExpectedManifestDigest = manifestDigest,
                         },
                         Cancellation))
        {
            Assert.Equal(manifestDigest, session.Manifest.ManifestDigest);
            Assert.Equal(
                MigrationArtifactSerializer.ComputeCatalogDigest(catalog),
                session.Manifest.CatalogDigest);
            Assert.Equal(
                MigrationArtifactSerializer.SerializeCatalog(catalog, writeIndented: false),
                MigrationArtifactSerializer.SerializeCatalog(session.Catalog, writeIndented: false));
        }

        AssertNoCsvWorkspaceDirectories(workspace.Root);
    }

    [Fact]
    public async Task InspectCsv_PathCollisionsReturnUsageWithoutChangingTheInput()
    {
        using var workspace = new TemporaryDirectory();
        string sourcePath = Path.Combine(workspace.Root, "orders.csv");
        string packagePath = Path.Combine(workspace.Root, "orders.csdbcsv");
        string catalogPath = Path.Combine(workspace.Root, "catalog.json");
        await WriteCsvAsync(sourcePath);
        byte[] originalSource = await File.ReadAllBytesAsync(sourcePath, Cancellation);
        (string Package, string Catalog)[] collisions =
        [
            (sourcePath, catalogPath),
            (packagePath, sourcePath),
            (packagePath, packagePath),
        ];

        foreach ((string package, string catalog) in collisions)
        {
            var output = new StringWriter();
            var error = new StringWriter();
            int code = await MigrationCommandRunner.RunAsync(
                [
                    "migrate", "inspect",
                    "--source", "csv",
                    "--input", sourcePath,
                    "--package", package,
                    "--out", catalog,
                ],
                output,
                error,
                Cancellation);

            Assert.Equal(InspectorCommandRunner.ExitUsage, code);
            Assert.True(string.IsNullOrWhiteSpace(output.ToString()));
            Assert.Contains("must use different files", error.ToString(), StringComparison.OrdinalIgnoreCase);
            Assert.Equal(originalSource, await File.ReadAllBytesAsync(sourcePath, Cancellation));
        }

        Assert.False(File.Exists(packagePath));
        Assert.False(File.Exists(catalogPath));
        Assert.Empty(Directory.EnumerateFiles(workspace.Root, ".csdbcsv-*.tmp"));
        Assert.Empty(Directory.EnumerateFiles(workspace.Root, ".csharpdb-migration-*.tmp"));
    }

    [Fact]
    public async Task InspectCsv_AutoDelimiterInsufficientDataIsActionableAndPublishesNothing()
    {
        using var workspace = new TemporaryDirectory();
        string sourcePath = Path.Combine(workspace.Root, "identifiers.csv");
        string packagePath = Path.Combine(workspace.Root, "identifiers.csdbcsv");
        string catalogPath = Path.Combine(workspace.Root, "catalog.json");
        const string insufficientCsv = "identifier\n001\n002\n";
        await WriteCsvAsync(sourcePath, insufficientCsv);
        byte[] originalSource = await File.ReadAllBytesAsync(sourcePath, Cancellation);
        var output = new StringWriter();
        var error = new StringWriter();

        int code = await MigrationCommandRunner.RunAsync(
            [
                "migrate", "inspect",
                "--source", "csv",
                "--input", sourcePath,
                "--package", packagePath,
                "--out", catalogPath,
                "--workspace", workspace.Root,
            ],
            output,
            error,
            Cancellation);

        Assert.Equal(InspectorCommandRunner.ExitError, code);
        Assert.True(string.IsNullOrWhiteSpace(output.ToString()));
        Assert.Contains("InsufficientData", error.ToString(), StringComparison.Ordinal);
        Assert.Contains("--delimiter", error.ToString(), StringComparison.Ordinal);
        Assert.Equal(originalSource, await File.ReadAllBytesAsync(sourcePath, Cancellation));
        Assert.False(File.Exists(packagePath));
        Assert.False(File.Exists(catalogPath));
        Assert.Empty(Directory.EnumerateFiles(workspace.Root, ".csdbcsv-*.tmp"));
        Assert.Empty(Directory.EnumerateFiles(workspace.Root, ".csharpdb-migration-*.tmp"));
        AssertNoCsvWorkspaceDirectories(workspace.Root);
    }

    [Fact]
    public async Task InspectCsv_CaseOnlyCatalogAliasIsRejectedOnCaseInsensitivePlatforms()
    {
        if (!OperatingSystem.IsWindows() && !OperatingSystem.IsMacOS())
            return;

        using var workspace = new TemporaryDirectory();
        string sourcePath = Path.Combine(workspace.Root, "Orders.csv");
        string caseAliasPath = Path.Combine(workspace.Root, "orders.csv");
        string packagePath = Path.Combine(workspace.Root, "orders.csdbcsv");
        await WriteCsvAsync(sourcePath);
        byte[] originalSource = await File.ReadAllBytesAsync(sourcePath, Cancellation);
        var output = new StringWriter();
        var error = new StringWriter();

        int code = await MigrationCommandRunner.RunAsync(
            [
                "migrate", "inspect",
                "--source", "csv",
                "--input", sourcePath,
                "--package", packagePath,
                "--out", caseAliasPath,
            ],
            output,
            error,
            Cancellation);

        Assert.Equal(InspectorCommandRunner.ExitUsage, code);
        Assert.True(string.IsNullOrWhiteSpace(output.ToString()));
        Assert.Contains("must use different files", error.ToString(), StringComparison.OrdinalIgnoreCase);
        Assert.Equal(originalSource, await File.ReadAllBytesAsync(sourcePath, Cancellation));
        Assert.False(File.Exists(packagePath));
        Assert.Empty(Directory.EnumerateFiles(workspace.Root, ".csdbcsv-*.tmp"));
        Assert.Empty(Directory.EnumerateFiles(workspace.Root, ".csharpdb-migration-*.tmp"));
        AssertNoCsvWorkspaceDirectories(workspace.Root);
    }

    [Fact]
    public async Task CsvCommands_RejectAliasParentsBeforeOverwritingSourceOrPackage()
    {
        using var workspace = new TemporaryDirectory();
        string realDirectory = Path.Combine(workspace.Root, "real");
        string aliasDirectory = Path.Combine(workspace.Root, "alias");
        Directory.CreateDirectory(realDirectory);
        if (!TryCreateDirectorySymbolicLink(aliasDirectory, realDirectory))
            return;

        string sourcePath = Path.Combine(realDirectory, "orders.csv");
        string aliasedSourcePath = Path.Combine(aliasDirectory, "orders.csv");
        string collisionPackagePath = Path.Combine(realDirectory, "collision.csdbcsv");
        await WriteCsvAsync(sourcePath);
        byte[] originalSource = await File.ReadAllBytesAsync(sourcePath, Cancellation);
        var inspectOutput = new StringWriter();
        var inspectError = new StringWriter();

        int inspectCode = await MigrationCommandRunner.RunAsync(
            [
                "migrate", "inspect",
                "--source", "csv",
                "--input", sourcePath,
                "--package", collisionPackagePath,
                "--out", aliasedSourcePath,
            ],
            inspectOutput,
            inspectError,
            Cancellation);

        Assert.Equal(InspectorCommandRunner.ExitUsage, inspectCode);
        Assert.True(string.IsNullOrWhiteSpace(inspectOutput.ToString()));
        Assert.Contains("must use different files", inspectError.ToString(), StringComparison.OrdinalIgnoreCase);
        Assert.Equal(originalSource, await File.ReadAllBytesAsync(sourcePath, Cancellation));
        Assert.False(File.Exists(collisionPackagePath));

        CsvArtifacts artifacts = await CreateCsvApplyReadyArtifactsAsync(realDirectory);
        byte[] originalPackage = await File.ReadAllBytesAsync(artifacts.PackagePath, Cancellation);
        string targetPath = Path.Combine(realDirectory, "staged.csdb");
        string aliasedPackageReportPath = Path.Combine(aliasDirectory, "orders.csdbcsv");
        var applyOutput = new StringWriter();
        var applyError = new StringWriter();
        int applyCode = await MigrationCommandRunner.RunAsync(
            ApplyArguments(
                artifacts,
                targetPath,
                aliasedPackageReportPath,
                [
                    "--source-package", artifacts.PackagePath,
                    "--expected-manifest-digest", artifacts.ManifestDigest,
                    "--workspace", realDirectory,
                ]),
            applyOutput,
            applyError,
            Cancellation);

        Assert.Equal(InspectorCommandRunner.ExitUsage, applyCode);
        Assert.True(string.IsNullOrWhiteSpace(applyOutput.ToString()));
        Assert.Contains("must use different files", applyError.ToString(), StringComparison.OrdinalIgnoreCase);
        Assert.Equal(originalPackage, await File.ReadAllBytesAsync(artifacts.PackagePath, Cancellation));
        Assert.Equal(originalSource, await File.ReadAllBytesAsync(sourcePath, Cancellation));
        Assert.False(File.Exists(targetPath));
        Assert.False(File.Exists(targetPath + ".wal"));
        Assert.False(File.Exists(targetPath + ".migration.lock"));
        AssertNoCsvWorkspaceDirectories(realDirectory);
    }

    [Fact]
    public async Task InspectCsv_ExistingPackageIsRefusedWithoutOverwriteOrCatalogPublication()
    {
        using var workspace = new TemporaryDirectory();
        string sourcePath = Path.Combine(workspace.Root, "orders.csv");
        string packagePath = Path.Combine(workspace.Root, "orders.csdbcsv");
        string catalogPath = Path.Combine(workspace.Root, "catalog.json");
        byte[] originalPackage = [0x43, 0x53, 0x44, 0x42, 0x43, 0x53, 0x56];
        await WriteCsvAsync(sourcePath);
        byte[] originalSource = await File.ReadAllBytesAsync(sourcePath, Cancellation);
        await File.WriteAllBytesAsync(packagePath, originalPackage, Cancellation);
        var output = new StringWriter();
        var error = new StringWriter();

        int code = await MigrationCommandRunner.RunAsync(
            [
                "migrate", "inspect",
                "--source", "csv",
                "--input", sourcePath,
                "--package", packagePath,
                "--out", catalogPath,
                "--workspace", workspace.Root,
            ],
            output,
            error,
            Cancellation);

        Assert.Equal(InspectorCommandRunner.ExitError, code);
        Assert.True(string.IsNullOrWhiteSpace(output.ToString()));
        Assert.Contains("exists", error.ToString(), StringComparison.OrdinalIgnoreCase);
        Assert.Equal(originalPackage, await File.ReadAllBytesAsync(packagePath, Cancellation));
        Assert.Equal(originalSource, await File.ReadAllBytesAsync(sourcePath, Cancellation));
        Assert.False(File.Exists(catalogPath));
        Assert.Empty(Directory.EnumerateFiles(workspace.Root, ".csdbcsv-*.tmp"));
        Assert.Empty(Directory.EnumerateFiles(workspace.Root, ".csharpdb-migration-*.tmp"));
    }

    [Fact]
    public async Task InspectCsv_CatalogPublicationFailurePreservesTheCompletedPackage()
    {
        using var workspace = new TemporaryDirectory();
        string sourcePath = Path.Combine(workspace.Root, "orders.csv");
        string packagePath = Path.Combine(workspace.Root, "orders.csdbcsv");
        string catalogDirectory = Path.Combine(workspace.Root, "catalog-output");
        string canaryPath = Path.Combine(catalogDirectory, "do-not-delete.txt");
        await WriteCsvAsync(sourcePath);
        Directory.CreateDirectory(catalogDirectory);
        await File.WriteAllTextAsync(canaryPath, "preserve this directory", Cancellation);
        var output = new StringWriter();
        var error = new StringWriter();

        int code = await MigrationCommandRunner.RunAsync(
            [
                "migrate", "inspect",
                "--source", "csv",
                "--input", sourcePath,
                "--package", packagePath,
                "--out", catalogDirectory,
                "--workspace", workspace.Root,
            ],
            output,
            error,
            Cancellation);

        Assert.Equal(InspectorCommandRunner.ExitError, code);
        Assert.True(string.IsNullOrWhiteSpace(output.ToString()));
        Assert.Contains("Catalog publication failed", error.ToString(), StringComparison.Ordinal);
        Assert.Contains("package was preserved", error.ToString(), StringComparison.OrdinalIgnoreCase);
        Assert.Contains(Path.GetFullPath(packagePath), error.ToString(), StringComparison.Ordinal);
        Assert.True(File.Exists(packagePath));
        Assert.True(Directory.Exists(catalogDirectory));
        Assert.Equal(
            "preserve this directory",
            await File.ReadAllTextAsync(canaryPath, Cancellation));

        await using (CsvSnapshotPackageSession session = await CsvSnapshotPackage.OpenAsync(
                         packagePath,
                         new CsvSnapshotPackageOpenOptions
                         {
                             WorkspacePath = workspace.Root,
                             MaxSourceBytes = 1024 * 1024,
                         },
                         Cancellation))
        {
            AssertCanonicalDigest(session.Manifest.ManifestDigest);
            Assert.Equal(MigrationSourceKind.Csv, session.Catalog.Source.Kind);
            Assert.Equal(3, session.Schema.RecordsExamined);
            Assert.Equal(
                MigrationArtifactSerializer.ComputeCatalogDigest(session.Catalog),
                session.Manifest.CatalogDigest);
        }

        Assert.Empty(Directory.EnumerateFiles(workspace.Root, ".csdbcsv-*.tmp"));
        Assert.Empty(Directory.EnumerateFiles(workspace.Root, ".csharpdb-migration-*.tmp"));
        AssertNoCsvWorkspaceDirectories(workspace.Root);
    }

    [Fact]
    public async Task ApplyCsv_MissingPackageOrDigestReturnsUsageBeforeTargetCreation()
    {
        using var workspace = new TemporaryDirectory();
        CsvArtifacts artifacts = await CreateCsvApplyReadyArtifactsAsync(workspace.Root);
        string targetPath = Path.Combine(workspace.Root, "staged.csdb");
        string reportPath = Path.Combine(workspace.Root, "run.json");
        string[][] optionSuffixes =
        [
            ["--expected-manifest-digest", artifacts.ManifestDigest],
            ["--source-package", artifacts.PackagePath],
        ];
        string[] expectedMessages =
        [
            "Missing required option --source-package",
            "Missing required option --expected-manifest-digest",
        ];

        for (int index = 0; index < optionSuffixes.Length; index++)
        {
            var output = new StringWriter();
            var error = new StringWriter();
            int code = await MigrationCommandRunner.RunAsync(
                ApplyArguments(
                    artifacts,
                    targetPath,
                    reportPath,
                    optionSuffixes[index]),
                output,
                error,
                Cancellation);

            Assert.Equal(InspectorCommandRunner.ExitUsage, code);
            Assert.True(string.IsNullOrWhiteSpace(output.ToString()));
            Assert.Contains(expectedMessages[index], error.ToString(), StringComparison.Ordinal);
            AssertTargetWasNotCreated(targetPath, reportPath);
        }
    }

    [Fact]
    public async Task ValidateCsv_MissingPackageOrDigestReturnsUsageBeforeTargetAccess()
    {
        using var workspace = new TemporaryDirectory();
        CsvArtifacts artifacts = await CreateCsvApplyReadyArtifactsAsync(workspace.Root);
        string targetPath = Path.Combine(workspace.Root, "missing-staged.csdb");
        string reportPath = Path.Combine(workspace.Root, "validation.json");
        string[][] optionSuffixes =
        [
            ["--expected-manifest-digest", artifacts.ManifestDigest],
            ["--source-package", artifacts.PackagePath],
        ];

        foreach (string[] suffix in optionSuffixes)
        {
            var output = new StringWriter();
            var error = new StringWriter();
            int code = await MigrationCommandRunner.RunAsync(
                ValidateArguments(artifacts, targetPath, reportPath, suffix),
                output,
                error,
                Cancellation);

            Assert.Equal(InspectorCommandRunner.ExitUsage, code);
            Assert.True(string.IsNullOrWhiteSpace(output.ToString()));
            Assert.Contains("Missing required option", error.ToString(), StringComparison.Ordinal);
            AssertTargetWasNotCreated(targetPath, reportPath);
        }
    }

    [Fact]
    public async Task ApplyCsv_FutureTargetWorkspacePathsReturnUsageWithoutCreatingAnything()
    {
        using var workspace = new TemporaryDirectory();
        CsvArtifacts artifacts = await CreateCsvApplyReadyArtifactsAsync(workspace.Root);
        (string Target, string Workspace, string Report)[] cases =
        [
            (
                Path.Combine(workspace.Root, "future-target.csdb"),
                Path.Combine(workspace.Root, "future-target.csdb"),
                Path.Combine(workspace.Root, "future-target-run.json")),
            (
                Path.Combine(workspace.Root, "nested-target.csdb"),
                Path.Combine(workspace.Root, "nested-target.csdb", "scratch"),
                Path.Combine(workspace.Root, "nested-target-run.json")),
        ];

        foreach ((string targetPath, string workspacePath, string reportPath) in cases)
        {
            var output = new StringWriter();
            var error = new StringWriter();
            int code = await MigrationCommandRunner.RunAsync(
                ApplyArguments(
                    artifacts,
                    targetPath,
                    reportPath,
                    [
                        "--source-package", artifacts.PackagePath,
                        "--expected-manifest-digest", artifacts.ManifestDigest,
                        "--workspace", workspacePath,
                    ]),
                output,
                error,
                Cancellation);

            Assert.Equal(InspectorCommandRunner.ExitUsage, code);
            Assert.True(string.IsNullOrWhiteSpace(output.ToString()));
            Assert.Contains("workspace", error.ToString(), StringComparison.OrdinalIgnoreCase);
            Assert.False(Directory.Exists(workspacePath));
            Assert.False(Directory.Exists(targetPath));
            AssertTargetWasNotCreated(targetPath, reportPath);
        }
    }

    [Fact]
    public async Task PlanCsv_DeterministicModeRequiresCompletePositivePolicyAndKeepsStrictBytes()
    {
        using var workspace = new TemporaryDirectory();
        string missingCatalogPath = Path.Combine(workspace.Root, "missing-catalog.json");
        string invalidPolicyOutput = Path.Combine(workspace.Root, "invalid-policy.json");
        var invalidPolicyError = new StringWriter();
        int invalidPolicyCode = await MigrationCommandRunner.RunAsync(
            PlanArguments(
                missingCatalogPath,
                invalidPolicyOutput,
                ["--reject-rules", DeterministicRejectRules]),
            TextWriter.Null,
            invalidPolicyError,
            Cancellation);

        Assert.Equal(InspectorCommandRunner.ExitUsage, invalidPolicyCode);
        Assert.Contains(
            "reject-mode",
            invalidPolicyError.ToString(),
            StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(
            "could not find",
            invalidPolicyError.ToString(),
            StringComparison.OrdinalIgnoreCase);
        Assert.False(File.Exists(invalidPolicyOutput));

        CsvArtifacts strict = await CreateCsvApplyReadyArtifactsAsync(workspace.Root);
        MigrationCatalog catalog = MigrationArtifactSerializer.DeserializeCatalog(
            await File.ReadAllTextAsync(strict.CatalogPath, Cancellation));
        MigrationPlan expectedStrict = new MigrationPlanner().CreatePlan(
            catalog,
            new MigrationPlanningOptions { AcceptAllExclusions = true });
        string strictJson = await File.ReadAllTextAsync(strict.PlanPath, Cancellation);

        Assert.Equal(
            MigrationArtifactSerializer.SerializePlan(expectedStrict, catalog),
            strictJson);
        Assert.Equal(MigrationRejectMode.FailFast, expectedStrict.Load.RejectMode);
        Assert.Null(expectedStrict.Load.RejectPolicy);

        string[][] policyOptions = DeterministicPlanPolicyArguments()
            .Chunk(2)
            .Select(pair => pair.ToArray())
            .Skip(1)
            .ToArray();
        for (int index = 0; index < policyOptions.Length; index++)
        {
            string outputPath = Path.Combine(workspace.Root, $"policy-only-{index}.json");
            var error = new StringWriter();
            int code = await MigrationCommandRunner.RunAsync(
                PlanArguments(strict.CatalogPath, outputPath, policyOptions[index]),
                TextWriter.Null,
                error,
                Cancellation);

            Assert.Equal(InspectorCommandRunner.ExitUsage, code);
            Assert.Contains("reject-mode", error.ToString(), StringComparison.OrdinalIgnoreCase);
            Assert.False(File.Exists(outputPath));
        }

        string[] completePolicy = DeterministicPlanPolicyArguments();
        for (int missingPair = 0; missingPair < completePolicy.Length / 2; missingPair++)
        {
            string outputPath = Path.Combine(workspace.Root, $"missing-policy-{missingPair}.json");
            string[] incomplete = RemoveOptionPair(completePolicy, missingPair);
            var error = new StringWriter();
            int code = await MigrationCommandRunner.RunAsync(
                PlanArguments(strict.CatalogPath, outputPath, incomplete),
                TextWriter.Null,
                error,
                Cancellation);

            Assert.Equal(InspectorCommandRunner.ExitUsage, code);
            Assert.False(File.Exists(outputPath));
        }

        for (int limitPair = 2; limitPair < completePolicy.Length / 2; limitPair++)
        {
            string outputPath = Path.Combine(workspace.Root, $"nonpositive-policy-{limitPair}.json");
            string[] nonpositive = completePolicy.ToArray();
            nonpositive[(limitPair * 2) + 1] = "0";
            var error = new StringWriter();
            int code = await MigrationCommandRunner.RunAsync(
                PlanArguments(strict.CatalogPath, outputPath, nonpositive),
                TextWriter.Null,
                error,
                Cancellation);

            Assert.Equal(InspectorCommandRunner.ExitUsage, code);
            Assert.Contains("positive", error.ToString(), StringComparison.OrdinalIgnoreCase);
            Assert.False(File.Exists(outputPath));
        }

        string deterministicPath = Path.Combine(workspace.Root, "deterministic-plan.json");
        var deterministicError = new StringWriter();
        int deterministicCode = await MigrationCommandRunner.RunAsync(
            PlanArguments(strict.CatalogPath, deterministicPath, completePolicy),
            TextWriter.Null,
            deterministicError,
            Cancellation);

        Assert.Equal(InspectorCommandRunner.ExitWarn, deterministicCode);
        Assert.True(string.IsNullOrWhiteSpace(deterministicError.ToString()));
        MigrationPlan deterministic = MigrationArtifactSerializer.DeserializePlan(
            await File.ReadAllTextAsync(deterministicPath, Cancellation),
            catalog);
        MigrationDeterministicRejectPolicy policy =
            Assert.IsType<MigrationDeterministicRejectPolicy>(deterministic.Load.RejectPolicy);
        Assert.Equal(MigrationRejectMode.DeterministicRejects, deterministic.Load.RejectMode);
        Assert.Equal(MigrationRejectContract.DeterministicRejectsV1, policy.ContractVersion);
        Assert.Equal(
            [
                CsvMigrationDataRules.MissingField,
                CsvMigrationDataRules.NullNotAllowed,
                CsvMigrationDataRules.TypeMismatch,
            ],
            policy.AllowedRuleIds);
        Assert.Equal(100, policy.MaxRejectedRowsPerBatch);
        Assert.Equal(10_000, policy.MaxRejectedRowsPerRun);
        Assert.Equal(4_096, policy.MaxRawValueBytes);
        Assert.Equal(65_536, policy.MaxRawValueBytesPerBatch);
        Assert.Equal(1_048_576, policy.MaxRawValueBytesPerRun);
        Assert.Equal(16_777_216, policy.MaxArtifactBytes);
        Assert.NotEqual(
            MigrationArtifactSerializer.ComputePlanDigest(expectedStrict),
            MigrationArtifactSerializer.ComputePlanDigest(deterministic));

        string[] allRulesPolicy = completePolicy.ToArray();
        allRulesPolicy[3] = "all";
        string allRulesPath = Path.Combine(workspace.Root, "deterministic-plan-all-rules.json");
        var allRulesError = new StringWriter();
        int allRulesCode = await MigrationCommandRunner.RunAsync(
            PlanArguments(strict.CatalogPath, allRulesPath, allRulesPolicy),
            TextWriter.Null,
            allRulesError,
            Cancellation);

        Assert.Equal(InspectorCommandRunner.ExitWarn, allRulesCode);
        Assert.True(string.IsNullOrWhiteSpace(allRulesError.ToString()));
        MigrationPlan allRules = MigrationArtifactSerializer.DeserializePlan(
            await File.ReadAllTextAsync(allRulesPath, Cancellation),
            catalog);
        Assert.Equal(policy.AllowedRuleIds, allRules.Load.RejectPolicy!.AllowedRuleIds);
        Assert.Equal(
            MigrationArtifactSerializer.ComputePlanDigest(deterministic),
            MigrationArtifactSerializer.ComputePlanDigest(allRules));
    }

    [Fact]
    public async Task StrictCsvApplyAndValidate_ForbidDeterministicConsentAndArtifactOptions()
    {
        using var workspace = new TemporaryDirectory();
        CsvArtifacts artifacts = await CreateCsvApplyReadyArtifactsAsync(workspace.Root);
        string targetPath = Path.Combine(workspace.Root, "staged.csdb");
        string runPath = Path.Combine(workspace.Root, "run.json");
        string validationPath = Path.Combine(workspace.Root, "validation.json");
        string rejectPath = Path.Combine(workspace.Root, "rejects.jsonl");
        string[] deterministicOptions =
        [
            "--allow-deterministic-rejects",
            "--reject-artifact", rejectPath,
        ];

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
                    .. deterministicOptions,
                ]),
            TextWriter.Null,
            applyError,
            Cancellation);

        Assert.Equal(InspectorCommandRunner.ExitUsage, applyCode);
        Assert.Contains("fail-fast", applyError.ToString(), StringComparison.OrdinalIgnoreCase);
        AssertTargetWasNotCreated(targetPath, runPath);
        Assert.False(File.Exists(rejectPath));

        var validateError = new StringWriter();
        int validateCode = await MigrationCommandRunner.RunAsync(
            ValidateArguments(
                artifacts,
                targetPath,
                validationPath,
                [
                    "--source-package", artifacts.PackagePath,
                    "--expected-manifest-digest", artifacts.ManifestDigest,
                    "--workspace", workspace.Root,
                    .. deterministicOptions,
                ]),
            TextWriter.Null,
            validateError,
            Cancellation);

        Assert.Equal(InspectorCommandRunner.ExitUsage, validateCode);
        Assert.Contains("fail-fast", validateError.ToString(), StringComparison.OrdinalIgnoreCase);
        AssertTargetWasNotCreated(targetPath, validationPath);
        Assert.False(File.Exists(rejectPath));
    }

    [Fact]
    public async Task DeterministicCsv_ApplyResumeAndValidate_PublishAndReuseExactRejectArtifact()
    {
        using var workspace = new TemporaryDirectory();
        CsvArtifacts artifacts = await CreateCsvApplyReadyArtifactsAsync(
            workspace.Root,
            MixedRejectCsvContents,
            inspectSuffix: ["--sample-rows", "2"],
            planSuffix: DeterministicPlanPolicyArguments());
        string targetPath = Path.Combine(workspace.Root, "staged.csdb");
        string runPath = Path.Combine(workspace.Root, "run.json");
        string rejectPath = Path.Combine(workspace.Root, "rejects.jsonl");
        string[] sourceOptions =
        [
            "--source-package", artifacts.PackagePath,
            "--expected-manifest-digest", artifacts.ManifestDigest,
            "--workspace", workspace.Root,
        ];

        (string[] MissingOptions, string ExpectedMessage)[] incompleteApplyOptions =
        [
            (
                [.. sourceOptions, "--reject-artifact", rejectPath],
                "--allow-deterministic-rejects"),
            (
                [.. sourceOptions, "--allow-deterministic-rejects"],
                "--reject-artifact"),
            (
                [
                    .. sourceOptions,
                    "--allow-deterministic-rejects",
                    "--reject-artifact", "rejects.jsonl",
                ],
                "absolute"),
            (
                [
                    .. sourceOptions,
                    "--allow-deterministic-rejects",
                    "--reject-artifact",
                    Path.Combine(workspace.Root, "child", "..", "rejects.jsonl"),
                ],
                "normalized"),
        ];
        for (int index = 0; index < incompleteApplyOptions.Length; index++)
        {
            string failedRunPath = Path.Combine(workspace.Root, $"run-invalid-{index}.json");
            var error = new StringWriter();
            int code = await MigrationCommandRunner.RunAsync(
                ApplyArguments(
                    artifacts,
                    targetPath,
                    failedRunPath,
                    incompleteApplyOptions[index].MissingOptions),
                TextWriter.Null,
                error,
                Cancellation);

            Assert.Equal(InspectorCommandRunner.ExitUsage, code);
            Assert.Contains(
                incompleteApplyOptions[index].ExpectedMessage,
                error.ToString(),
                StringComparison.OrdinalIgnoreCase);
            AssertTargetWasNotCreated(targetPath, failedRunPath);
            Assert.False(File.Exists(rejectPath));
        }

        MigrationCatalog boundCatalog = MigrationArtifactSerializer.DeserializeCatalog(
            await File.ReadAllTextAsync(artifacts.CatalogPath, Cancellation));
        MigrationPlan boundPlan = MigrationArtifactSerializer.DeserializePlan(
            await File.ReadAllTextAsync(artifacts.PlanPath, Cancellation),
            boundCatalog);
        MigrationRejectArtifactDestinationBinding rejectBinding =
            MigrationRejectArtifactDestinationValidator.ValidateForPublication(
                boundPlan,
                rejectPath);
        (string ArtifactPath, string ReportPath)[] collisionCases =
        [
            (targetPath, Path.Combine(workspace.Root, "run-target-collision.json")),
            (rejectPath, rejectBinding.TemporaryPath),
        ];
        foreach ((string artifactPath, string collisionReportPath) in collisionCases)
        {
            var collisionError = new StringWriter();
            int collisionCode = await MigrationCommandRunner.RunAsync(
                ApplyArguments(
                    artifacts,
                    targetPath,
                    collisionReportPath,
                    [
                        .. sourceOptions,
                        "--allow-deterministic-rejects",
                        "--reject-artifact", artifactPath,
                    ]),
                TextWriter.Null,
                collisionError,
                Cancellation);

            Assert.Equal(InspectorCommandRunner.ExitUsage, collisionCode);
            Assert.Contains(
                "different paths",
                collisionError.ToString(),
                StringComparison.OrdinalIgnoreCase);
            AssertTargetWasNotCreated(targetPath, collisionReportPath);
            Assert.False(File.Exists(rejectPath));
        }

        var applyOutput = new StringWriter();
        var applyError = new StringWriter();
        int applyCode = await MigrationCommandRunner.RunAsync(
            ApplyArguments(
                artifacts,
                targetPath,
                runPath,
                [
                    .. sourceOptions,
                    "--allow-deterministic-rejects",
                    "--reject-artifact", rejectPath,
                    "--format", "json",
                ]),
            applyOutput,
            applyError,
            Cancellation);

        Assert.Equal(InspectorCommandRunner.ExitWarn, applyCode);
        Assert.True(string.IsNullOrWhiteSpace(applyError.ToString()));
        Assert.True(File.Exists(targetPath));
        Assert.True(File.Exists(runPath));
        Assert.True(File.Exists(rejectPath));
        Assert.False(File.Exists(targetPath + ".migration.lock"));
        byte[] originalRejectBytes = await File.ReadAllBytesAsync(rejectPath, Cancellation);
        using JsonDocument applyStdout = JsonDocument.Parse(applyOutput.ToString());
        using JsonDocument applyReport = JsonDocument.Parse(
            await File.ReadAllTextAsync(runPath, Cancellation));
        Assert.Equal(
            applyStdout.RootElement.GetRawText(),
            applyReport.RootElement.GetRawText());
        AssertDeterministicRunReport(
            applyReport.RootElement,
            rejectedRows: 1,
            rejectedRowsWritten: 1,
            rejectedRowsSkipped: 0,
            artifactBytes: originalRejectBytes.LongLength,
            artifactReused: false);
        Assert.DoesNotContain(
            "bad",
            applyReport.RootElement.GetRawText(),
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            rejectPath,
            applyReport.RootElement.GetRawText(),
            StringComparison.OrdinalIgnoreCase);

        string[] artifactLines = Encoding.UTF8.GetString(originalRejectBytes).Split(
            '\n',
            StringSplitOptions.RemoveEmptyEntries);
        Assert.Equal(2, artifactLines.Length);
        using JsonDocument artifactHeader = JsonDocument.Parse(artifactLines[0]);
        using JsonDocument artifactEntry = JsonDocument.Parse(artifactLines[1]);
        Assert.Equal(
            MigrationRejectLedgerCodec.ArtifactFormat,
            artifactHeader.RootElement.GetProperty("format").GetString());
        Assert.Equal(
            applyReport.RootElement.GetProperty("planDigest").GetString(),
            artifactHeader.RootElement.GetProperty("planDigest").GetString());
        JsonElement entry = artifactEntry.RootElement;
        Assert.Equal(MigrationRejectLedgerCodec.EntryFormat, entry.GetProperty("format").GetString());
        Assert.Equal(CsvMigrationObjectIds.Table, entry.GetProperty("sourceObjectId").GetString());
        Assert.Equal(0, entry.GetProperty("batchOrdinal").GetInt64());
        Assert.Equal(2, entry.GetProperty("sourceRowOrdinal").GetInt64());
        Assert.Equal(CsvMigrationDataRules.TypeMismatch, entry.GetProperty("ruleId").GetString());
        Assert.Equal(CsvMigrationObjectIds.Column(0), entry.GetProperty("columnObjectId").GetString());
        Dictionary<string, string?> evidence = entry.GetProperty("evidence")
            .EnumerateArray()
            .ToDictionary(
                item => item.GetProperty("name").GetString()!,
                item => item.GetProperty("value").ValueKind == JsonValueKind.Null
                    ? null
                    : item.GetProperty("value").GetString(),
                StringComparer.Ordinal);
        Assert.Equal("0", evidence["columnIndex"]);
        Assert.Equal("3", evidence["dataRecordNumber"]);
        Assert.Equal("4", evidence["endPhysicalLine"]);
        Assert.Equal("Text", evidence["fieldKind"]);
        Assert.Equal("4", evidence["logicalRecordNumber"]);
        Assert.Equal("bad", evidence[MigrationRejectLedgerCodec.RawValueEvidenceName]);
        Assert.Equal("4", evidence["startPhysicalLine"]);
        Assert.Equal("false", evidence["wasQuoted"]);

        await using (var database = await Database.OpenAsync(targetPath, Cancellation))
        await using (var query = await database.ExecuteAsync(
                         "SELECT id, name FROM csv_data ORDER BY id;",
                         Cancellation))
        {
            var rows = await query.ToListAsync(Cancellation);
            Assert.Equal([1L, 2L, 3L], rows.Select(row => row[0].AsInteger));
            Assert.Equal(["alpha", "bravo", "charlie"], rows.Select(row => row[1].AsText));
        }
        Assert.Equal("awaiting-validation", await ReadLifecycleAsync(targetPath));

        string resumeRunPath = Path.Combine(workspace.Root, "run-resume.json");
        var resumeOutput = new StringWriter();
        var resumeError = new StringWriter();
        int resumeCode = await MigrationCommandRunner.RunAsync(
            ApplyArguments(
                artifacts,
                targetPath,
                resumeRunPath,
                [
                    .. sourceOptions,
                    "--allow-deterministic-rejects",
                    "--reject-artifact", rejectPath,
                    "--resume",
                    "--format", "json",
                ]),
            resumeOutput,
            resumeError,
            Cancellation);

        Assert.Equal(InspectorCommandRunner.ExitWarn, resumeCode);
        Assert.True(string.IsNullOrWhiteSpace(resumeError.ToString()));
        using JsonDocument resumeReport = JsonDocument.Parse(
            await File.ReadAllTextAsync(resumeRunPath, Cancellation));
        Assert.Equal(resumeOutput.ToString().Trim(), resumeReport.RootElement.GetRawText());
        AssertDeterministicRunReport(
            resumeReport.RootElement,
            rejectedRows: 1,
            rejectedRowsWritten: 0,
            rejectedRowsSkipped: 1,
            artifactBytes: originalRejectBytes.LongLength,
            artifactReused: true);
        Assert.Equal(originalRejectBytes, await File.ReadAllBytesAsync(rejectPath, Cancellation));
        Assert.Equal(
            applyReport.RootElement.GetProperty("rejectArtifactDigest").GetString(),
            resumeReport.RootElement.GetProperty("rejectArtifactDigest").GetString());
        Assert.Equal("awaiting-validation", await ReadLifecycleAsync(targetPath));

        byte[] applyConflictBytes = Encoding.UTF8.GetBytes(
            "operator-owned-apply-conflict\n");
        await File.WriteAllBytesAsync(rejectPath, applyConflictBytes, Cancellation);
        string conflictRunPath = Path.Combine(workspace.Root, "run-artifact-conflict.json");
        var conflictOutput = new StringWriter();
        var conflictError = new StringWriter();
        int conflictCode = await MigrationCommandRunner.RunAsync(
            ApplyArguments(
                artifacts,
                targetPath,
                conflictRunPath,
                [
                    .. sourceOptions,
                    "--allow-deterministic-rejects",
                    "--reject-artifact", rejectPath,
                    "--resume",
                    "--format", "json",
                ]),
            conflictOutput,
            conflictError,
            Cancellation);

        Assert.Equal(InspectorCommandRunner.ExitError, conflictCode);
        Assert.True(string.IsNullOrWhiteSpace(conflictOutput.ToString()));
        Assert.Contains(
            "MIG-APPLY-REJECT-ARTIFACT-001",
            conflictError.ToString(),
            StringComparison.Ordinal);
        Assert.Equal(applyConflictBytes, await File.ReadAllBytesAsync(rejectPath, Cancellation));
        using (JsonDocument conflictReport = JsonDocument.Parse(
                   await File.ReadAllTextAsync(conflictRunPath, Cancellation)))
        {
            JsonElement conflict = conflictReport.RootElement;
            Assert.Equal("awaitingValidation", conflict.GetProperty("status").GetString());
            Assert.Equal("unconfirmed", conflict.GetProperty("rejectArtifactStatus").GetString());
            Assert.Equal(
                "MIG-APPLY-REJECT-ARTIFACT-001",
                conflict.GetProperty("errorCode").GetString());
            Assert.Equal(1, conflict.GetProperty("rejectedRows").GetInt64());
            Assert.Equal(0, conflict.GetProperty("rejectedRowsWritten").GetInt64());
            Assert.Equal(1, conflict.GetProperty("rejectedRowsSkipped").GetInt64());
            Assert.False(conflict.TryGetProperty("rejectArtifactPath", out _));
            Assert.DoesNotContain(
                "operator-owned-apply-conflict",
                conflict.GetRawText(),
                StringComparison.Ordinal);
        }
        Assert.Equal("awaiting-validation", await ReadLifecycleAsync(targetPath));
        await File.WriteAllBytesAsync(rejectPath, originalRejectBytes, Cancellation);

        string missingConsentReport = Path.Combine(workspace.Root, "validation-no-consent.json");
        var missingConsentError = new StringWriter();
        int missingConsentCode = await MigrationCommandRunner.RunAsync(
            ValidateArguments(
                artifacts,
                targetPath,
                missingConsentReport,
                [
                    .. sourceOptions,
                    "--reject-artifact", rejectPath,
                ]),
            TextWriter.Null,
            missingConsentError,
            Cancellation);
        Assert.Equal(InspectorCommandRunner.ExitUsage, missingConsentCode);
        Assert.Contains(
            "--allow-deterministic-rejects",
            missingConsentError.ToString(),
            StringComparison.Ordinal);
        Assert.False(File.Exists(missingConsentReport));
        Assert.Equal("awaiting-validation", await ReadLifecycleAsync(targetPath));

        string missingArtifactReport = Path.Combine(workspace.Root, "validation-no-artifact.json");
        var missingArtifactError = new StringWriter();
        int missingArtifactCode = await MigrationCommandRunner.RunAsync(
            ValidateArguments(
                artifacts,
                targetPath,
                missingArtifactReport,
                [
                    .. sourceOptions,
                    "--allow-deterministic-rejects",
                ]),
            TextWriter.Null,
            missingArtifactError,
            Cancellation);
        Assert.Equal(InspectorCommandRunner.ExitUsage, missingArtifactCode);
        Assert.Contains(
            "--reject-artifact",
            missingArtifactError.ToString(),
            StringComparison.Ordinal);
        Assert.False(File.Exists(missingArtifactReport));
        Assert.Equal("awaiting-validation", await ReadLifecycleAsync(targetPath));

        byte[] differentBytes = Encoding.UTF8.GetBytes("operator-owned-different-artifact\n");
        await File.WriteAllBytesAsync(rejectPath, differentBytes, Cancellation);
        string rejectedValidationPath = Path.Combine(workspace.Root, "validation-rejected.json");
        var rejectedValidationError = new StringWriter();
        int rejectedValidationCode = await MigrationCommandRunner.RunAsync(
            ValidateArguments(
                artifacts,
                targetPath,
                rejectedValidationPath,
                [
                    .. sourceOptions,
                    "--allow-deterministic-rejects",
                    "--reject-artifact", rejectPath,
                    "--level", "checksum",
                    "--spill-dir", workspace.Root,
                ]),
            TextWriter.Null,
            rejectedValidationError,
            Cancellation);

        Assert.Equal(InspectorCommandRunner.ExitError, rejectedValidationCode);
        Assert.True(!string.IsNullOrWhiteSpace(rejectedValidationError.ToString()));
        Assert.Equal(differentBytes, await File.ReadAllBytesAsync(rejectPath, Cancellation));
        Assert.True(File.Exists(rejectedValidationPath));
        MigrationValidationReport rejectedValidation =
            MigrationValidationReportSerializer.Deserialize(
                await File.ReadAllTextAsync(rejectedValidationPath, Cancellation));
        Assert.Equal(MigrationValidationStatus.Passed, rejectedValidation.Outcome);
        Assert.Equal(MigrationValidationLevel.Checksum, rejectedValidation.Level);
        Assert.Equal("awaiting-validation", await ReadLifecycleAsync(targetPath));
        await File.WriteAllBytesAsync(rejectPath, originalRejectBytes, Cancellation);

        const string plantedOutputFailure = "planted-output-failure-secret";
        string outputFailureReportPath = Path.Combine(
            workspace.Root,
            "validation-output-failure.json");
        var outputFailureError = new StringWriter();
        int outputFailureCode = await MigrationCommandRunner.RunAsync(
            ValidateArguments(
                artifacts,
                targetPath,
                outputFailureReportPath,
                [
                    .. sourceOptions,
                    "--allow-deterministic-rejects",
                    "--reject-artifact", rejectPath,
                    "--level", "checksum",
                    "--spill-dir", workspace.Root,
                    "--format", "json",
                ]),
            new ThrowingTextWriter(plantedOutputFailure),
            outputFailureError,
            Cancellation);

        Assert.Equal(InspectorCommandRunner.ExitError, outputFailureCode);
        Assert.Contains(
            "MIG-VALIDATE-OUTPUT-AFTER-ACTIVATION-001",
            outputFailureError.ToString(),
            StringComparison.Ordinal);
        Assert.Contains(
            "was activated",
            outputFailureError.ToString(),
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            plantedOutputFailure,
            outputFailureError.ToString(),
            StringComparison.Ordinal);
        MigrationValidationReport outputFailureReport =
            MigrationValidationReportSerializer.Deserialize(
                await File.ReadAllTextAsync(outputFailureReportPath, Cancellation));
        Assert.Equal(MigrationValidationStatus.Passed, outputFailureReport.Outcome);
        Assert.Equal("activated", await ReadLifecycleAsync(targetPath));
        Assert.Equal(originalRejectBytes, await File.ReadAllBytesAsync(rejectPath, Cancellation));

        string validationPath = Path.Combine(workspace.Root, "validation.json");
        var validationOutput = new StringWriter();
        var validationError = new StringWriter();
        int validationCode = await MigrationCommandRunner.RunAsync(
            ValidateArguments(
                artifacts,
                targetPath,
                validationPath,
                [
                    .. sourceOptions,
                    "--allow-deterministic-rejects",
                    "--reject-artifact", rejectPath,
                    "--level", "checksum",
                    "--spill-dir", workspace.Root,
                ]),
            validationOutput,
            validationError,
            Cancellation);

        Assert.Equal(InspectorCommandRunner.ExitWarn, validationCode);
        Assert.True(string.IsNullOrWhiteSpace(validationError.ToString()));
        Assert.Contains("Status: PASSED", validationOutput.ToString(), StringComparison.Ordinal);
        Assert.Contains("Activation: activated", validationOutput.ToString(), StringComparison.Ordinal);
        MigrationValidationReport validation = MigrationValidationReportSerializer.Deserialize(
            await File.ReadAllTextAsync(validationPath, Cancellation));
        Assert.Equal(MigrationValidationStatus.Passed, validation.Outcome);
        Assert.Equal(MigrationValidationLevel.Checksum, validation.Level);
        Assert.Equal(originalRejectBytes, await File.ReadAllBytesAsync(rejectPath, Cancellation));
        Assert.Equal("activated", await ReadLifecycleAsync(targetPath));
        Assert.Empty(Directory.EnumerateFiles(
            workspace.Root,
            ".csharpdb-reject-*.tmp"));
        AssertNoCsvWorkspaceDirectories(workspace.Root);
    }

    [Fact]
    public async Task ApplyCsv_MalformedDigestReturnsUsageBeforeTargetCreation()
    {
        using var workspace = new TemporaryDirectory();
        CsvArtifacts artifacts = await CreateCsvApplyReadyArtifactsAsync(workspace.Root);
        string targetPath = Path.Combine(workspace.Root, "staged.csdb");
        string reportPath = Path.Combine(workspace.Root, "run.json");
        string[] malformedDigests =
        [
            "sha256:abc",
            "SHA256:" + new string('0', 64),
            "sha256:" + new string('A', 64),
            "sha512:" + new string('0', 64),
        ];

        foreach (string digest in malformedDigests)
        {
            var output = new StringWriter();
            var error = new StringWriter();
            int code = await MigrationCommandRunner.RunAsync(
                ApplyArguments(
                    artifacts,
                    targetPath,
                    reportPath,
                    [
                        "--source-package", artifacts.PackagePath,
                        "--expected-manifest-digest", digest,
                    ]),
                output,
                error,
                Cancellation);

            Assert.Equal(InspectorCommandRunner.ExitUsage, code);
            Assert.True(string.IsNullOrWhiteSpace(output.ToString()));
            Assert.Contains("canonical lowercase", error.ToString(), StringComparison.OrdinalIgnoreCase);
            AssertTargetWasNotCreated(targetPath, reportPath);
        }
    }

    [Fact]
    public async Task ApplyCsv_WrongCanonicalDigestFailsBeforeTargetCreation()
    {
        using var workspace = new TemporaryDirectory();
        CsvArtifacts artifacts = await CreateCsvApplyReadyArtifactsAsync(workspace.Root);
        string targetPath = Path.Combine(workspace.Root, "staged.csdb");
        string reportPath = Path.Combine(workspace.Root, "run.json");
        byte[] originalPackage = await File.ReadAllBytesAsync(artifacts.PackagePath, Cancellation);
        string wrongDigest = DifferentDigest(artifacts.ManifestDigest);
        var output = new StringWriter();
        var error = new StringWriter();

        int code = await MigrationCommandRunner.RunAsync(
            ApplyArguments(
                artifacts,
                targetPath,
                reportPath,
                [
                    "--source-package", artifacts.PackagePath,
                    "--expected-manifest-digest", wrongDigest,
                    "--workspace", workspace.Root,
                ]),
            output,
            error,
            Cancellation);

        Assert.Equal(InspectorCommandRunner.ExitError, code);
        Assert.True(string.IsNullOrWhiteSpace(output.ToString()));
        Assert.Contains(CsvSnapshotPackageRules.IntegrityMismatch, error.ToString(), StringComparison.Ordinal);
        Assert.Contains("trusted manifest digest", error.ToString(), StringComparison.OrdinalIgnoreCase);
        Assert.Equal(originalPackage, await File.ReadAllBytesAsync(artifacts.PackagePath, Cancellation));
        AssertTargetWasNotCreated(targetPath, reportPath);
        AssertNoCsvWorkspaceDirectories(workspace.Root);
    }

    [Fact]
    public async Task ApplyCsv_DifferentValidPackageAndDigestFailCatalogBindingBeforeTargetCreation()
    {
        using var workspace = new TemporaryDirectory();
        string firstDirectory = Path.Combine(workspace.Root, "first");
        string secondDirectory = Path.Combine(workspace.Root, "second");
        Directory.CreateDirectory(firstDirectory);
        Directory.CreateDirectory(secondDirectory);
        CsvArtifacts first = await CreateCsvApplyReadyArtifactsAsync(firstDirectory);
        CsvArtifacts second = await CreateCsvApplyReadyArtifactsAsync(
            secondDirectory,
            "id,name\n10,delta\n20,echo\n30,foxtrot\n");
        Assert.NotEqual(first.ManifestDigest, second.ManifestDigest);
        byte[] originalSecondPackage = await File.ReadAllBytesAsync(
            second.PackagePath,
            Cancellation);
        string targetPath = Path.Combine(workspace.Root, "staged.csdb");
        string reportPath = Path.Combine(workspace.Root, "run.json");
        var output = new StringWriter();
        var error = new StringWriter();

        int code = await MigrationCommandRunner.RunAsync(
            ApplyArguments(
                first,
                targetPath,
                reportPath,
                [
                    "--source-package", second.PackagePath,
                    "--expected-manifest-digest", second.ManifestDigest,
                    "--workspace", workspace.Root,
                ]),
            output,
            error,
            Cancellation);

        Assert.Equal(InspectorCommandRunner.ExitError, code);
        Assert.True(string.IsNullOrWhiteSpace(output.ToString()));
        Assert.Contains(
            "retained CSV package catalog does not match",
            error.ToString(),
            StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("trusted manifest digest", error.ToString(), StringComparison.OrdinalIgnoreCase);
        Assert.Equal(
            originalSecondPackage,
            await File.ReadAllBytesAsync(second.PackagePath, Cancellation));
        AssertTargetWasNotCreated(targetPath, reportPath);
        AssertNoCsvWorkspaceDirectories(workspace.Root);
    }

    [Fact]
    public async Task InspectPlanAndApplyCsv_SucceedsAfterTheRawInputIsDeleted()
    {
        using var workspace = new TemporaryDirectory();
        CsvArtifacts artifacts = await CreateCsvApplyReadyArtifactsAsync(workspace.Root);
        byte[] originalPackage = await File.ReadAllBytesAsync(artifacts.PackagePath, Cancellation);
        File.Delete(artifacts.SourcePath);
        string targetPath = Path.Combine(workspace.Root, "staged.csdb");
        string reportPath = Path.Combine(workspace.Root, "run.json");
        var output = new StringWriter();
        var error = new StringWriter();

        int code = await MigrationCommandRunner.RunAsync(
            ApplyArguments(
                artifacts,
                targetPath,
                reportPath,
                [
                    "--source-package", artifacts.PackagePath,
                    "--expected-manifest-digest", artifacts.ManifestDigest,
                    "--workspace", workspace.Root,
                    "--format", "json",
                ]),
            output,
            error,
            Cancellation);

        Assert.Equal(InspectorCommandRunner.ExitWarn, code);
        Assert.True(string.IsNullOrWhiteSpace(error.ToString()));
        Assert.False(File.Exists(artifacts.SourcePath));
        Assert.True(File.Exists(artifacts.PackagePath));
        Assert.Equal(originalPackage, await File.ReadAllBytesAsync(artifacts.PackagePath, Cancellation));
        Assert.True(File.Exists(targetPath));
        Assert.True(File.Exists(reportPath));
        Assert.False(File.Exists(targetPath + ".migration.lock"));
        using JsonDocument stdout = JsonDocument.Parse(output.ToString());
        using JsonDocument report = JsonDocument.Parse(
            await File.ReadAllTextAsync(reportPath, Cancellation));
        JsonElement result = stdout.RootElement;
        Assert.Equal("csharpdb-migration-run/v1", result.GetProperty("format").GetString());
        Assert.Equal("awaitingValidation", result.GetProperty("status").GetString());
        Assert.Equal(3, result.GetProperty("rowsWritten").GetInt64());
        string targetIdentity = Assert.IsType<string>(
            result.GetProperty("targetIdentity").GetString());
        long firstBatchesWritten = result.GetProperty("batchesWritten").GetInt64();
        Assert.True(firstBatchesWritten > 0);
        Assert.Equal(artifacts.ManifestDigest, result
            .GetProperty("sourcePackageManifestDigest")
            .GetString());
        Assert.Equal(result.GetRawText(), report.RootElement.GetRawText());
        AssertNoCsvWorkspaceDirectories(workspace.Root);

        string resumeReportPath = Path.Combine(workspace.Root, "run-resume.json");
        var resumeOutput = new StringWriter();
        var resumeError = new StringWriter();
        int resumeCode = await MigrationCommandRunner.RunAsync(
            ApplyArguments(
                artifacts,
                targetPath,
                resumeReportPath,
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
        using JsonDocument resumedStdout = JsonDocument.Parse(resumeOutput.ToString());
        using JsonDocument resumedReport = JsonDocument.Parse(
            await File.ReadAllTextAsync(resumeReportPath, Cancellation));
        JsonElement resumed = resumedStdout.RootElement;
        Assert.Equal(resumed.GetRawText(), resumedReport.RootElement.GetRawText());
        Assert.Equal(targetIdentity, resumed.GetProperty("targetIdentity").GetString());
        Assert.Equal(0, resumed.GetProperty("batchesWritten").GetInt64());
        Assert.Equal(firstBatchesWritten, resumed.GetProperty("batchesSkipped").GetInt64());
        Assert.Equal(0, resumed.GetProperty("rowsWritten").GetInt64());
        Assert.Equal(3, resumed.GetProperty("rowsSkipped").GetInt64());
        Assert.Equal(
            artifacts.ManifestDigest,
            resumed.GetProperty("sourcePackageManifestDigest").GetString());
        Assert.Equal(originalPackage, await File.ReadAllBytesAsync(artifacts.PackagePath, Cancellation));
        Assert.False(File.Exists(targetPath + ".migration.lock"));
        AssertNoCsvWorkspaceDirectories(workspace.Root);

        string validationPath = Path.Combine(workspace.Root, "validation.json");
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
        Assert.Contains("Status: PASSED", validationOutput.ToString(), StringComparison.Ordinal);
        Assert.Contains("Activation: activated", validationOutput.ToString(), StringComparison.Ordinal);
        MigrationValidationReport validation = MigrationValidationReportSerializer.Deserialize(
            await File.ReadAllTextAsync(validationPath, Cancellation));
        Assert.Equal(MigrationValidationStatus.Passed, validation.Outcome);
        Assert.Equal(MigrationValidationLevel.Checksum, validation.Level);
        Assert.Equal(originalPackage, await File.ReadAllBytesAsync(artifacts.PackagePath, Cancellation));
        Assert.False(File.Exists(targetPath + ".migration.lock"));
        Assert.Empty(Directory.EnumerateDirectories(workspace.Root, "csharpdb-validation-*"));
        AssertNoCsvWorkspaceDirectories(workspace.Root);

        await using var database = await Database.OpenAsync(targetPath, Cancellation);
        await using var query = await database.ExecuteAsync(
            "SELECT id, name FROM csv_data ORDER BY id;",
            Cancellation);
        var rows = await query.ToListAsync(Cancellation);
        Assert.Equal(3, rows.Count);
        Assert.Equal([1L, 2L, 3L], rows.Select(row => row[0].AsInteger));
        Assert.Equal(
            ["alpha", "bravo\nincorporated", "charlie"],
            rows.Select(row => row[1].AsText));
    }

    [Fact]
    public async Task SyntheticCommandsRejectCsvSourcePackageOptionsBeforeIo()
    {
        using var workspace = new TemporaryDirectory();
        string catalogPath = Path.Combine(workspace.Root, "synthetic-catalog.json");
        string planPath = Path.Combine(workspace.Root, "synthetic-plan.json");
        string unusedPackagePath = Path.Combine(workspace.Root, "unused.csdbcsv");
        string digest = "sha256:" + new string('0', 64);
        int inspectCode = await MigrationCommandRunner.RunAsync(
            ["migrate", "inspect", "--source", "synthetic", "--out", catalogPath],
            TextWriter.Null,
            TextWriter.Null,
            Cancellation);
        int planCode = await MigrationCommandRunner.RunAsync(
            [
                "migrate", "plan", catalogPath,
                "--out", planPath,
                "--accept-exclusions", "all",
            ],
            TextWriter.Null,
            TextWriter.Null,
            Cancellation);
        Assert.Equal(InspectorCommandRunner.ExitWarn, inspectCode);
        Assert.Equal(InspectorCommandRunner.ExitWarn, planCode);

        var inspectError = new StringWriter();
        int inspectWithCsvOptions = await MigrationCommandRunner.RunAsync(
            [
                "migrate", "inspect",
                "--source", "synthetic",
                "--input", Path.Combine(workspace.Root, "unused.csv"),
                "--package", unusedPackagePath,
                "--out", Path.Combine(workspace.Root, "unused-catalog.json"),
            ],
            TextWriter.Null,
            inspectError,
            Cancellation);
        Assert.Equal(InspectorCommandRunner.ExitUsage, inspectWithCsvOptions);
        Assert.Contains("Unknown option", inspectError.ToString(), StringComparison.Ordinal);

        string targetPath = Path.Combine(workspace.Root, "staged.csdb");
        string runPath = Path.Combine(workspace.Root, "run.json");
        var applyOutput = new StringWriter();
        var applyError = new StringWriter();
        int applyCode = await MigrationCommandRunner.RunAsync(
            [
                "migrate", "apply", planPath,
                "--catalog", catalogPath,
                "--source-package", unusedPackagePath,
                "--expected-manifest-digest", digest,
                "--target", targetPath,
                "--out", runPath,
            ],
            applyOutput,
            applyError,
            Cancellation);

        Assert.Equal(InspectorCommandRunner.ExitUsage, applyCode);
        Assert.True(string.IsNullOrWhiteSpace(applyOutput.ToString()));
        Assert.Contains("cannot be used with a synthetic migration", applyError.ToString(), StringComparison.Ordinal);
        AssertTargetWasNotCreated(targetPath, runPath);

        string validationPath = Path.Combine(workspace.Root, "validation.json");
        var validateError = new StringWriter();
        int validateCode = await MigrationCommandRunner.RunAsync(
            [
                "migrate", "validate", planPath,
                "--catalog", catalogPath,
                "--source-package", unusedPackagePath,
                "--expected-manifest-digest", digest,
                "--target", targetPath,
                "--out", validationPath,
            ],
            TextWriter.Null,
            validateError,
            Cancellation);

        Assert.Equal(InspectorCommandRunner.ExitUsage, validateCode);
        Assert.Contains("cannot be used with a synthetic migration", validateError.ToString(), StringComparison.Ordinal);
        AssertTargetWasNotCreated(targetPath, validationPath);
        Assert.False(File.Exists(unusedPackagePath));
    }

    private static async ValueTask<CsvArtifacts> CreateCsvApplyReadyArtifactsAsync(
        string directory,
        string csvContents = CsvContents,
        IReadOnlyList<string>? inspectSuffix = null,
        IReadOnlyList<string>? planSuffix = null)
    {
        string sourcePath = Path.Combine(directory, "orders.csv");
        string packagePath = Path.Combine(directory, "orders.csdbcsv");
        string catalogPath = Path.Combine(directory, "catalog.json");
        string planPath = Path.Combine(directory, "plan.json");
        await WriteCsvAsync(sourcePath, csvContents);
        var inspectOutput = new StringWriter();
        var inspectError = new StringWriter();

        var inspectArguments = new List<string>
        {
            "migrate", "inspect",
            "--source", "csv",
            "--input", sourcePath,
            "--package", packagePath,
            "--out", catalogPath,
            "--workspace", directory,
        };
        if (inspectSuffix is not null)
            inspectArguments.AddRange(inspectSuffix);
        int inspectCode = await MigrationCommandRunner.RunAsync(
            inspectArguments.ToArray(),
            inspectOutput,
            inspectError,
            Cancellation);
        Assert.True(
            inspectCode is InspectorCommandRunner.ExitOk or InspectorCommandRunner.ExitWarn,
            inspectError.ToString());
        Assert.True(string.IsNullOrWhiteSpace(inspectError.ToString()));
        string manifestDigest = ReadStatusField(inspectOutput.ToString(), "manifestDigest");
        AssertNoCsvWorkspaceDirectories(directory);

        var planArguments = new List<string>
        {
            "migrate", "plan", catalogPath,
            "--out", planPath,
            "--accept-exclusions", "all",
        };
        if (planSuffix is not null)
            planArguments.AddRange(planSuffix);
        var planError = new StringWriter();
        int planCode = await MigrationCommandRunner.RunAsync(
            planArguments.ToArray(),
            TextWriter.Null,
            planError,
            Cancellation);
        Assert.Equal(InspectorCommandRunner.ExitWarn, planCode);
        Assert.True(string.IsNullOrWhiteSpace(planError.ToString()));

        return new CsvArtifacts(
            sourcePath,
            packagePath,
            catalogPath,
            planPath,
            manifestDigest);
    }

    private static string[] DeterministicPlanPolicyArguments() =>
    [
        "--reject-mode", "deterministic",
        "--reject-rules", DeterministicRejectRules,
        "--max-rejected-rows-per-batch", "100",
        "--max-rejected-rows-per-run", "10000",
        "--max-reject-evidence-value-bytes", "4096",
        "--max-reject-evidence-bytes-per-batch", "65536",
        "--max-reject-evidence-bytes-per-run", "1048576",
        "--max-reject-artifact-bytes", "16777216",
    ];

    private static string[] PlanArguments(
        string catalogPath,
        string outputPath,
        IReadOnlyList<string> suffix)
    {
        var arguments = new List<string>
        {
            "migrate", "plan", catalogPath,
            "--out", outputPath,
            "--accept-exclusions", "all",
        };
        arguments.AddRange(suffix);
        return arguments.ToArray();
    }

    private static string[] RemoveOptionPair(
        IReadOnlyList<string> options,
        int pairIndex)
    {
        var result = new List<string>(options.Count - 2);
        for (int index = 0; index < options.Count; index += 2)
        {
            if (index / 2 != pairIndex)
            {
                result.Add(options[index]);
                result.Add(options[index + 1]);
            }
        }
        return result.ToArray();
    }

    private static void AssertDeterministicRunReport(
        JsonElement report,
        long rejectedRows,
        long rejectedRowsWritten,
        long rejectedRowsSkipped,
        long artifactBytes,
        bool artifactReused)
    {
        Assert.Equal("csharpdb-migration-run/v1", report.GetProperty("format").GetString());
        Assert.Equal("awaitingValidation", report.GetProperty("status").GetString());
        Assert.Equal(
            MigrationRejectContract.DeterministicRejectsV1,
            report.GetProperty("rejectContractVersion").GetString());
        Assert.Equal(rejectedRows, report.GetProperty("rejectedRows").GetInt64());
        Assert.Equal(rejectedRowsWritten, report.GetProperty("rejectedRowsWritten").GetInt64());
        Assert.Equal(rejectedRowsSkipped, report.GetProperty("rejectedRowsSkipped").GetInt64());
        Assert.Equal(
            MigrationRejectLedgerCodec.ArtifactFormat,
            report.GetProperty("rejectArtifactFormat").GetString());
        string digest = Assert.IsType<string>(
            report.GetProperty("rejectArtifactDigest").GetString());
        Assert.Equal(64, digest.Length);
        Assert.All(digest, character =>
            Assert.True(character is >= '0' and <= '9' or >= 'a' and <= 'f'));
        Assert.Equal(artifactBytes, report.GetProperty("rejectArtifactBytes").GetInt64());
        Assert.Equal(artifactReused, report.GetProperty("rejectArtifactReused").GetBoolean());
        Assert.False(report.TryGetProperty("rejectArtifactPath", out _));
        Assert.False(report.TryGetProperty("firstRejectedRow", out _));
    }

    private static async ValueTask<string> ReadLifecycleAsync(string targetPath)
    {
        await using Database database = await Database.OpenAsync(targetPath, Cancellation);
        await using var result = await database.ExecuteAsync(
            "SELECT \"lifecycle_state\" FROM \"__csharpdb_migration_state\" WHERE \"singleton\" = 1",
            Cancellation);
        Assert.True(await result.MoveNextAsync(Cancellation));
        string lifecycle = result.Current[0].AsText;
        Assert.False(await result.MoveNextAsync(Cancellation));
        return lifecycle;
    }

    private static async ValueTask WriteCsvAsync(
        string path,
        string contents = CsvContents) =>
        await File.WriteAllTextAsync(
            path,
            contents,
            new UTF8Encoding(encoderShouldEmitUTF8Identifier: false),
            Cancellation);

    private static string[] ApplyArguments(
        CsvArtifacts artifacts,
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

    private static string[] ValidateArguments(
        CsvArtifacts artifacts,
        string targetPath,
        string reportPath,
        IReadOnlyList<string> suffix)
    {
        var arguments = new List<string>
        {
            "migrate", "validate", artifacts.PlanPath,
            "--catalog", artifacts.CatalogPath,
            "--target", targetPath,
            "--out", reportPath,
        };
        arguments.AddRange(suffix);
        return arguments.ToArray();
    }

    private static string ReadStatusField(string output, string name)
    {
        string prefix = name + "=";
        string part = output
            .Split('|', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries)
            .Single(item => item.StartsWith(prefix, StringComparison.Ordinal));
        return part[prefix.Length..].Trim();
    }

    private static string DifferentDigest(string digest)
    {
        AssertCanonicalDigest(digest);
        char replacement = digest[7] == '0' ? '1' : '0';
        return digest[..7] + replacement + digest[8..];
    }

    private static void AssertCanonicalDigest(string digest)
    {
        Assert.StartsWith("sha256:", digest, StringComparison.Ordinal);
        Assert.Equal(71, digest.Length);
        Assert.All(digest.AsSpan(7).ToArray(), character =>
            Assert.True(character is >= '0' and <= '9' or >= 'a' and <= 'f'));
    }

    private static void AssertTargetWasNotCreated(string targetPath, string reportPath)
    {
        Assert.False(File.Exists(targetPath));
        Assert.False(File.Exists(targetPath + ".wal"));
        Assert.False(File.Exists(targetPath + ".migration.lock"));
        Assert.False(File.Exists(reportPath));
    }

    private static void AssertNoCsvWorkspaceDirectories(string directory) =>
        Assert.Empty(Directory.EnumerateDirectories(directory, "csharpdb-csv-*"));

    private static bool TryCreateDirectorySymbolicLink(string linkPath, string targetPath)
    {
        try
        {
            Directory.CreateSymbolicLink(linkPath, targetPath);
            return true;
        }
        catch (Exception exception) when (
            exception is PlatformNotSupportedException or
                UnauthorizedAccessException or
                IOException)
        {
            return false;
        }
    }

    private sealed record CsvArtifacts(
        string SourcePath,
        string PackagePath,
        string CatalogPath,
        string PlanPath,
        string ManifestDigest);

    private sealed class ThrowingTextWriter(string message) : TextWriter
    {
        public override Encoding Encoding => Encoding.UTF8;

        public override Task WriteLineAsync(string? value) =>
            Task.FromException(new IOException(message));
    }

    private sealed class TemporaryDirectory : IDisposable
    {
        public TemporaryDirectory()
        {
            Root = Path.Combine(
                Path.GetTempPath(),
                $"csharpdb-csv-cli-{Guid.NewGuid():N}");
            Directory.CreateDirectory(Root);
        }

        public string Root { get; }

        public void Dispose()
        {
            if (Directory.Exists(Root))
                Directory.Delete(Root, recursive: true);
        }
    }
}
