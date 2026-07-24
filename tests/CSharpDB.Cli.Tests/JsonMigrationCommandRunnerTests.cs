using System.Text;
using System.Text.Json;
using CSharpDB.Engine;
using CSharpDB.Migration;
using CSharpDB.Migration.Files.Json;
using CSharpDB.Migration.Validation;

namespace CSharpDB.Cli.Tests;

[Collection("CliConsole")]
public sealed class JsonMigrationCommandRunnerTests
{
    private const string RootArrayContents =
        """
        [
          {"id":1,"name":"alpha"},
          {"id":2,"name":"bravo"},
          {"id":3,"name":"charlie"}
        ]
        """;

    private const string NdjsonContents =
        """
        {"id":1,"name":"alpha"}
        {"id":2,"name":"bravo"}
        {"id":3,"name":"charlie"}

        """;

    private static readonly UTF8Encoding StrictUtf8 = new(false, true);

    private static CancellationToken Cancellation =>
        TestContext.Current.CancellationToken;

    public static TheoryData<string, string, int> InvalidInspectOptions =>
        new()
        {
            {
                "--framing",
                "objects",
                InspectorCommandRunner.ExitUsage
            },
            {
                "--sample-rows",
                "0",
                InspectorCommandRunner.ExitUsage
            },
            {
                "--sample-rows",
                "2147483648",
                InspectorCommandRunner.ExitUsage
            },
            {
                "--max-source-bytes",
                "0",
                InspectorCommandRunner.ExitError
            },
            {
                "--max-source-bytes",
                "not-a-number",
                InspectorCommandRunner.ExitUsage
            },
        };

    [Theory]
    [InlineData("root-array")]
    [InlineData("ndjson")]
    public async Task InspectApplyResumeAndValidateJson_SucceedsAfterRawInputDeletion(
        string framing)
    {
        using var workspace = new TemporaryDirectory();
        string sourcePath = workspace.PathFor(
            framing == "root-array" ? "orders.json" : "orders.ndjson");
        string packagePath = workspace.PathFor("orders.csdbjson");
        string catalogPath = workspace.PathFor("catalog.json");
        string planPath = workspace.PathFor("plan.json");
        await WriteJsonAsync(sourcePath, ContentsFor(framing));
        byte[] originalSource = await File.ReadAllBytesAsync(
            sourcePath,
            Cancellation);
        var inspectOutput = new StringWriter();
        var inspectError = new StringWriter();

        int inspectCode = await MigrationCommandRunner.RunAsync(
            [
                "migrate", "inspect",
                "--source", "json",
                "--input", sourcePath,
                "--package", packagePath,
                "--out", catalogPath,
                "--framing", framing,
                "--workspace", workspace.Root,
                "--max-source-bytes", "1048576",
            ],
            inspectOutput,
            inspectError,
            Cancellation);

        AssertSuccessful(inspectCode, inspectError);
        Assert.True(string.IsNullOrWhiteSpace(inspectError.ToString()));
        Assert.Contains("Status: OK", inspectOutput.ToString(), StringComparison.Ordinal);
        Assert.Contains(
            $"catalog={Path.GetFullPath(catalogPath)}",
            inspectOutput.ToString(),
            StringComparison.Ordinal);
        Assert.Contains(
            $"package={Path.GetFullPath(packagePath)}",
            inspectOutput.ToString(),
            StringComparison.Ordinal);
        Assert.Equal(
            originalSource,
            await File.ReadAllBytesAsync(sourcePath, Cancellation));
        Assert.True(File.Exists(packagePath));
        Assert.True(File.Exists(catalogPath));
        string manifestDigest = ReadStatusField(
            inspectOutput.ToString(),
            "manifestDigest");
        AssertCanonicalDigest(manifestDigest);

        MigrationCatalog catalog = MigrationArtifactSerializer.DeserializeCatalog(
            await File.ReadAllTextAsync(catalogPath, Cancellation));
        Assert.Equal(MigrationSourceKind.Json, catalog.Source.Kind);
        await using (JsonSnapshotPackageSession session =
                     await JsonSnapshotPackage.OpenAsync(
                         packagePath,
                         new JsonSnapshotPackageOpenOptions
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
                MigrationArtifactSerializer.SerializeCatalog(
                    catalog,
                    writeIndented: false),
                MigrationArtifactSerializer.SerializeCatalog(
                    session.Catalog,
                    writeIndented: false));
        }
        AssertNoJsonWorkspaceDirectories(workspace.Root);

        File.Delete(sourcePath);
        Assert.False(File.Exists(sourcePath));
        byte[] originalPackage = await File.ReadAllBytesAsync(
            packagePath,
            Cancellation);
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
        AssertSuccessful(planCode, planError);
        Assert.True(string.IsNullOrWhiteSpace(planError.ToString()));
        MigrationPlan plan = MigrationArtifactSerializer.DeserializePlan(
            await File.ReadAllTextAsync(planPath, Cancellation),
            catalog);

        string targetPath = workspace.PathFor("staged.csdb");
        string runPath = workspace.PathFor("run.json");
        var applyOutput = new StringWriter();
        var applyError = new StringWriter();
        int applyCode = await MigrationCommandRunner.RunAsync(
            ApplyArguments(
                planPath,
                catalogPath,
                targetPath,
                runPath,
                packagePath,
                manifestDigest,
                workspace.Root,
                resume: false),
            applyOutput,
            applyError,
            Cancellation);

        AssertSuccessful(applyCode, applyError);
        Assert.True(string.IsNullOrWhiteSpace(applyError.ToString()));
        Assert.False(File.Exists(sourcePath));
        Assert.Equal(
            originalPackage,
            await File.ReadAllBytesAsync(packagePath, Cancellation));
        Assert.True(File.Exists(targetPath));
        Assert.True(File.Exists(runPath));
        Assert.False(File.Exists(targetPath + ".migration.lock"));
        using JsonDocument applyStdout = JsonDocument.Parse(
            applyOutput.ToString());
        using JsonDocument applyReport = JsonDocument.Parse(
            await File.ReadAllTextAsync(runPath, Cancellation));
        JsonElement first = applyStdout.RootElement;
        Assert.Equal(first.GetRawText(), applyReport.RootElement.GetRawText());
        Assert.Equal(
            "csharpdb-migration-run/v1",
            first.GetProperty("format").GetString());
        Assert.Equal(
            "awaitingValidation",
            first.GetProperty("status").GetString());
        Assert.Equal(3, first.GetProperty("rowsWritten").GetInt64());
        Assert.Equal(0, first.GetProperty("rowsSkipped").GetInt64());
        Assert.True(first.GetProperty("batchesWritten").GetInt64() > 0);
        Assert.Equal(
            MigrationArtifactSerializer.ComputePlanDigest(plan),
            first.GetProperty("planDigest").GetString());
        Assert.Equal(
            MigrationArtifactSerializer.ComputeCatalogDigest(catalog),
            first.GetProperty("catalogDigest").GetString());
        Assert.Equal(
            JsonSnapshotPackage.Format,
            first.GetProperty("sourcePackageFormat").GetString());
        Assert.Equal(
            manifestDigest,
            first.GetProperty("sourcePackageManifestDigest").GetString());
        string targetIdentity = Assert.IsType<string>(
            first.GetProperty("targetIdentity").GetString());
        string sourceSnapshotIdentity = Assert.IsType<string>(
            first.GetProperty("sourceSnapshotIdentity").GetString());
        long batchesWritten = first.GetProperty("batchesWritten").GetInt64();
        AssertNoJsonWorkspaceDirectories(workspace.Root);

        string resumePath = workspace.PathFor("run-resume.json");
        var resumeOutput = new StringWriter();
        var resumeError = new StringWriter();
        int resumeCode = await MigrationCommandRunner.RunAsync(
            ApplyArguments(
                planPath,
                catalogPath,
                targetPath,
                resumePath,
                packagePath,
                manifestDigest,
                workspace.Root,
                resume: true),
            resumeOutput,
            resumeError,
            Cancellation);

        AssertSuccessful(resumeCode, resumeError);
        Assert.True(string.IsNullOrWhiteSpace(resumeError.ToString()));
        using JsonDocument resumeStdout = JsonDocument.Parse(
            resumeOutput.ToString());
        using JsonDocument resumeReport = JsonDocument.Parse(
            await File.ReadAllTextAsync(resumePath, Cancellation));
        JsonElement resumed = resumeStdout.RootElement;
        Assert.Equal(
            resumed.GetRawText(),
            resumeReport.RootElement.GetRawText());
        Assert.Equal(
            targetIdentity,
            resumed.GetProperty("targetIdentity").GetString());
        Assert.Equal(0, resumed.GetProperty("batchesWritten").GetInt64());
        Assert.Equal(
            batchesWritten,
            resumed.GetProperty("batchesSkipped").GetInt64());
        Assert.Equal(0, resumed.GetProperty("rowsWritten").GetInt64());
        Assert.Equal(3, resumed.GetProperty("rowsSkipped").GetInt64());
        Assert.Equal(
            JsonSnapshotPackage.Format,
            resumed.GetProperty("sourcePackageFormat").GetString());
        Assert.Equal(
            manifestDigest,
            resumed.GetProperty("sourcePackageManifestDigest").GetString());
        Assert.Equal(
            originalPackage,
            await File.ReadAllBytesAsync(packagePath, Cancellation));
        Assert.False(File.Exists(targetPath + ".migration.lock"));
        AssertNoJsonWorkspaceDirectories(workspace.Root);

        await AssertTargetRowsAsync(targetPath);
        Assert.Equal(
            "awaiting-validation",
            await ReadLifecycleAsync(targetPath));

        string validationPath = workspace.PathFor("validation.json");
        var validationOutput = new StringWriter();
        var validationError = new StringWriter();
        int validationCode = await MigrationCommandRunner.RunAsync(
            [
                "migrate", "validate", planPath,
                "--catalog", catalogPath,
                "--source-package", packagePath,
                "--expected-manifest-digest", manifestDigest,
                "--workspace", workspace.Root,
                "--max-source-bytes", "1048576",
                "--target", targetPath,
                "--out", validationPath,
                "--level", "checksum",
                "--spill-dir", workspace.Root,
            ],
            validationOutput,
            validationError,
            Cancellation);

        AssertSuccessful(validationCode, validationError);
        Assert.True(string.IsNullOrWhiteSpace(validationError.ToString()));
        Assert.Contains(
            "Status: PASSED",
            validationOutput.ToString(),
            StringComparison.Ordinal);
        Assert.Contains(
            "Activation: activated",
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
            MigrationArtifactSerializer.ComputePlanDigest(plan),
            validation.Binding.PlanDigest);
        Assert.Equal(
            MigrationArtifactSerializer.ComputeCatalogDigest(catalog),
            validation.Binding.CatalogDigest);
        Assert.Equal(
            plan.CapabilityDigest,
            validation.Binding.CapabilityDigest);
        Assert.Equal(
            catalog.Source.Identity,
            validation.Binding.SourceIdentity);
        Assert.Equal(
            catalog.Source.Fingerprint,
            validation.Binding.SourceFingerprint);
        Assert.Equal(
            sourceSnapshotIdentity,
            validation.Binding.SourceSnapshotIdentity);
        Assert.Equal(
            targetIdentity,
            validation.Binding.TargetIdentity);
        Assert.Equal(
            originalPackage,
            await File.ReadAllBytesAsync(packagePath, Cancellation));
        Assert.Equal("activated", await ReadLifecycleAsync(targetPath));
        Assert.Empty(Directory.EnumerateDirectories(
            workspace.Root,
            "csharpdb-validation-*"));
        AssertNoJsonWorkspaceDirectories(workspace.Root);
        await AssertTargetRowsAsync(targetPath);
    }

    [Theory]
    [MemberData(nameof(InvalidInspectOptions))]
    public async Task InspectJson_InvalidFramingOrLimitFailsClosedWithoutPublication(
        string option,
        string value,
        int expectedExitCode)
    {
        using var workspace = new TemporaryDirectory();
        string sourcePath = workspace.PathFor("orders.json");
        string packagePath = workspace.PathFor("orders.csdbjson");
        string catalogPath = workspace.PathFor("catalog.json");
        await WriteJsonAsync(sourcePath, RootArrayContents);
        byte[] originalSource = await File.ReadAllBytesAsync(
            sourcePath,
            Cancellation);
        var output = new StringWriter();
        var error = new StringWriter();

        int code = await MigrationCommandRunner.RunAsync(
            [
                "migrate", "inspect",
                "--source", "json",
                "--input", sourcePath,
                "--package", packagePath,
                "--out", catalogPath,
                option, value,
            ],
            output,
            error,
            Cancellation);

        Assert.Equal(expectedExitCode, code);
        Assert.True(string.IsNullOrWhiteSpace(output.ToString()));
        Assert.False(string.IsNullOrWhiteSpace(error.ToString()));
        Assert.Equal(
            originalSource,
            await File.ReadAllBytesAsync(sourcePath, Cancellation));
        Assert.False(File.Exists(packagePath));
        Assert.False(File.Exists(catalogPath));
        AssertNoJsonTemporaryFiles(workspace.Root);
        AssertNoJsonWorkspaceDirectories(workspace.Root);
    }

    [Fact]
    public async Task InspectJson_OmittedOptionsUseRootArrayJsonDataAndThousandRowSample()
    {
        using var workspace = new TemporaryDirectory();
        string sourcePath = workspace.PathFor("defaulted.json");
        string packagePath = workspace.PathFor("defaulted.csdbjson");
        string catalogPath = workspace.PathFor("defaulted-catalog.json");
        await WriteJsonAsync(
            sourcePath,
            BuildRootArray(recordCount: 1_001));
        var output = new StringWriter();
        var error = new StringWriter();

        int code = await MigrationCommandRunner.RunAsync(
            [
                "migrate", "inspect",
                "--source", "json",
                "--input", sourcePath,
                "--package", packagePath,
                "--out", catalogPath,
                "--workspace", workspace.Root,
                "--max-source-bytes", "1048576",
            ],
            output,
            error,
            Cancellation);

        AssertSuccessful(code, error);
        Assert.True(string.IsNullOrWhiteSpace(error.ToString()));
        string manifestDigest = ReadStatusField(
            output.ToString(),
            "manifestDigest");
        await using JsonSnapshotPackageSession session =
            await JsonSnapshotPackage.OpenAsync(
                packagePath,
                new JsonSnapshotPackageOpenOptions
                {
                    WorkspacePath = workspace.Root,
                    MaxSourceBytes = 1024 * 1024,
                    ExpectedManifestDigest = manifestDigest,
                },
                Cancellation);
        Assert.Equal("json_data", session.Schema.TableName);
        Assert.Equal(1_001, session.Schema.TotalRecords);
        Assert.Equal(1_000, session.Schema.ProfileRecordsExamined);
        Assert.True(session.Schema.ProfileRecordLimitReached);
        Assert.Equal(MigrationSourceKind.Json, session.Catalog.Source.Kind);
        AssertNoJsonTemporaryFiles(workspace.Root);
    }

    [Fact]
    public async Task InspectJson_CatalogPublicationFailurePreservesCompletedPackage()
    {
        using var workspace = new TemporaryDirectory();
        string sourcePath = workspace.PathFor("orders.json");
        string packagePath = workspace.PathFor("orders.csdbjson");
        string catalogDirectory = workspace.PathFor("catalog-output");
        string canaryPath = Path.Combine(
            catalogDirectory,
            "do-not-delete.txt");
        await WriteJsonAsync(sourcePath, RootArrayContents);
        Directory.CreateDirectory(catalogDirectory);
        await File.WriteAllTextAsync(
            canaryPath,
            "preserve this directory",
            StrictUtf8,
            Cancellation);
        var output = new StringWriter();
        var error = new StringWriter();

        int code = await MigrationCommandRunner.RunAsync(
            [
                "migrate", "inspect",
                "--source", "json",
                "--input", sourcePath,
                "--package", packagePath,
                "--out", catalogDirectory,
                "--workspace", workspace.Root,
                "--max-source-bytes", "1048576",
            ],
            output,
            error,
            Cancellation);

        Assert.Equal(InspectorCommandRunner.ExitError, code);
        Assert.True(string.IsNullOrWhiteSpace(output.ToString()));
        Assert.Contains(
            "Catalog publication failed",
            error.ToString(),
            StringComparison.Ordinal);
        Assert.Contains(
            "package was preserved",
            error.ToString(),
            StringComparison.OrdinalIgnoreCase);
        Assert.Contains(
            Path.GetFullPath(packagePath),
            error.ToString(),
            StringComparison.Ordinal);
        Assert.True(File.Exists(packagePath));
        Assert.True(Directory.Exists(catalogDirectory));
        Assert.Equal(
            "preserve this directory",
            await File.ReadAllTextAsync(canaryPath, Cancellation));
        byte[] publishedPackage = await File.ReadAllBytesAsync(
            packagePath,
            Cancellation);

        await using (JsonSnapshotPackageSession session =
                     await JsonSnapshotPackage.OpenAsync(
                         packagePath,
                         new JsonSnapshotPackageOpenOptions
                         {
                             WorkspacePath = workspace.Root,
                             MaxSourceBytes = 1024 * 1024,
                         },
                         Cancellation))
        {
            AssertCanonicalDigest(session.Manifest.ManifestDigest);
            Assert.Equal(MigrationSourceKind.Json, session.Catalog.Source.Kind);
            Assert.Equal("json_data", session.Schema.TableName);
            Assert.Equal(3, session.Schema.TotalRecords);
            Assert.Equal(
                MigrationArtifactSerializer.ComputeCatalogDigest(
                    session.Catalog),
                session.Manifest.CatalogDigest);
        }

        Assert.Equal(
            publishedPackage,
            await File.ReadAllBytesAsync(packagePath, Cancellation));
        AssertNoJsonTemporaryFiles(workspace.Root);
        AssertNoJsonWorkspaceDirectories(workspace.Root);
    }

    [Fact]
    public async Task ApplyJson_MissingPackageOrDigestReturnsUsageBeforeTargetCreation()
    {
        using var workspace = new TemporaryDirectory();
        JsonArtifacts artifacts = await CreateApplyReadyArtifactsAsync(
            workspace.Root,
            "root-array",
            "orders");
        string targetPath = workspace.PathFor("staged.csdb");
        string reportPath = workspace.PathFor("run.json");
        string[][] suffixes =
        [
            [
                "--expected-manifest-digest",
                artifacts.ManifestDigest,
            ],
            [
                "--source-package",
                artifacts.PackagePath,
            ],
        ];
        string[] expectedMessages =
        [
            "Missing required option --source-package",
            "Missing required option --expected-manifest-digest",
        ];

        for (int index = 0; index < suffixes.Length; index++)
        {
            var output = new StringWriter();
            var error = new StringWriter();
            int code = await MigrationCommandRunner.RunAsync(
                ApplyArguments(
                    artifacts,
                    targetPath,
                    reportPath,
                    suffixes[index]),
                output,
                error,
                Cancellation);

            Assert.Equal(InspectorCommandRunner.ExitUsage, code);
            Assert.True(string.IsNullOrWhiteSpace(output.ToString()));
            Assert.Contains(
                expectedMessages[index],
                error.ToString(),
                StringComparison.Ordinal);
            AssertTargetWasNotCreated(targetPath, reportPath);
        }
    }

    [Fact]
    public async Task ApplyJson_WrongManifestPinFailsBeforeTargetCreation()
    {
        using var workspace = new TemporaryDirectory();
        JsonArtifacts artifacts = await CreateApplyReadyArtifactsAsync(
            workspace.Root,
            "root-array",
            "orders");
        string targetPath = workspace.PathFor("staged.csdb");
        string reportPath = workspace.PathFor("run.json");
        byte[] originalPackage = await File.ReadAllBytesAsync(
            artifacts.PackagePath,
            Cancellation);
        var output = new StringWriter();
        var error = new StringWriter();

        int code = await MigrationCommandRunner.RunAsync(
            ApplyArguments(
                artifacts,
                targetPath,
                reportPath,
                [
                    "--source-package", artifacts.PackagePath,
                    "--expected-manifest-digest",
                    DifferentDigest(artifacts.ManifestDigest),
                    "--workspace", workspace.Root,
                ]),
            output,
            error,
            Cancellation);

        Assert.Equal(InspectorCommandRunner.ExitError, code);
        Assert.True(string.IsNullOrWhiteSpace(output.ToString()));
        Assert.Contains(
            JsonSnapshotPackageRules.IntegrityMismatch,
            error.ToString(),
            StringComparison.Ordinal);
        Assert.Contains(
            "trusted manifest digest",
            error.ToString(),
            StringComparison.OrdinalIgnoreCase);
        Assert.Equal(
            originalPackage,
            await File.ReadAllBytesAsync(
                artifacts.PackagePath,
                Cancellation));
        AssertTargetWasNotCreated(targetPath, reportPath);
        AssertNoJsonWorkspaceDirectories(workspace.Root);
    }

    [Fact]
    public async Task ApplyJson_DifferentValidPackageFailsCatalogBindingBeforeTargetCreation()
    {
        using var workspace = new TemporaryDirectory();
        string firstDirectory = workspace.PathFor("first");
        string secondDirectory = workspace.PathFor("second");
        Directory.CreateDirectory(firstDirectory);
        Directory.CreateDirectory(secondDirectory);
        JsonArtifacts first = await CreateApplyReadyArtifactsAsync(
            firstDirectory,
            "root-array",
            "orders");
        JsonArtifacts second = await CreateApplyReadyArtifactsAsync(
            secondDirectory,
            "root-array",
            "orders",
            """
            [
              {"id":10,"name":"delta"},
              {"id":20,"name":"echo"},
              {"id":30,"name":"foxtrot"}
            ]
            """);
        Assert.NotEqual(first.ManifestDigest, second.ManifestDigest);
        byte[] originalSecondPackage = await File.ReadAllBytesAsync(
            second.PackagePath,
            Cancellation);
        string targetPath = workspace.PathFor("staged.csdb");
        string reportPath = workspace.PathFor("run.json");
        var output = new StringWriter();
        var error = new StringWriter();

        int code = await MigrationCommandRunner.RunAsync(
            ApplyArguments(
                first,
                targetPath,
                reportPath,
                [
                    "--source-package", second.PackagePath,
                    "--expected-manifest-digest",
                    second.ManifestDigest,
                    "--workspace", workspace.Root,
                ]),
            output,
            error,
            Cancellation);

        Assert.Equal(InspectorCommandRunner.ExitError, code);
        Assert.True(string.IsNullOrWhiteSpace(output.ToString()));
        Assert.Contains(
            "retained JSON package catalog does not match",
            error.ToString(),
            StringComparison.OrdinalIgnoreCase);
        Assert.Equal(
            originalSecondPackage,
            await File.ReadAllBytesAsync(
                second.PackagePath,
                Cancellation));
        AssertTargetWasNotCreated(targetPath, reportPath);
        AssertNoJsonWorkspaceDirectories(workspace.Root);
    }

    [Fact]
    public async Task ApplyJson_TypedV2PackageIsRejectedByUntypedV1Route()
    {
        using var workspace = new TemporaryDirectory();
        string v1Directory = workspace.PathFor("v1");
        string v2Directory = workspace.PathFor("v2");
        Directory.CreateDirectory(v1Directory);
        Directory.CreateDirectory(v2Directory);
        JsonArtifacts v1 = await CreateApplyReadyArtifactsAsync(
            v1Directory,
            "root-array",
            "orders");
        TypedPackage typed = await CreateTypedPackageAsync(v2Directory);
        byte[] originalTypedPackage = await File.ReadAllBytesAsync(
            typed.PackagePath,
            Cancellation);
        string targetPath = workspace.PathFor("staged.csdb");
        string reportPath = workspace.PathFor("run.json");
        var output = new StringWriter();
        var error = new StringWriter();

        int code = await MigrationCommandRunner.RunAsync(
            ApplyArguments(
                v1,
                targetPath,
                reportPath,
                [
                    "--source-package", typed.PackagePath,
                    "--expected-manifest-digest",
                    typed.ManifestDigest,
                    "--workspace", workspace.Root,
                ]),
            output,
            error,
            Cancellation);

        Assert.Equal(InspectorCommandRunner.ExitError, code);
        Assert.True(string.IsNullOrWhiteSpace(output.ToString()));
        Assert.Contains(
            JsonSnapshotPackageRules.InvalidFormat,
            error.ToString(),
            StringComparison.Ordinal);
        Assert.Equal(
            originalTypedPackage,
            await File.ReadAllBytesAsync(
                typed.PackagePath,
                Cancellation));
        AssertTargetWasNotCreated(targetPath, reportPath);
        AssertNoJsonWorkspaceDirectories(workspace.Root);
    }

    [Fact]
    public async Task PlanJson_TypedV2CatalogIsRejectedBeforePlanPublication()
    {
        using var workspace = new TemporaryDirectory();
        TypedPackage typed = await CreateTypedPackageAsync(workspace.Root);
        string catalogPath = workspace.PathFor("typed-catalog.json");
        string planPath = workspace.PathFor("typed-plan.json");
        await File.WriteAllTextAsync(
            catalogPath,
            MigrationArtifactSerializer.SerializeCatalog(typed.Catalog),
            StrictUtf8,
            Cancellation);
        byte[] originalTypedPackage = await File.ReadAllBytesAsync(
            typed.PackagePath,
            Cancellation);
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

        Assert.Equal(InspectorCommandRunner.ExitUsage, planCode);
        Assert.True(string.IsNullOrWhiteSpace(planOutput.ToString()));
        Assert.Contains(
            "untyped retained JSON package v1",
            planError.ToString(),
            StringComparison.OrdinalIgnoreCase);
        Assert.False(File.Exists(planPath));
        Assert.Equal(
            originalTypedPackage,
            await File.ReadAllBytesAsync(
                typed.PackagePath,
                Cancellation));
        AssertNoJsonWorkspaceDirectories(workspace.Root);
    }

    [Fact]
    public async Task PlanJson_DeterministicRejectModeIsRejectedWithoutPublication()
    {
        using var workspace = new TemporaryDirectory();
        JsonArtifacts artifacts = await CreateApplyReadyArtifactsAsync(
            workspace.Root,
            "root-array",
            "orders");
        string planPath = workspace.PathFor("deterministic-plan.json");
        var output = new StringWriter();
        var error = new StringWriter();

        int code = await MigrationCommandRunner.RunAsync(
            [
                "migrate", "plan", artifacts.CatalogPath,
                "--out", planPath,
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
            output,
            error,
            Cancellation);

        Assert.Equal(InspectorCommandRunner.ExitUsage, code);
        Assert.True(string.IsNullOrWhiteSpace(output.ToString()));
        Assert.Contains(
            "only for retained CSV migrations",
            error.ToString(),
            StringComparison.OrdinalIgnoreCase);
        Assert.False(File.Exists(planPath));
        AssertNoJsonWorkspaceDirectories(workspace.Root);
    }

    [Fact]
    public async Task JsonCommands_RejectPathAndWorkspaceCollisionsBeforeMutation()
    {
        using var workspace = new TemporaryDirectory();
        string sourcePath = workspace.PathFor("orders.json");
        string packagePath = workspace.PathFor("orders.csdbjson");
        string catalogPath = workspace.PathFor("catalog.json");
        await WriteJsonAsync(sourcePath, RootArrayContents);
        byte[] originalSource = await File.ReadAllBytesAsync(
            sourcePath,
            Cancellation);
        (string Package, string Catalog)[] inspectCollisions =
        [
            (sourcePath, catalogPath),
            (packagePath, sourcePath),
            (packagePath, packagePath),
        ];

        foreach ((string package, string catalog) in inspectCollisions)
        {
            var output = new StringWriter();
            var error = new StringWriter();
            int code = await MigrationCommandRunner.RunAsync(
                [
                    "migrate", "inspect",
                    "--source", "json",
                    "--input", sourcePath,
                    "--package", package,
                    "--out", catalog,
                ],
                output,
                error,
                Cancellation);

            Assert.Equal(InspectorCommandRunner.ExitUsage, code);
            Assert.True(string.IsNullOrWhiteSpace(output.ToString()));
            Assert.Contains(
                "different files",
                error.ToString(),
                StringComparison.OrdinalIgnoreCase);
            Assert.Equal(
                originalSource,
                await File.ReadAllBytesAsync(
                    sourcePath,
                    Cancellation));
        }
        Assert.False(File.Exists(packagePath));
        Assert.False(File.Exists(catalogPath));
        AssertNoJsonTemporaryFiles(workspace.Root);
        AssertNoJsonWorkspaceDirectories(workspace.Root);

        string artifactsDirectory = workspace.PathFor("artifacts");
        Directory.CreateDirectory(artifactsDirectory);
        JsonArtifacts artifacts = await CreateApplyReadyArtifactsAsync(
            artifactsDirectory,
            "root-array",
            "retained");
        byte[] originalPackage = await File.ReadAllBytesAsync(
            artifacts.PackagePath,
            Cancellation);
        string collidingReportPath = workspace.PathFor(
            "colliding-run.json");
        var collisionOutput = new StringWriter();
        var collisionError = new StringWriter();
        int collisionCode = await MigrationCommandRunner.RunAsync(
            ApplyArguments(
                artifacts,
                artifacts.PackagePath,
                collidingReportPath,
                [
                    "--source-package", artifacts.PackagePath,
                    "--expected-manifest-digest",
                    artifacts.ManifestDigest,
                    "--workspace", workspace.Root,
                ]),
            collisionOutput,
            collisionError,
            Cancellation);

        Assert.Equal(InspectorCommandRunner.ExitUsage, collisionCode);
        Assert.True(string.IsNullOrWhiteSpace(collisionOutput.ToString()));
        Assert.Contains(
            "different files",
            collisionError.ToString(),
            StringComparison.OrdinalIgnoreCase);
        Assert.Equal(
            originalPackage,
            await File.ReadAllBytesAsync(
                artifacts.PackagePath,
                Cancellation));
        Assert.False(File.Exists(collidingReportPath));
        Assert.False(File.Exists(
            artifacts.PackagePath + ".migration.lock"));

        string targetPath = workspace.PathFor("future-target.csdb");
        string reportPath = workspace.PathFor("future-run.json");
        var workspaceOutput = new StringWriter();
        var workspaceError = new StringWriter();
        int workspaceCode = await MigrationCommandRunner.RunAsync(
            ApplyArguments(
                artifacts,
                targetPath,
                reportPath,
                [
                    "--source-package", artifacts.PackagePath,
                    "--expected-manifest-digest",
                    artifacts.ManifestDigest,
                    "--workspace", targetPath,
                ]),
            workspaceOutput,
            workspaceError,
            Cancellation);

        Assert.Equal(InspectorCommandRunner.ExitUsage, workspaceCode);
        Assert.True(string.IsNullOrWhiteSpace(workspaceOutput.ToString()));
        Assert.Contains(
            "workspace",
            workspaceError.ToString(),
            StringComparison.OrdinalIgnoreCase);
        AssertTargetWasNotCreated(targetPath, reportPath);
        Assert.False(Directory.Exists(targetPath));
        Assert.Equal(
            originalPackage,
            await File.ReadAllBytesAsync(
                artifacts.PackagePath,
                Cancellation));
        AssertNoJsonWorkspaceDirectories(workspace.Root);
    }

    private static async ValueTask<JsonArtifacts>
        CreateApplyReadyArtifactsAsync(
            string directory,
            string framing,
            string stem,
            string? contents = null)
    {
        string sourcePath = Path.Combine(
            directory,
            stem + (framing == "ndjson" ? ".ndjson" : ".json"));
        string packagePath = Path.Combine(
            directory,
            stem + JsonSnapshotPackage.FileExtension);
        string catalogPath = Path.Combine(
            directory,
            stem + "-catalog.json");
        string planPath = Path.Combine(
            directory,
            stem + "-plan.json");
        await WriteJsonAsync(
            sourcePath,
            contents ?? ContentsFor(framing));
        var inspectOutput = new StringWriter();
        var inspectError = new StringWriter();
        int inspectCode = await MigrationCommandRunner.RunAsync(
            [
                "migrate", "inspect",
                "--source", "json",
                "--input", sourcePath,
                "--package", packagePath,
                "--out", catalogPath,
                "--framing", framing,
                "--workspace", directory,
                "--max-source-bytes", "1048576",
            ],
            inspectOutput,
            inspectError,
            Cancellation);
        AssertSuccessful(inspectCode, inspectError);
        Assert.True(
            string.IsNullOrWhiteSpace(inspectError.ToString()),
            inspectError.ToString());
        string manifestDigest = ReadStatusField(
            inspectOutput.ToString(),
            "manifestDigest");
        AssertCanonicalDigest(manifestDigest);
        AssertNoJsonWorkspaceDirectories(directory);

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
        AssertSuccessful(planCode, planError);
        Assert.True(
            string.IsNullOrWhiteSpace(planError.ToString()),
            planError.ToString());

        return new JsonArtifacts(
            sourcePath,
            packagePath,
            catalogPath,
            planPath,
            manifestDigest);
    }

    private static async ValueTask<TypedPackage> CreateTypedPackageAsync(
        string directory)
    {
        string sourcePath = Path.Combine(directory, "typed.json");
        string sidecarPath = Path.Combine(
            directory,
            "typed" + JsonTypedIntentSidecar.FileExtension);
        string packagePath = Path.Combine(
            directory,
            "typed" + JsonTypedSnapshotPackage.FileExtension);
        await WriteJsonAsync(
            sourcePath,
            """[{"value":"1"},{"value":"2"}]""");
        await using JsonSourceSnapshot snapshot =
            await JsonSourceSnapshot.CreateFromFileAsync(
                sourcePath,
                new JsonSourceSnapshotOptions
                {
                    WorkspacePath = directory,
                    MaxSourceBytes = 1024 * 1024,
                },
                Cancellation);
        JsonSourceBinding binding = await JsonSourceBinding.CreateAsync(
            snapshot,
            logicalSourceIdentity: "typed-v2-cli-rejection",
            cancellationToken: Cancellation);
        JsonTypedIntentManifest intent =
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
                        },
                    ],
                },
                Cancellation);
        JsonTypedTableSchemaInferenceResult schema =
            await JsonTypedTableSchemaInferer.InferAsync(
                binding,
                snapshot,
                intent,
                maxProfileRecords: 100,
                cancellationToken: Cancellation);
        JsonTypedSnapshotPackageManifest manifest =
            await JsonTypedSnapshotPackage.WriteAsync(
                packagePath,
                snapshot,
                schema,
                CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
                Cancellation);
        AssertCanonicalDigest(manifest.ManifestDigest);
        return new TypedPackage(
            packagePath,
            manifest.ManifestDigest,
            schema.CreateCatalog(
                CSharpDbCapabilityCatalogLoader.CurrentTargetVersion));
    }

    private static string[] ApplyArguments(
        string planPath,
        string catalogPath,
        string targetPath,
        string reportPath,
        string packagePath,
        string manifestDigest,
        string workspacePath,
        bool resume)
    {
        var arguments = new List<string>
        {
            "migrate", "apply", planPath,
            "--catalog", catalogPath,
            "--source-package", packagePath,
            "--expected-manifest-digest", manifestDigest,
            "--workspace", workspacePath,
            "--max-source-bytes", "1048576",
            "--target", targetPath,
            "--out", reportPath,
            "--format", "json",
        };
        if (resume)
            arguments.Add("--resume");
        return arguments.ToArray();
    }

    private static string[] ApplyArguments(
        JsonArtifacts artifacts,
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

    private static async ValueTask AssertTargetRowsAsync(
        string targetPath)
    {
        await using Database database =
            await Database.OpenAsync(targetPath, Cancellation);
        await using var query = await database.ExecuteAsync(
            "SELECT id, name FROM json_data ORDER BY id;",
            Cancellation);
        var rows = await query.ToListAsync(Cancellation);
        Assert.Equal(3, rows.Count);
        Assert.Equal(
            [1L, 2L, 3L],
            rows.Select(row => row[0].AsInteger));
        Assert.Equal(
            ["alpha", "bravo", "charlie"],
            rows.Select(row => row[1].AsText));
    }

    private static async ValueTask<string> ReadLifecycleAsync(
        string targetPath)
    {
        await using Database database =
            await Database.OpenAsync(targetPath, Cancellation);
        await using var result = await database.ExecuteAsync(
            "SELECT \"lifecycle_state\" FROM \"__csharpdb_migration_state\" WHERE \"singleton\" = 1",
            Cancellation);
        Assert.True(await result.MoveNextAsync(Cancellation));
        string lifecycle = result.Current[0].AsText;
        Assert.False(await result.MoveNextAsync(Cancellation));
        return lifecycle;
    }

    private static string ContentsFor(string framing) =>
        framing switch
        {
            "root-array" => RootArrayContents,
            "ndjson" => NdjsonContents,
            _ => throw new ArgumentOutOfRangeException(nameof(framing)),
        };

    private static string BuildRootArray(int recordCount)
    {
        var json = new StringBuilder(recordCount * 12);
        json.Append('[');
        for (int index = 0; index < recordCount; index++)
        {
            if (index > 0)
                json.Append(',');
            json.Append("{\"id\":");
            json.Append(index);
            json.Append('}');
        }
        json.Append(']');
        return json.ToString();
    }

    private static async ValueTask WriteJsonAsync(
        string path,
        string contents) =>
        await File.WriteAllTextAsync(
            path,
            contents,
            StrictUtf8,
            Cancellation);

    private static void AssertSuccessful(
        int code,
        StringWriter error) =>
        Assert.True(
            code is InspectorCommandRunner.ExitOk or
                InspectorCommandRunner.ExitWarn,
            error.ToString());

    private static string ReadStatusField(
        string output,
        string name)
    {
        string prefix = name + "=";
        string part = output
            .Split(
                '|',
                StringSplitOptions.TrimEntries |
                StringSplitOptions.RemoveEmptyEntries)
            .Single(item => item.StartsWith(
                prefix,
                StringComparison.Ordinal));
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
        Assert.StartsWith(
            "sha256:",
            digest,
            StringComparison.Ordinal);
        Assert.Equal(71, digest.Length);
        Assert.All(
            digest.AsSpan(7).ToArray(),
            character => Assert.True(
                character is >= '0' and <= '9' or
                    >= 'a' and <= 'f'));
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

    private static void AssertNoJsonTemporaryFiles(
        string directory)
    {
        Assert.Empty(Directory.EnumerateFiles(
            directory,
            ".csdbjson-*.tmp"));
        Assert.Empty(Directory.EnumerateFiles(
            directory,
            ".csharpdb-migration-*.tmp"));
    }

    private static void AssertNoJsonWorkspaceDirectories(
        string directory) =>
        Assert.Empty(Directory.EnumerateDirectories(
            directory,
            "csharpdb-json-*"));

    private sealed record JsonArtifacts(
        string SourcePath,
        string PackagePath,
        string CatalogPath,
        string PlanPath,
        string ManifestDigest);

    private sealed record TypedPackage(
        string PackagePath,
        string ManifestDigest,
        MigrationCatalog Catalog);

    private sealed class TemporaryDirectory : IDisposable
    {
        public TemporaryDirectory()
        {
            Root = Path.Combine(
                Path.GetTempPath(),
                "csharpdb-json-cli-" +
                Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(Root);
        }

        public string Root { get; }

        public string PathFor(string name) =>
            Path.Combine(Root, name);

        public void Dispose()
        {
            if (Directory.Exists(Root))
                Directory.Delete(Root, recursive: true);
        }
    }
}
