using System.Text;
using System.Text.Json;
using CSharpDB.Engine;
using CSharpDB.Migration;
using CSharpDB.Migration.Files.Json;
using CSharpDB.Migration.Validation;
using PrimitiveDbType = CSharpDB.Primitives.DbType;

namespace CSharpDB.Cli.Tests;

[Collection("CliConsole")]
public sealed class TypedJsonMigrationCommandRunnerTests
{
    private const string PrivateSourceValue =
        "authorization: bearer typed-cli-private-value";

    private const string AllCodecObject =
        """
        {
          "binary":"AQIDBA==",
          "decimalString":"12345678901234567890.123456789012345678",
          "decimalNumber":123.45,
          "guid":"00112233-4455-6677-8899-aabbccddeeff",
          "date":"2024-02-29",
          "time":"08:09:10.1234567",
          "dateTime":"2026-07-23 08:09:10.1234567",
          "dateTimeOffset":"2026-07-23 08:09:10.1234567-07:00",
          "int64":"-9223372036854775808",
          "uint64":"18446744073709551615",
          "ordinary":"ordinary"
        }
        """;

    private static readonly UTF8Encoding StrictUtf8 =
        new(false, true);

    private static CancellationToken Cancellation =>
        TestContext.Current.CancellationToken;

    [Theory]
    [InlineData("root-array")]
    [InlineData("ndjson")]
    public async Task
        InspectApplyResumeAndValidateTypedJson_SucceedsAfterRawAndSidecarDeletion(
            string framing)
    {
        using var workspace = new TemporaryDirectory();
        TypedArtifacts artifacts =
            await CreateTypedArtifactsAsync(
                workspace.Root,
                "all-codecs-" + framing,
                framing,
                Frame(framing, AllCodecObject),
                AllCodecOptions());
        byte[] originalSource =
            await File.ReadAllBytesAsync(
                artifacts.SourcePath,
                Cancellation);
        byte[] originalSidecar =
            await File.ReadAllBytesAsync(
                artifacts.SidecarPath,
                Cancellation);
        byte[] originalPackage =
            await File.ReadAllBytesAsync(
                artifacts.PackagePath,
                Cancellation);

        Assert.NotEmpty(originalSource);
        Assert.NotEmpty(originalSidecar);
        await using (JsonTypedSnapshotPackageSession session =
            await JsonTypedSnapshotPackage.OpenAsync(
                artifacts.PackagePath,
                new JsonSnapshotPackageOpenOptions
                {
                    WorkspacePath = workspace.Root,
                    MaxSourceBytes = 1024 * 1024,
                    ExpectedManifestDigest =
                        artifacts.ManifestDigest,
                },
                Cancellation))
        {
            Assert.Equal(
                artifacts.ManifestDigest,
                session.Manifest.ManifestDigest);
            Assert.Equal(
                artifacts.IntentManifestDigest,
                session.Manifest.IntentManifestDigest);
            Assert.Equal(
                artifacts.IntentManifestDigest,
                session.IntentManifest.ManifestDigest);
            Assert.Equal(
                MigrationArtifactSerializer.ComputeCatalogDigest(
                    artifacts.Catalog),
                session.Manifest.CatalogDigest);
            Assert.Equal(
                session.Manifest.CatalogDigest,
                session.DataSource.CatalogDigest);
            Assert.Equal(
                MigrationArtifactSerializer.SerializeCatalog(
                    artifacts.Catalog,
                    writeIndented: false),
                MigrationArtifactSerializer.SerializeCatalog(
                    session.Catalog,
                    writeIndented: false));
            Assert.All(
                session.Schema.Columns.Take(10),
                column => Assert.NotNull(column.Intent));
            Assert.Null(session.Schema.Columns[10].Intent);
        }
        AssertNoJsonWorkspaceDirectories(workspace.Root);

        File.Delete(artifacts.SourcePath);
        File.Delete(artifacts.SidecarPath);
        Assert.False(File.Exists(artifacts.SourcePath));
        Assert.False(File.Exists(artifacts.SidecarPath));

        MigrationPlan plan = await PublishPlanAsync(artifacts);
        Assert.False(File.Exists(artifacts.SourcePath));
        Assert.False(File.Exists(artifacts.SidecarPath));

        string targetPath = workspace.PathFor("staged.csdb");
        string runPath = workspace.PathFor("run.json");
        var applyOutput = new StringWriter();
        var applyError = new StringWriter();
        int applyCode = await MigrationCommandRunner.RunAsync(
            ApplyArguments(
                artifacts,
                targetPath,
                runPath,
                workspace.Root,
                resume: false),
            applyOutput,
            applyError,
            Cancellation);

        AssertSuccessful(applyCode, applyError);
        Assert.True(
            string.IsNullOrWhiteSpace(applyError.ToString()),
            applyError.ToString());
        Assert.False(File.Exists(artifacts.SourcePath));
        Assert.False(File.Exists(artifacts.SidecarPath));
        Assert.Equal(
            originalPackage,
            await File.ReadAllBytesAsync(
                artifacts.PackagePath,
                Cancellation));
        Assert.True(File.Exists(targetPath));
        Assert.True(File.Exists(runPath));
        Assert.False(File.Exists(targetPath + ".migration.lock"));
        using JsonDocument applyStdout =
            JsonDocument.Parse(applyOutput.ToString());
        using JsonDocument applyReport =
            JsonDocument.Parse(
                await File.ReadAllTextAsync(
                    runPath,
                    Cancellation));
        JsonElement applied = applyStdout.RootElement;
        Assert.Equal(
            applied.GetRawText(),
            applyReport.RootElement.GetRawText());
        Assert.Equal(
            "awaitingValidation",
            applied.GetProperty("status").GetString());
        Assert.Equal(
            JsonTypedSnapshotPackage.Format,
            applied.GetProperty("sourcePackageFormat")
                .GetString());
        Assert.Equal(
            artifacts.ManifestDigest,
            applied.GetProperty(
                    "sourcePackageManifestDigest")
                .GetString());
        Assert.Equal(
            artifacts.IntentManifestDigest,
            applied.GetProperty(
                    "sourcePackageIntentManifestDigest")
                .GetString());
        Assert.Equal(
            MigrationArtifactSerializer.ComputePlanDigest(plan),
            applied.GetProperty("planDigest").GetString());
        Assert.Equal(
            MigrationArtifactSerializer.ComputeCatalogDigest(
                artifacts.Catalog),
            applied.GetProperty("catalogDigest").GetString());
        Assert.Equal(
            1,
            applied.GetProperty("rowsWritten").GetInt64());
        Assert.Equal(
            0,
            applied.GetProperty("rowsSkipped").GetInt64());
        Assert.True(
            applied.GetProperty("batchesWritten").GetInt64() >
            0);
        string targetIdentity = Assert.IsType<string>(
            applied.GetProperty("targetIdentity").GetString());
        string sourceSnapshotIdentity = Assert.IsType<string>(
            applied.GetProperty("sourceSnapshotIdentity")
                .GetString());
        long batchesWritten =
            applied.GetProperty("batchesWritten").GetInt64();
        AssertNoJsonWorkspaceDirectories(workspace.Root);

        string resumePath = workspace.PathFor("run-resume.json");
        var resumeOutput = new StringWriter();
        var resumeError = new StringWriter();
        int resumeCode = await MigrationCommandRunner.RunAsync(
            ApplyArguments(
                artifacts,
                targetPath,
                resumePath,
                workspace.Root,
                resume: true),
            resumeOutput,
            resumeError,
            Cancellation);

        AssertSuccessful(resumeCode, resumeError);
        Assert.True(
            string.IsNullOrWhiteSpace(resumeError.ToString()),
            resumeError.ToString());
        using JsonDocument resumeStdout =
            JsonDocument.Parse(resumeOutput.ToString());
        using JsonDocument resumeReport =
            JsonDocument.Parse(
                await File.ReadAllTextAsync(
                    resumePath,
                    Cancellation));
        JsonElement resumed = resumeStdout.RootElement;
        Assert.Equal(
            resumed.GetRawText(),
            resumeReport.RootElement.GetRawText());
        Assert.Equal(
            targetIdentity,
            resumed.GetProperty("targetIdentity").GetString());
        Assert.Equal(
            0,
            resumed.GetProperty("rowsWritten").GetInt64());
        Assert.Equal(
            1,
            resumed.GetProperty("rowsSkipped").GetInt64());
        Assert.Equal(
            0,
            resumed.GetProperty("batchesWritten").GetInt64());
        Assert.Equal(
            batchesWritten,
            resumed.GetProperty("batchesSkipped").GetInt64());
        Assert.Equal(
            JsonTypedSnapshotPackage.Format,
            resumed.GetProperty("sourcePackageFormat")
                .GetString());
        Assert.Equal(
            artifacts.ManifestDigest,
            resumed.GetProperty(
                    "sourcePackageManifestDigest")
                .GetString());
        Assert.Equal(
            artifacts.IntentManifestDigest,
            resumed.GetProperty(
                    "sourcePackageIntentManifestDigest")
                .GetString());
        Assert.Equal(
            originalPackage,
            await File.ReadAllBytesAsync(
                artifacts.PackagePath,
                Cancellation));
        Assert.False(File.Exists(targetPath + ".migration.lock"));
        AssertNoJsonWorkspaceDirectories(workspace.Root);

        await AssertAllCodecTargetRowAsync(targetPath);
        Assert.Equal(
            "awaiting-validation",
            await ReadLifecycleAsync(targetPath));

        string validationPath =
            workspace.PathFor("validation.json");
        var validationOutput = new StringWriter();
        var validationError = new StringWriter();
        int validationCode = await MigrationCommandRunner.RunAsync(
            [
                "migrate", "validate", artifacts.PlanPath,
                "--catalog", artifacts.CatalogPath,
                "--source-package", artifacts.PackagePath,
                "--expected-manifest-digest",
                artifacts.ManifestDigest,
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
        Assert.True(
            string.IsNullOrWhiteSpace(
                validationError.ToString()),
            validationError.ToString());
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
        Assert.Equal(
            MigrationValidationStatus.Passed,
            validation.Outcome);
        Assert.Equal(
            MigrationValidationLevel.Checksum,
            validation.Level);
        Assert.Equal(
            MigrationArtifactSerializer.ComputePlanDigest(plan),
            validation.Binding.PlanDigest);
        Assert.Equal(
            MigrationArtifactSerializer.ComputeCatalogDigest(
                artifacts.Catalog),
            validation.Binding.CatalogDigest);
        Assert.Equal(
            sourceSnapshotIdentity,
            validation.Binding.SourceSnapshotIdentity);
        Assert.Equal(
            targetIdentity,
            validation.Binding.TargetIdentity);
        Assert.Equal(
            originalPackage,
            await File.ReadAllBytesAsync(
                artifacts.PackagePath,
                Cancellation));
        Assert.Equal(
            "activated",
            await ReadLifecycleAsync(targetPath));
        Assert.Empty(
            Directory.EnumerateDirectories(
                workspace.Root,
                "csharpdb-validation-*"));
        AssertNoJsonWorkspaceDirectories(workspace.Root);
        AssertNoJsonTemporaryFiles(workspace.Root);
        await AssertAllCodecTargetRowAsync(targetPath);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task
        ApplyJson_V1AndV2PackagesAreMutuallyIsolatedBeforeTargetCreation(
            bool typedCatalog)
    {
        using var workspace = new TemporaryDirectory();
        string typedDirectory =
            workspace.CreateDirectory("typed");
        string untypedDirectory =
            workspace.CreateDirectory("untyped");
        TypedArtifacts typed =
            await CreateTypedArtifactsAsync(
                typedDirectory,
                "typed",
                "root-array",
                Frame(
                    "root-array",
                    """{"value":"1"}""",
                    """{"value":"2"}"""),
                OneIntentOptions(nullable: false));
        await PublishPlanAsync(typed);
        UntypedArtifacts untyped =
            await CreateUntypedArtifactsAsync(
                untypedDirectory,
                "untyped",
                "root-array");

        string planPath = typedCatalog
            ? typed.PlanPath
            : untyped.PlanPath;
        string catalogPath = typedCatalog
            ? typed.CatalogPath
            : untyped.CatalogPath;
        string packagePath = typedCatalog
            ? untyped.PackagePath
            : typed.PackagePath;
        string manifestDigest = typedCatalog
            ? untyped.ManifestDigest
            : typed.ManifestDigest;
        byte[] originalPackage =
            await File.ReadAllBytesAsync(
                packagePath,
                Cancellation);
        string targetPath = workspace.PathFor("staged.csdb");
        string reportPath = workspace.PathFor("run.json");
        var output = new StringWriter();
        var error = new StringWriter();

        int code = await MigrationCommandRunner.RunAsync(
            ApplyArguments(
                planPath,
                catalogPath,
                packagePath,
                manifestDigest,
                targetPath,
                reportPath,
                workspace.Root,
                resume: false),
            output,
            error,
            Cancellation);

        Assert.Equal(InspectorCommandRunner.ExitError, code);
        Assert.True(
            string.IsNullOrWhiteSpace(output.ToString()));
        Assert.Contains(
            JsonSnapshotPackageRules.InvalidFormat,
            error.ToString(),
            StringComparison.Ordinal);
        Assert.Equal(
            originalPackage,
            await File.ReadAllBytesAsync(
                packagePath,
                Cancellation));
        AssertTargetWasNotCreated(targetPath, reportPath);
        AssertNoJsonWorkspaceDirectories(workspace.Root);
        AssertNoJsonWorkspaceDirectories(typedDirectory);
        AssertNoJsonWorkspaceDirectories(untypedDirectory);
    }

    [Fact]
    public async Task
        ApplyTypedJson_DifferentValidV2PackageFailsCatalogBindingBeforeTargetCreation()
    {
        using var workspace = new TemporaryDirectory();
        TypedArtifacts required =
            await CreateTypedArtifactsAsync(
                workspace.CreateDirectory("required"),
                "required",
                "root-array",
                Frame(
                    "root-array",
                    """{"value":"1"}""",
                    """{"value":"2"}"""),
                OneIntentOptions(nullable: false));
        await PublishPlanAsync(required);
        TypedArtifacts nullable =
            await CreateTypedArtifactsAsync(
                workspace.CreateDirectory("nullable"),
                "nullable",
                "root-array",
                Frame(
                    "root-array",
                    """{"value":"1"}""",
                    """{"value":"2"}"""),
                OneIntentOptions(nullable: true));
        Assert.NotEqual(
            MigrationArtifactSerializer.ComputeCatalogDigest(
                required.Catalog),
            MigrationArtifactSerializer.ComputeCatalogDigest(
                nullable.Catalog));
        byte[] originalPackage =
            await File.ReadAllBytesAsync(
                nullable.PackagePath,
                Cancellation);
        string targetPath = workspace.PathFor("staged.csdb");
        string reportPath = workspace.PathFor("run.json");
        var output = new StringWriter();
        var error = new StringWriter();

        int code = await MigrationCommandRunner.RunAsync(
            ApplyArguments(
                required.PlanPath,
                required.CatalogPath,
                nullable.PackagePath,
                nullable.ManifestDigest,
                targetPath,
                reportPath,
                workspace.Root,
                resume: false),
            output,
            error,
            Cancellation);

        Assert.Equal(InspectorCommandRunner.ExitError, code);
        Assert.True(
            string.IsNullOrWhiteSpace(output.ToString()));
        Assert.Contains(
            "catalog",
            error.ToString(),
            StringComparison.OrdinalIgnoreCase);
        Assert.Contains(
            "match the supplied catalog artifact",
            error.ToString(),
            StringComparison.OrdinalIgnoreCase);
        Assert.Equal(
            originalPackage,
            await File.ReadAllBytesAsync(
                nullable.PackagePath,
                Cancellation));
        AssertTargetWasNotCreated(targetPath, reportPath);
        AssertNoJsonWorkspaceDirectories(workspace.Root);
    }

    [Theory]
    [InlineData("root-array")]
    [InlineData("ndjson")]
    public async Task
        InspectTypedJson_SidecarSourceMismatchFailsWithoutPublicationOrLeak(
            string framing)
    {
        using var workspace = new TemporaryDirectory();
        string sidecarSourcePath =
            workspace.PathFor(
                framing == "ndjson"
                    ? "sidecar-source.ndjson"
                    : "sidecar-source.json");
        string inputPath =
            workspace.PathFor(
                framing == "ndjson"
                    ? "actual.ndjson"
                    : "actual.json");
        string sidecarPath =
            workspace.PathFor(
                "source" +
                JsonTypedIntentSidecar.FileExtension);
        string packagePath =
            workspace.PathFor("source.csdbjson");
        string catalogPath =
            workspace.PathFor("catalog.json");
        string sourceId = "typed-cli/source-mismatch";
        await WriteJsonAsync(
            sidecarSourcePath,
            Frame(framing, """{"value":"1"}"""));
        await WriteJsonAsync(
            inputPath,
            Frame(
                framing,
                $$"""{"value":"{{PrivateSourceValue}}"}"""));
        TypedIntentArtifact intent =
            await WriteTypedIntentAsync(
                sidecarSourcePath,
                sidecarPath,
                framing,
                sourceId,
                workspace.Root,
                OneIntentOptions(nullable: false));
        byte[] originalInput =
            await File.ReadAllBytesAsync(inputPath, Cancellation);
        byte[] originalSidecar =
            await File.ReadAllBytesAsync(
                sidecarPath,
                Cancellation);
        var output = new StringWriter();
        var error = new StringWriter();

        int code = await MigrationCommandRunner.RunAsync(
            TypedInspectArguments(
                inputPath,
                sidecarPath,
                intent.ManifestDigest,
                packagePath,
                catalogPath,
                framing,
                sourceId,
                workspace.Root),
            output,
            error,
            Cancellation);

        Assert.Equal(InspectorCommandRunner.ExitError, code);
        Assert.True(
            string.IsNullOrWhiteSpace(output.ToString()));
        Assert.Contains(
            JsonTypedIntentRules.SourceMismatch,
            error.ToString(),
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            PrivateSourceValue,
            error.ToString(),
            StringComparison.Ordinal);
        Assert.Equal(
            originalInput,
            await File.ReadAllBytesAsync(
                inputPath,
                Cancellation));
        Assert.Equal(
            originalSidecar,
            await File.ReadAllBytesAsync(
                sidecarPath,
                Cancellation));
        Assert.False(File.Exists(packagePath));
        Assert.False(File.Exists(catalogPath));
        AssertNoJsonTemporaryFiles(workspace.Root);
        AssertNoJsonWorkspaceDirectories(workspace.Root);
    }

    [Fact]
    public async Task
        InspectTypedJson_ValidSidecarWithWrongIndependentPinFailsBeforePublication()
    {
        using var workspace = new TemporaryDirectory();
        TypedInspectFixture fixture =
            await CreateTypedInspectFixtureAsync(
                workspace.Root,
                "wrong-intent-pin");
        string wrongDigest = ZeroDigest();
        Assert.NotEqual(
            fixture.IntentManifestDigest,
            wrongDigest);
        byte[] originalSource =
            await File.ReadAllBytesAsync(
                fixture.SourcePath,
                Cancellation);
        byte[] originalSidecar =
            await File.ReadAllBytesAsync(
                fixture.SidecarPath,
                Cancellation);
        var output = new StringWriter();
        var error = new StringWriter();

        int code = await MigrationCommandRunner.RunAsync(
            TypedInspectArguments(
                fixture.SourcePath,
                fixture.SidecarPath,
                wrongDigest,
                fixture.PackagePath,
                fixture.CatalogPath,
                "root-array",
                fixture.SourceId,
                workspace.Root),
            output,
            error,
            Cancellation);

        Assert.Equal(InspectorCommandRunner.ExitError, code);
        Assert.True(
            string.IsNullOrWhiteSpace(output.ToString()));
        Assert.Contains(
            JsonTypedIntentRules.IntegrityMismatch,
            error.ToString(),
            StringComparison.Ordinal);
        Assert.Contains(
            "independently retained",
            error.ToString(),
            StringComparison.OrdinalIgnoreCase);
        Assert.Equal(
            originalSource,
            await File.ReadAllBytesAsync(
                fixture.SourcePath,
                Cancellation));
        Assert.Equal(
            originalSidecar,
            await File.ReadAllBytesAsync(
                fixture.SidecarPath,
                Cancellation));
        Assert.False(File.Exists(fixture.PackagePath));
        Assert.False(File.Exists(fixture.CatalogPath));
        AssertNoJsonTemporaryFiles(workspace.Root);
        AssertNoJsonWorkspaceDirectories(workspace.Root);
    }

    [Theory]
    [InlineData("intent-only")]
    [InlineData("digest-only")]
    [InlineData("uppercase-digest")]
    public async Task
        InspectTypedJson_RejectsIncompleteOrNoncanonicalIntentPinOptionsBeforeSnapshot(
            string optionCase)
    {
        using var workspace = new TemporaryDirectory();
        TypedInspectFixture fixture =
            await CreateTypedInspectFixtureAsync(
                workspace.Root,
                "invalid-intent-options");
        byte[] originalSource =
            await File.ReadAllBytesAsync(
                fixture.SourcePath,
                Cancellation);
        byte[] originalSidecar =
            await File.ReadAllBytesAsync(
                fixture.SidecarPath,
                Cancellation);
        var arguments = new List<string>
        {
            "migrate", "inspect",
            "--source", "json",
            "--input", fixture.SourcePath,
            "--package", fixture.PackagePath,
            "--out", fixture.CatalogPath,
            "--workspace", workspace.Root,
            "--max-source-bytes", "1048576",
        };
        switch (optionCase)
        {
            case "intent-only":
                arguments.AddRange(
                    ["--typed-intent", fixture.SidecarPath]);
                break;
            case "digest-only":
                arguments.AddRange(
                    [
                        "--expected-intent-manifest-digest",
                        fixture.IntentManifestDigest,
                    ]);
                break;
            case "uppercase-digest":
                arguments.AddRange(
                    [
                        "--typed-intent", fixture.SidecarPath,
                        "--expected-intent-manifest-digest",
                        fixture.IntentManifestDigest
                            .ToUpperInvariant(),
                    ]);
                break;
            default:
                throw new ArgumentOutOfRangeException(
                    nameof(optionCase));
        }
        var output = new StringWriter();
        var error = new StringWriter();

        int code = await MigrationCommandRunner.RunAsync(
            arguments.ToArray(),
            output,
            error,
            Cancellation);

        Assert.Equal(InspectorCommandRunner.ExitUsage, code);
        Assert.True(
            string.IsNullOrWhiteSpace(output.ToString()));
        Assert.Contains(
            optionCase == "uppercase-digest"
                ? "canonical lowercase"
                : "must be supplied together",
            error.ToString(),
            StringComparison.OrdinalIgnoreCase);
        Assert.Equal(
            originalSource,
            await File.ReadAllBytesAsync(
                fixture.SourcePath,
                Cancellation));
        Assert.Equal(
            originalSidecar,
            await File.ReadAllBytesAsync(
                fixture.SidecarPath,
                Cancellation));
        Assert.False(File.Exists(fixture.PackagePath));
        Assert.False(File.Exists(fixture.CatalogPath));
        AssertNoJsonTemporaryFiles(workspace.Root);
        AssertNoJsonWorkspaceDirectories(workspace.Root);
    }

    [Fact]
    public async Task
        InspectTypedJson_TamperedSidecarFailsPinnedIntegrityWithoutPublicationOrLeak()
    {
        using var workspace = new TemporaryDirectory();
        string inputPath = workspace.PathFor("source.json");
        string sidecarPath =
            workspace.PathFor(
                "source" +
                JsonTypedIntentSidecar.FileExtension);
        string packagePath =
            workspace.PathFor("source.csdbjson");
        string catalogPath =
            workspace.PathFor("catalog.json");
        string sourceId = "typed-cli/sidecar-tamper";
        await WriteJsonAsync(
            inputPath,
            $$"""[{"value":"{{PrivateSourceValue}}"}]""");
        TypedIntentArtifact intent =
            await WriteTypedIntentAsync(
                inputPath,
                sidecarPath,
                "root-array",
                sourceId,
                workspace.Root,
                OneIntentOptions(nullable: false));
        string canonical =
            await File.ReadAllTextAsync(
                sidecarPath,
                StrictUtf8,
                Cancellation);
        string tampered = canonical.Replace(
            "\"expectedPropertyName\":\"value\"",
            "\"expectedPropertyName\":\"vxlue\"",
            StringComparison.Ordinal);
        Assert.NotEqual(canonical, tampered);
        await File.WriteAllTextAsync(
            sidecarPath,
            tampered,
            StrictUtf8,
            Cancellation);
        byte[] tamperedBytes =
            await File.ReadAllBytesAsync(
                sidecarPath,
                Cancellation);
        var output = new StringWriter();
        var error = new StringWriter();

        int code = await MigrationCommandRunner.RunAsync(
            TypedInspectArguments(
                inputPath,
                sidecarPath,
                intent.ManifestDigest,
                packagePath,
                catalogPath,
                "root-array",
                sourceId,
                workspace.Root),
            output,
            error,
            Cancellation);

        Assert.Equal(InspectorCommandRunner.ExitError, code);
        Assert.True(
            string.IsNullOrWhiteSpace(output.ToString()));
        Assert.Contains(
            JsonTypedIntentRules.IntegrityMismatch,
            error.ToString(),
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            PrivateSourceValue,
            error.ToString(),
            StringComparison.Ordinal);
        Assert.Equal(
            tamperedBytes,
            await File.ReadAllBytesAsync(
                sidecarPath,
                Cancellation));
        Assert.False(File.Exists(packagePath));
        Assert.False(File.Exists(catalogPath));
        AssertNoJsonTemporaryFiles(workspace.Root);
        AssertNoJsonWorkspaceDirectories(workspace.Root);
    }

    [Fact]
    public async Task
        InspectTypedJson_CatalogPublicationFailurePreservesPinOpenablePackage()
    {
        using var workspace = new TemporaryDirectory();
        TypedInspectFixture fixture =
            await CreateTypedInspectFixtureAsync(
                workspace.Root,
                "catalog-publication");
        string catalogDirectory =
            workspace.PathFor("catalog-output");
        string canaryPath = Path.Combine(
            catalogDirectory,
            "do-not-delete.txt");
        Directory.CreateDirectory(catalogDirectory);
        await File.WriteAllTextAsync(
            canaryPath,
            "preserve this directory",
            StrictUtf8,
            Cancellation);
        byte[] originalSource =
            await File.ReadAllBytesAsync(
                fixture.SourcePath,
                Cancellation);
        byte[] originalSidecar =
            await File.ReadAllBytesAsync(
                fixture.SidecarPath,
                Cancellation);
        var output = new StringWriter();
        var error = new StringWriter();

        int code = await MigrationCommandRunner.RunAsync(
            TypedInspectArguments(
                fixture.SourcePath,
                fixture.SidecarPath,
                fixture.IntentManifestDigest,
                fixture.PackagePath,
                catalogDirectory,
                "root-array",
                fixture.SourceId,
                workspace.Root),
            output,
            error,
            Cancellation);

        Assert.Equal(InspectorCommandRunner.ExitError, code);
        Assert.True(
            string.IsNullOrWhiteSpace(output.ToString()));
        Assert.Contains(
            "Catalog publication failed",
            error.ToString(),
            StringComparison.Ordinal);
        Assert.Contains(
            "package was preserved",
            error.ToString(),
            StringComparison.OrdinalIgnoreCase);
        Assert.Contains(
            Path.GetFullPath(fixture.PackagePath),
            error.ToString(),
            StringComparison.Ordinal);
        Assert.True(File.Exists(fixture.PackagePath));
        Assert.Equal(
            "preserve this directory",
            await File.ReadAllTextAsync(
                canaryPath,
                Cancellation));
        Assert.Equal(
            originalSource,
            await File.ReadAllBytesAsync(
                fixture.SourcePath,
                Cancellation));
        Assert.Equal(
            originalSidecar,
            await File.ReadAllBytesAsync(
                fixture.SidecarPath,
                Cancellation));

        string manifestDigest;
        await using (JsonTypedSnapshotPackageSession session =
            await JsonTypedSnapshotPackage.OpenAsync(
                fixture.PackagePath,
                new JsonSnapshotPackageOpenOptions
                {
                    WorkspacePath = workspace.Root,
                    MaxSourceBytes = 1024 * 1024,
                },
                Cancellation))
        {
            manifestDigest =
                session.Manifest.ManifestDigest;
            AssertCanonicalDigest(manifestDigest);
            Assert.Equal(
                fixture.IntentManifestDigest,
                session.Manifest.IntentManifestDigest);
            Assert.Equal(
                MigrationArtifactSerializer
                    .ComputeCatalogDigest(session.Catalog),
                session.Manifest.CatalogDigest);
        }
        await using (JsonTypedSnapshotPackageSession pinned =
            await JsonTypedSnapshotPackage.OpenAsync(
                fixture.PackagePath,
                new JsonSnapshotPackageOpenOptions
                {
                    WorkspacePath = workspace.Root,
                    MaxSourceBytes = 1024 * 1024,
                    ExpectedManifestDigest = manifestDigest,
                },
                Cancellation))
        {
            Assert.Equal(
                manifestDigest,
                pinned.Manifest.ManifestDigest);
        }
        AssertNoJsonTemporaryFiles(workspace.Root);
        AssertNoJsonWorkspaceDirectories(workspace.Root);
    }

    [Fact]
    public async Task
        ApplyTypedJson_TamperedPackageFailsPinnedIntegrityBeforeTargetCreation()
    {
        using var workspace = new TemporaryDirectory();
        TypedArtifacts artifacts =
            await CreateTypedArtifactsAsync(
                workspace.Root,
                "package-tamper",
                "root-array",
                Frame(
                    "root-array",
                    """{"value":"1"}""",
                    """{"value":"2"}"""),
                OneIntentOptions(nullable: false));
        await PublishPlanAsync(artifacts);
        byte[] tampered =
            await File.ReadAllBytesAsync(
                artifacts.PackagePath,
                Cancellation);
        tampered[^1] ^= 0x01;
        await File.WriteAllBytesAsync(
            artifacts.PackagePath,
            tampered,
            Cancellation);
        string targetPath = workspace.PathFor("staged.csdb");
        string reportPath = workspace.PathFor("run.json");
        var output = new StringWriter();
        var error = new StringWriter();

        int code = await MigrationCommandRunner.RunAsync(
            ApplyArguments(
                artifacts,
                targetPath,
                reportPath,
                workspace.Root,
                resume: false),
            output,
            error,
            Cancellation);

        Assert.Equal(InspectorCommandRunner.ExitError, code);
        Assert.True(
            string.IsNullOrWhiteSpace(output.ToString()));
        Assert.Contains(
            JsonSnapshotPackageRules.IntegrityMismatch,
            error.ToString(),
            StringComparison.Ordinal);
        Assert.Equal(
            tampered,
            await File.ReadAllBytesAsync(
                artifacts.PackagePath,
                Cancellation));
        AssertTargetWasNotCreated(targetPath, reportPath);
        AssertNoJsonWorkspaceDirectories(workspace.Root);
    }

    [Fact]
    public async Task
        ApplyTypedJson_ValidPackageWithWrongIndependentPinFailsBeforeTargetCreation()
    {
        using var workspace = new TemporaryDirectory();
        TypedArtifacts artifacts =
            await CreateTypedArtifactsAsync(
                workspace.Root,
                "wrong-package-pin",
                "root-array",
                Frame(
                    "root-array",
                    """{"value":"1"}""",
                    """{"value":"2"}"""),
                OneIntentOptions(nullable: false));
        await PublishPlanAsync(artifacts);
        string wrongDigest = ZeroDigest();
        Assert.NotEqual(
            artifacts.ManifestDigest,
            wrongDigest);
        byte[] originalPackage =
            await File.ReadAllBytesAsync(
                artifacts.PackagePath,
                Cancellation);
        string targetPath = workspace.PathFor("staged.csdb");
        string reportPath = workspace.PathFor("run.json");
        var output = new StringWriter();
        var error = new StringWriter();

        int code = await MigrationCommandRunner.RunAsync(
            ApplyArguments(
                artifacts.PlanPath,
                artifacts.CatalogPath,
                artifacts.PackagePath,
                wrongDigest,
                targetPath,
                reportPath,
                workspace.Root,
                resume: false),
            output,
            error,
            Cancellation);

        Assert.Equal(InspectorCommandRunner.ExitError, code);
        Assert.True(
            string.IsNullOrWhiteSpace(output.ToString()));
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
    public async Task
        PlanTypedJson_DeterministicRejectsRemainExplicitlyOutsideThisRoute()
    {
        using var workspace = new TemporaryDirectory();
        TypedArtifacts artifacts =
            await CreateTypedArtifactsAsync(
                workspace.Root,
                "deterministic-deferred",
                "root-array",
                Frame(
                    "root-array",
                    """{"value":"1"}""",
                    """{"value":"bad"}"""),
                OneIntentOptions(nullable: false));
        byte[] originalPackage =
            await File.ReadAllBytesAsync(
                artifacts.PackagePath,
                Cancellation);
        var output = new StringWriter();
        var error = new StringWriter();

        int code = await MigrationCommandRunner.RunAsync(
            [
                "migrate", "plan", artifacts.CatalogPath,
                "--out", artifacts.PlanPath,
                "--accept-exclusions", "all",
                "--reject-mode", "deterministic",
                "--reject-rules", "all",
                "--max-rejected-rows-per-batch", "100",
                "--max-rejected-rows-per-run", "10000",
                "--max-reject-evidence-value-bytes", "4096",
                "--max-reject-evidence-bytes-per-batch",
                "65536",
                "--max-reject-evidence-bytes-per-run",
                "1048576",
                "--max-reject-artifact-bytes", "16777216",
            ],
            output,
            error,
            Cancellation);

        Assert.Equal(InspectorCommandRunner.ExitUsage, code);
        Assert.True(
            string.IsNullOrWhiteSpace(output.ToString()));
        Assert.Contains(
            "deterministic",
            error.ToString(),
            StringComparison.OrdinalIgnoreCase);
        Assert.Contains(
            "typed",
            error.ToString(),
            StringComparison.OrdinalIgnoreCase);
        Assert.False(File.Exists(artifacts.PlanPath));
        Assert.Equal(
            originalPackage,
            await File.ReadAllBytesAsync(
                artifacts.PackagePath,
                Cancellation));
        AssertNoJsonWorkspaceDirectories(workspace.Root);
    }

    [Fact]
    public async Task
        PlanTypedJson_RejectsMalformedRouteFacetsBeforePublication()
    {
        using var workspace = new TemporaryDirectory();
        TypedArtifacts artifacts =
            await CreateTypedArtifactsAsync(
                workspace.Root,
                "malformed-route",
                "root-array",
                Frame(
                    "root-array",
                    """{"value":"1"}"""),
                OneIntentOptions(nullable: false));
        byte[] originalPackage =
            await File.ReadAllBytesAsync(
                artifacts.PackagePath,
                Cancellation);
        (string Stem, string FacetName, string? Value)[]
            cases =
            [
                (
                    "missing-schema",
                    "jsonSchemaAlgorithm",
                    null),
                (
                    "wrong-schema",
                    "jsonSchemaAlgorithm",
                    JsonTableSchemaInferenceResult.AlgorithmId),
                (
                    "wrong-scalar",
                    "jsonScalarPolicy",
                    JsonTableSchemaInferenceResult.ScalarPolicyId),
                (
                    "wrong-intent-format",
                    "jsonTypedIntentFormat",
                    "csharpdb-json-typed-intent/v999"),
                (
                    "missing-intent-digest",
                    "jsonTypedIntentManifestDigest",
                    null),
                (
                    "noncanonical-intent-digest",
                    "jsonTypedIntentManifestDigest",
                    artifacts.IntentManifestDigest
                        .ToUpperInvariant()),
            ];

        foreach ((string stem, string facetName, string? value)
                 in cases)
        {
            MigrationCatalog malformed =
                ReplaceTableFacet(
                    artifacts.Catalog,
                    facetName,
                    value);
            string catalogPath =
                workspace.PathFor(stem + "-catalog.json");
            string planPath =
                workspace.PathFor(stem + "-plan.json");
            await File.WriteAllTextAsync(
                catalogPath,
                MigrationArtifactSerializer.SerializeCatalog(
                    malformed),
                StrictUtf8,
                Cancellation);
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

            Assert.Equal(
                InspectorCommandRunner.ExitUsage,
                code);
            Assert.True(
                string.IsNullOrWhiteSpace(
                    output.ToString()));
            Assert.Contains(
                "explicitly typed retained JSON package v2",
                error.ToString(),
                StringComparison.OrdinalIgnoreCase);
            Assert.False(File.Exists(planPath));
        }

        Assert.Equal(
            originalPackage,
            await File.ReadAllBytesAsync(
                artifacts.PackagePath,
                Cancellation));
        AssertNoJsonTemporaryFiles(workspace.Root);
        AssertNoJsonWorkspaceDirectories(workspace.Root);
    }

    [Theory]
    [InlineData("input")]
    [InlineData("package")]
    [InlineData("catalog")]
    public async Task
        InspectTypedJson_RejectsIntentPathCollisionsBeforeMutation(
            string collision)
    {
        using var workspace = new TemporaryDirectory();
        string inputPath = workspace.PathFor("source.json");
        string sidecarPath =
            workspace.PathFor(
                "source" +
                JsonTypedIntentSidecar.FileExtension);
        string packagePath =
            workspace.PathFor("source.csdbjson");
        string catalogPath =
            workspace.PathFor("catalog.json");
        string sourceId = "typed-cli/path-collision";
        await WriteJsonAsync(
            inputPath,
            """[{"value":"1"}]""");
        TypedIntentArtifact intent =
            await WriteTypedIntentAsync(
                inputPath,
                sidecarPath,
                "root-array",
                sourceId,
                workspace.Root,
                OneIntentOptions(nullable: false));
        byte[] originalInput =
            await File.ReadAllBytesAsync(inputPath, Cancellation);
        byte[] originalSidecar =
            await File.ReadAllBytesAsync(
                sidecarPath,
                Cancellation);

        string selectedIntentPath = collision == "input"
            ? inputPath
            : sidecarPath;
        string selectedPackagePath = collision == "package"
            ? sidecarPath
            : packagePath;
        string selectedCatalogPath = collision == "catalog"
            ? sidecarPath
            : catalogPath;
        var output = new StringWriter();
        var error = new StringWriter();

        int code = await MigrationCommandRunner.RunAsync(
            TypedInspectArguments(
                inputPath,
                selectedIntentPath,
                intent.ManifestDigest,
                selectedPackagePath,
                selectedCatalogPath,
                "root-array",
                sourceId,
                workspace.Root),
            output,
            error,
            Cancellation);

        Assert.Equal(InspectorCommandRunner.ExitUsage, code);
        Assert.True(
            string.IsNullOrWhiteSpace(output.ToString()));
        Assert.Contains(
            "different",
            error.ToString(),
            StringComparison.OrdinalIgnoreCase);
        Assert.Equal(
            originalInput,
            await File.ReadAllBytesAsync(
                inputPath,
                Cancellation));
        Assert.Equal(
            originalSidecar,
            await File.ReadAllBytesAsync(
                sidecarPath,
                Cancellation));
        Assert.False(File.Exists(packagePath));
        Assert.False(File.Exists(catalogPath));
        AssertNoJsonTemporaryFiles(workspace.Root);
        AssertNoJsonWorkspaceDirectories(workspace.Root);
    }

    private static async ValueTask<TypedInspectFixture>
        CreateTypedInspectFixtureAsync(
            string directory,
            string stem)
    {
        Directory.CreateDirectory(directory);
        string sourcePath =
            Path.Combine(directory, stem + ".json");
        string sidecarPath =
            Path.Combine(
                directory,
                stem +
                JsonTypedIntentSidecar.FileExtension);
        string packagePath =
            Path.Combine(
                directory,
                stem +
                JsonTypedSnapshotPackage.FileExtension);
        string catalogPath =
            Path.Combine(
                directory,
                stem + "-catalog.json");
        string sourceId = "typed-cli/" + stem;
        await WriteJsonAsync(
            sourcePath,
            """[{"value":"1"}]""");
        TypedIntentArtifact intent =
            await WriteTypedIntentAsync(
                sourcePath,
                sidecarPath,
                "root-array",
                sourceId,
                directory,
                OneIntentOptions(nullable: false));
        AssertNoJsonWorkspaceDirectories(directory);
        return new TypedInspectFixture(
            sourcePath,
            sidecarPath,
            packagePath,
            catalogPath,
            sourceId,
            intent.ManifestDigest);
    }

    private static async ValueTask<TypedArtifacts>
        CreateTypedArtifactsAsync(
            string directory,
            string stem,
            string framing,
            string contents,
            JsonTypedIntentOptions intentOptions)
    {
        Directory.CreateDirectory(directory);
        string sourcePath = Path.Combine(
            directory,
            stem +
            (framing == "ndjson" ? ".ndjson" : ".json"));
        string sidecarPath = Path.Combine(
            directory,
            stem + JsonTypedIntentSidecar.FileExtension);
        string packagePath = Path.Combine(
            directory,
            stem + JsonTypedSnapshotPackage.FileExtension);
        string catalogPath = Path.Combine(
            directory,
            stem + "-catalog.json");
        string planPath = Path.Combine(
            directory,
            stem + "-plan.json");
        string sourceId = "typed-cli/" + stem;
        await WriteJsonAsync(sourcePath, contents);
        TypedIntentArtifact intent =
            await WriteTypedIntentAsync(
                sourcePath,
                sidecarPath,
                framing,
                sourceId,
                directory,
                intentOptions);
        var output = new StringWriter();
        var error = new StringWriter();

        int code = await MigrationCommandRunner.RunAsync(
            TypedInspectArguments(
                sourcePath,
                sidecarPath,
                intent.ManifestDigest,
                packagePath,
                catalogPath,
                framing,
                sourceId,
                directory),
            output,
            error,
            Cancellation);

        AssertSuccessful(code, error);
        Assert.True(
            string.IsNullOrWhiteSpace(error.ToString()),
            error.ToString());
        Assert.Contains(
            "Status: OK",
            output.ToString(),
            StringComparison.Ordinal);
        string manifestDigest = ReadStatusField(
            output.ToString(),
            "manifestDigest");
        AssertCanonicalDigest(manifestDigest);
        Assert.True(File.Exists(packagePath));
        Assert.True(File.Exists(catalogPath));
        MigrationCatalog catalog =
            MigrationArtifactSerializer.DeserializeCatalog(
                await File.ReadAllTextAsync(
                    catalogPath,
                    Cancellation));
        Assert.Equal(MigrationSourceKind.Json, catalog.Source.Kind);
        AssertNoJsonTemporaryFiles(directory);
        AssertNoJsonWorkspaceDirectories(directory);
        return new TypedArtifacts(
            sourcePath,
            sidecarPath,
            packagePath,
            catalogPath,
            planPath,
            intent.ManifestDigest,
            manifestDigest,
            catalog);
    }

    private static async ValueTask<UntypedArtifacts>
        CreateUntypedArtifactsAsync(
            string directory,
            string stem,
            string framing)
    {
        Directory.CreateDirectory(directory);
        string sourcePath = Path.Combine(
            directory,
            stem +
            (framing == "ndjson" ? ".ndjson" : ".json"));
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
            Frame(
                framing,
                """{"value":"1"}""",
                """{"value":"2"}"""));
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
        string manifestDigest = ReadStatusField(
            inspectOutput.ToString(),
            "manifestDigest");
        AssertCanonicalDigest(manifestDigest);

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
        AssertNoJsonWorkspaceDirectories(directory);
        return new UntypedArtifacts(
            packagePath,
            catalogPath,
            planPath,
            manifestDigest);
    }

    private static async ValueTask<TypedIntentArtifact>
        WriteTypedIntentAsync(
            string sourcePath,
            string sidecarPath,
            string framing,
            string sourceId,
            string workspacePath,
            JsonTypedIntentOptions intentOptions)
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
        JsonSourceBinding binding =
            await JsonSourceBinding.CreateAsync(
                snapshot,
                new JsonStreamingReaderOptions
                {
                    Framing = ParseFraming(framing),
                },
                sourceId,
                Cancellation);
        JsonTypedIntentManifest manifest =
            await JsonTypedIntentSidecar.WriteAsync(
                sidecarPath,
                binding,
                intentOptions,
                Cancellation);
        AssertCanonicalDigest(manifest.ManifestDigest);
        return new TypedIntentArtifact(
            sidecarPath,
            manifest.ManifestDigest);
    }

    private static async ValueTask<MigrationPlan>
        PublishPlanAsync(TypedArtifacts artifacts)
    {
        var output = new StringWriter();
        var error = new StringWriter();
        int code = await MigrationCommandRunner.RunAsync(
            [
                "migrate", "plan", artifacts.CatalogPath,
                "--out", artifacts.PlanPath,
                "--accept-exclusions", "all",
            ],
            output,
            error,
            Cancellation);
        AssertSuccessful(code, error);
        Assert.True(
            string.IsNullOrWhiteSpace(error.ToString()),
            error.ToString());
        return MigrationArtifactSerializer.DeserializePlan(
            await File.ReadAllTextAsync(
                artifacts.PlanPath,
                Cancellation),
            artifacts.Catalog);
    }

    private static string[] TypedInspectArguments(
        string inputPath,
        string sidecarPath,
        string sidecarManifestDigest,
        string packagePath,
        string catalogPath,
        string framing,
        string sourceId,
        string workspacePath) =>
    [
        "migrate", "inspect",
        "--source", "json",
        "--input", inputPath,
        "--typed-intent", sidecarPath,
        "--expected-intent-manifest-digest",
        sidecarManifestDigest,
        "--package", packagePath,
        "--out", catalogPath,
        "--framing", framing,
        "--source-id", sourceId,
        "--workspace", workspacePath,
        "--max-source-bytes", "1048576",
    ];

    private static string[] ApplyArguments(
        TypedArtifacts artifacts,
        string targetPath,
        string reportPath,
        string workspacePath,
        bool resume) =>
        ApplyArguments(
            artifacts.PlanPath,
            artifacts.CatalogPath,
            artifacts.PackagePath,
            artifacts.ManifestDigest,
            targetPath,
            reportPath,
            workspacePath,
            resume);

    private static string[] ApplyArguments(
        string planPath,
        string catalogPath,
        string packagePath,
        string manifestDigest,
        string targetPath,
        string reportPath,
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

    private static MigrationCatalog ReplaceTableFacet(
        MigrationCatalog catalog,
        string facetName,
        string? value)
    {
        MigrationCatalogObject[] objects =
            catalog.Objects
                .Select(
                    item =>
                    {
                        if (item.Kind !=
                            MigrationObjectKind.Table)
                        {
                            return item;
                        }

                        var facets = item.Facets
                            .Where(
                                facet =>
                                    !string.Equals(
                                        facet.Name,
                                        facetName,
                                        StringComparison.Ordinal))
                            .ToList();
                        if (value is not null)
                        {
                            facets.Add(
                                new MigrationCatalogFacet
                                {
                                    Name = facetName,
                                    Value = value,
                                });
                        }
                        return item with
                        {
                            Facets =
                                Array.AsReadOnly(
                                    facets.ToArray()),
                        };
                    })
                .ToArray();
        return catalog with
        {
            Objects = Array.AsReadOnly(objects),
        };
    }

    private static string ZeroDigest() =>
        "sha256:" + new string('0', 64);

    private static async ValueTask AssertAllCodecTargetRowAsync(
        string targetPath)
    {
        await using Database database =
            await Database.OpenAsync(targetPath, Cancellation);
        await using var query = await database.ExecuteAsync(
            """
            SELECT
              "binary",
              "decimalString",
              "decimalNumber",
              "guid",
              "date",
              "time",
              "dateTime",
              "dateTimeOffset",
              "int64",
              "uint64",
              "ordinary"
            FROM "json_data";
            """,
            Cancellation);
        var rows = await query.ToListAsync(Cancellation);
        var row = Assert.Single(rows);

        Assert.Equal(PrimitiveDbType.Blob, row[0].Type);
        Assert.Equal(
            [1, 2, 3, 4],
            row[0].AsBlob.ToArray());
        Assert.Equal(PrimitiveDbType.Text, row[1].Type);
        Assert.Equal(
            "12345678901234567890.123456789012345678",
            row[1].AsText);
        Assert.Equal(PrimitiveDbType.Decimal, row[2].Type);
        Assert.Equal(123.45m, row[2].AsDecimal);
        Assert.Equal(
            "00112233-4455-6677-8899-aabbccddeeff",
            row[3].AsText);
        Assert.Equal("2024-02-29", row[4].AsText);
        Assert.Equal("08:09:10.1234567", row[5].AsText);
        Assert.Equal(
            "2026-07-23 08:09:10.1234567",
            row[6].AsText);
        Assert.Equal(
            "2026-07-23 15:09:10.1234567+00:00",
            row[7].AsText);
        Assert.Equal(PrimitiveDbType.Integer, row[8].Type);
        Assert.Equal(long.MinValue, row[8].AsInteger);
        Assert.Equal(PrimitiveDbType.Text, row[9].Type);
        Assert.Equal(
            "18446744073709551615",
            row[9].AsText);
        Assert.Equal("ordinary", row[10].AsText);
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

    private static JsonTypedIntentOptions AllCodecOptions() =>
        new()
        {
            MaxDecodedBinaryBytes = 4096,
            MaxDecimalDigits = 100,
            Columns =
            [
                Intent(
                    0,
                    "binary",
                    JsonTypedValueCodec.BinaryBase64),
                Intent(
                    1,
                    "decimalString",
                    JsonTypedValueCodec.DecimalString,
                    precision: 38,
                    scale: 18),
                Intent(
                    2,
                    "decimalNumber",
                    JsonTypedValueCodec.DecimalNumber,
                    precision: 10,
                    scale: 2),
                Intent(3, "guid", JsonTypedValueCodec.GuidD),
                Intent(
                    4,
                    "date",
                    JsonTypedValueCodec.DateCSharpDbText),
                Intent(
                    5,
                    "time",
                    JsonTypedValueCodec.TimeCSharpDbText),
                Intent(
                    6,
                    "dateTime",
                    JsonTypedValueCodec.DateTimeCSharpDbText),
                Intent(
                    7,
                    "dateTimeOffset",
                    JsonTypedValueCodec
                        .DateTimeOffsetCSharpDbText),
                Intent(
                    8,
                    "int64",
                    JsonTypedValueCodec.Int64String),
                Intent(
                    9,
                    "uint64",
                    JsonTypedValueCodec.UInt64String),
            ],
        };

    private static JsonTypedIntentOptions OneIntentOptions(
        bool nullable) =>
        new()
        {
            Columns =
            [
                Intent(
                    0,
                    "value",
                    JsonTypedValueCodec.Int64String,
                    nullable),
            ],
        };

    private static JsonTypedColumnIntent Intent(
        int columnIndex,
        string expectedPropertyName,
        JsonTypedValueCodec codec,
        bool? nullable = false,
        int? precision = null,
        int? scale = null) =>
        new()
        {
            ColumnIndex = columnIndex,
            ExpectedPropertyName = expectedPropertyName,
            Codec = codec,
            Nullable = nullable,
            MissingPolicy = JsonMissingPropertyPolicy.Reject,
            Precision = precision,
            Scale = scale,
        };

    private static JsonInputFraming ParseFraming(
        string framing) =>
        framing switch
        {
            "root-array" => JsonInputFraming.RootArray,
            "ndjson" => JsonInputFraming.MultipleValues,
            _ => throw new ArgumentOutOfRangeException(
                nameof(framing)),
        };

    private static string Frame(
        string framing,
        params string[] values) =>
        framing switch
        {
            "root-array" =>
                "[\n" +
                string.Join(",\n", values) +
                "\n]",
            "ndjson" =>
                string.Join("\n", values) + "\n",
            _ => throw new ArgumentOutOfRangeException(
                nameof(framing)),
        };

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
        Assert.Empty(
            Directory.EnumerateFiles(
                directory,
                ".csdbjson-*.tmp"));
        Assert.Empty(
            Directory.EnumerateFiles(
                directory,
                ".csharpdb-migration-*.tmp"));
    }

    private static void AssertNoJsonWorkspaceDirectories(
        string directory) =>
        Assert.Empty(
            Directory.EnumerateDirectories(
                directory,
                "csharpdb-json-*"));

    private sealed record TypedIntentArtifact(
        string SidecarPath,
        string ManifestDigest);

    private sealed record TypedInspectFixture(
        string SourcePath,
        string SidecarPath,
        string PackagePath,
        string CatalogPath,
        string SourceId,
        string IntentManifestDigest);

    private sealed record TypedArtifacts(
        string SourcePath,
        string SidecarPath,
        string PackagePath,
        string CatalogPath,
        string PlanPath,
        string IntentManifestDigest,
        string ManifestDigest,
        MigrationCatalog Catalog);

    private sealed record UntypedArtifacts(
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
                "csharpdb-typed-json-cli-" +
                Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(Root);
        }

        internal string Root { get; }

        internal string PathFor(string name) =>
            Path.Combine(Root, name);

        internal string CreateDirectory(string name)
        {
            string path = PathFor(name);
            Directory.CreateDirectory(path);
            return path;
        }

        public void Dispose()
        {
            if (Directory.Exists(Root))
            {
                Directory.Delete(
                    Root,
                    recursive: true);
            }
        }
    }
}
