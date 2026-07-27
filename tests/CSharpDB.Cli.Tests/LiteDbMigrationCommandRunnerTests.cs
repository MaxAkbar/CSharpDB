using System.Text.Json;
using System.Text.RegularExpressions;
using CSharpDB.Engine;
using CSharpDB.Migration;
using CSharpDB.Migration.LiteDb;
using CSharpDB.Migration.Validation;
using LiteDB;

namespace CSharpDB.Cli.Tests;

public sealed partial class LiteDbMigrationCommandRunnerTests
{
    private const string PrivateDocumentValue =
        "PRIVATE-LITEDB-DOCUMENT-VALUE";

    private static CancellationToken Cancellation =>
        TestContext.Current.CancellationToken;

    [Fact]
    public async Task
        InspectPlanAndPreview_UseRetainedSnapshotWithoutDisclosingSourceData()
    {
        using var workspace = new TemporaryDirectory();
        string sourcePath = workspace.PathFor(
            "private-live-source.db");
        string packagePath = workspace.PathFor(
            "retained.csdblitedb");
        string catalogPath = workspace.PathFor(
            "catalog.json");
        string planPath = workspace.PathFor(
            "plan.json");
        _ = CreateDatabase(sourcePath);
        byte[] sourceBefore =
            await File.ReadAllBytesAsync(
                sourcePath,
                Cancellation);

        var inspectOutput = new StringWriter();
        var inspectError = new StringWriter();
        int inspectCode =
            await MigrationCommandRunner.RunAsync(
                [
                    "migrate", "inspect",
                    "--source", "litedb",
                    "--input", sourcePath,
                    "--package", packagePath,
                    "--out", catalogPath,
                ],
                inspectOutput,
                inspectError,
                Cancellation);

        AssertSuccessfulCommand(
            inspectCode,
            inspectError);
        Assert.Equal(
            sourceBefore,
            await File.ReadAllBytesAsync(
                sourcePath,
                Cancellation));
        Assert.Equal(
            sourceBefore,
            await File.ReadAllBytesAsync(
                packagePath,
                Cancellation));
        string catalogJson =
            await File.ReadAllTextAsync(
                catalogPath,
                Cancellation);
        Assert.DoesNotContain(
            sourcePath,
            inspectOutput.ToString(),
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            sourcePath,
            catalogJson,
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            PrivateDocumentValue,
            inspectOutput.ToString(),
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            PrivateDocumentValue,
            catalogJson,
            StringComparison.Ordinal);

        string packageDigest =
            ReadManifestDigest(
                inspectOutput.ToString());
        Assert.Matches(
            "^sha256:[0-9a-f]{64}$",
            packageDigest);
        MigrationCatalog catalog =
            MigrationArtifactSerializer
                .DeserializeCatalog(catalogJson);
        Assert.Equal(
            MigrationSourceKind.LiteDb,
            catalog.Source.Kind);
        Assert.Equal(
            MigrationConsistencyKind.Snapshot,
            catalog.Source.Consistency.Kind);
        Assert.Equal(
            packageDigest,
            catalog.Source.Fingerprint);
        MigrationCatalogObject main =
            catalog.Objects.Single(item =>
                item.Kind ==
                    MigrationObjectKind.Namespace &&
                item.SourceName == "main");
        Assert.Contains(
            main.Facets,
            facet =>
                facet.Name ==
                    "liteDbCatalogContract" &&
                facet.Value ==
                    LiteDbMigrationSourceInspector
                        .CatalogContract);

        var planError = new StringWriter();
        int planCode =
            await MigrationCommandRunner.RunAsync(
                [
                    "migrate", "plan", catalogPath,
                    "--out", planPath,
                    "--accept-exclusions", "all",
                ],
                TextWriter.Null,
                planError,
                Cancellation);
        AssertSuccessfulCommand(planCode, planError);

        var previewOutput = new StringWriter();
        var previewError = new StringWriter();
        int previewCode =
            await MigrationCommandRunner.RunAsync(
                [
                    "migrate", "preview", planPath,
                    "--catalog", catalogPath,
                    "--format", "json",
                ],
                previewOutput,
                previewError,
                Cancellation);
        AssertSuccessfulCommand(
            previewCode,
            previewError);
        using JsonDocument preview =
            JsonDocument.Parse(
                previewOutput.ToString());
        Assert.Equal(
            "csharpdb-migration-preview/v1",
            preview.RootElement
                .GetProperty("format")
                .GetString());
    }

    [Fact]
    public async Task
        Inspect_RefusesPathCollisionsExistingDestinationsAndPasswordArguments()
    {
        using var workspace = new TemporaryDirectory();
        string sourcePath =
            workspace.PathFor("source.db");
        string packagePath =
            workspace.PathFor("retained.csdblitedb");
        string catalogPath =
            workspace.PathFor("catalog.json");
        _ = CreateDatabase(sourcePath);
        byte[] sourceBefore =
            await File.ReadAllBytesAsync(
                sourcePath,
                Cancellation);

        (string Package, string Catalog)[] collisions =
        [
            (sourcePath, catalogPath),
            (packagePath, sourcePath),
            (packagePath, packagePath),
        ];
        foreach ((string package, string catalog)
                 in collisions)
        {
            int code = await RunInspectAsync(
                sourcePath,
                package,
                catalog);
            Assert.Equal(
                InspectorCommandRunner.ExitUsage,
                code);
            Assert.Equal(
                sourceBefore,
                await File.ReadAllBytesAsync(
                    sourcePath,
                    Cancellation));
        }

        await File.WriteAllBytesAsync(
            packagePath,
            [0x01, 0x02],
            Cancellation);
        int existingPackageCode =
            await RunInspectAsync(
                sourcePath,
                packagePath,
                catalogPath);
        Assert.Equal(
            InspectorCommandRunner.ExitUsage,
            existingPackageCode);
        Assert.Equal(
            new byte[] { 0x01, 0x02 },
            await File.ReadAllBytesAsync(
                packagePath,
                Cancellation));
        File.Delete(packagePath);

        await File.WriteAllTextAsync(
            catalogPath,
            "existing catalog",
            Cancellation);
        int existingCatalogCode =
            await RunInspectAsync(
                sourcePath,
                packagePath,
                catalogPath);
        Assert.Equal(
            InspectorCommandRunner.ExitUsage,
            existingCatalogCode);
        Assert.False(File.Exists(packagePath));
        Assert.Equal(
            "existing catalog",
            await File.ReadAllTextAsync(
                catalogPath,
                Cancellation));

        var output = new StringWriter();
        var error = new StringWriter();
        int passwordCode =
            await MigrationCommandRunner.RunAsync(
                [
                    "migrate", "inspect",
                    "--source", "litedb",
                    "--input", sourcePath,
                    "--package", packagePath,
                    "--out", workspace.PathFor(
                        "new-catalog.json"),
                    "--password",
                    "do-not-print-this-secret",
                ],
                output,
                error,
                Cancellation);
        Assert.Equal(
            InspectorCommandRunner.ExitUsage,
            passwordCode);
        Assert.DoesNotContain(
            "do-not-print-this-secret",
            output.ToString(),
            StringComparison.Ordinal);
        Assert.DoesNotContain(
            "do-not-print-this-secret",
            error.ToString(),
            StringComparison.Ordinal);
        Assert.False(File.Exists(packagePath));
    }

    [Fact]
    public async Task
        RetainedSnapshot_ApplyResumeAndChecksumValidateAfterLiveSourceDeletion()
    {
        using var workspace = new TemporaryDirectory();
        LiteDbArtifacts artifacts =
            await CreateApplyReadyArtifactsAsync(
                workspace.Root);
        byte[] retainedBytes =
            await File.ReadAllBytesAsync(
                artifacts.PackagePath,
                Cancellation);
        File.Delete(artifacts.SourcePath);
        string targetPath =
            workspace.PathFor("staged.csdb");
        string runPath =
            workspace.PathFor("run.json");

        var applyOutput = new StringWriter();
        var applyError = new StringWriter();
        int applyCode =
            await MigrationCommandRunner.RunAsync(
                ApplyArguments(
                    artifacts,
                    targetPath,
                    runPath,
                    [
                        "--source-package",
                        artifacts.PackagePath,
                        "--expected-manifest-digest",
                        artifacts.ManifestDigest,
                        "--workspace",
                        workspace.Root,
                        "--format", "json",
                    ]),
                applyOutput,
                applyError,
                Cancellation);

        Assert.True(
            applyCode is
                InspectorCommandRunner.ExitOk or
                InspectorCommandRunner.ExitWarn,
            applyError.ToString());
        Assert.True(
            string.IsNullOrWhiteSpace(
                applyError.ToString()),
            applyError.ToString());
        Assert.False(
            File.Exists(artifacts.SourcePath));
        Assert.Equal(
            retainedBytes,
            await File.ReadAllBytesAsync(
                artifacts.PackagePath,
                Cancellation));
        using JsonDocument applied =
            JsonDocument.Parse(
                applyOutput.ToString());
        JsonElement appliedReport =
            applied.RootElement;
        Assert.Equal(
            LiteDbSnapshotPackageSession.Format,
            appliedReport
                .GetProperty("sourcePackageFormat")
                .GetString());
        Assert.Equal(
            artifacts.ManifestDigest,
            appliedReport
                .GetProperty(
                    "sourcePackageManifestDigest")
                .GetString());
        Assert.Equal(
            artifacts.Documents.Count,
            appliedReport
                .GetProperty("rowsWritten")
                .GetInt64());
        long firstBatches =
            appliedReport
                .GetProperty("batchesWritten")
                .GetInt64();
        Assert.True(firstBatches > 0);
        AssertNoLiteDbWorkspaceDirectories(
            workspace.Root);

        string resumePath =
            workspace.PathFor("run-resume.json");
        var resumeOutput = new StringWriter();
        var resumeError = new StringWriter();
        int resumeCode =
            await MigrationCommandRunner.RunAsync(
                ApplyArguments(
                    artifacts,
                    targetPath,
                    resumePath,
                    [
                        "--source-package",
                        artifacts.PackagePath,
                        "--expected-manifest-digest",
                        artifacts.ManifestDigest,
                        "--workspace",
                        workspace.Root,
                        "--resume",
                        "--format", "json",
                    ]),
                resumeOutput,
                resumeError,
                Cancellation);

        Assert.True(
            resumeCode is
                InspectorCommandRunner.ExitOk or
                InspectorCommandRunner.ExitWarn,
            resumeError.ToString());
        Assert.True(
            string.IsNullOrWhiteSpace(
                resumeError.ToString()),
            resumeError.ToString());
        using JsonDocument resumed =
            JsonDocument.Parse(
                resumeOutput.ToString());
        Assert.Equal(
            0,
            resumed.RootElement
                .GetProperty("batchesWritten")
                .GetInt64());
        Assert.Equal(
            firstBatches,
            resumed.RootElement
                .GetProperty("batchesSkipped")
                .GetInt64());
        Assert.Equal(
            artifacts.Documents.Count,
            resumed.RootElement
                .GetProperty("rowsSkipped")
                .GetInt64());
        AssertNoLiteDbWorkspaceDirectories(
            workspace.Root);

        string validationPath =
            workspace.PathFor("validation.json");
        var validationOutput = new StringWriter();
        var validationError = new StringWriter();
        int validationCode =
            await MigrationCommandRunner.RunAsync(
                [
                    "migrate", "validate",
                    artifacts.PlanPath,
                    "--catalog",
                    artifacts.CatalogPath,
                    "--source-package",
                    artifacts.PackagePath,
                    "--expected-manifest-digest",
                    artifacts.ManifestDigest,
                    "--workspace", workspace.Root,
                    "--target", targetPath,
                    "--out", validationPath,
                    "--level", "checksum",
                    "--spill-dir", workspace.Root,
                ],
                validationOutput,
                validationError,
                Cancellation);

        Assert.True(
            validationCode is
                InspectorCommandRunner.ExitOk or
                InspectorCommandRunner.ExitWarn,
            validationError.ToString());
        Assert.True(
            string.IsNullOrWhiteSpace(
                validationError.ToString()),
            validationError.ToString());
        Assert.Contains(
            "Activation: activated",
            validationOutput.ToString(),
            StringComparison.Ordinal);
        MigrationValidationReport validation =
            MigrationValidationReportSerializer
                .Deserialize(
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
            retainedBytes,
            await File.ReadAllBytesAsync(
                artifacts.PackagePath,
                Cancellation));
        AssertNoLiteDbWorkspaceDirectories(
            workspace.Root);

        await using Database database =
            await Database.OpenAsync(
                targetPath,
                Cancellation);
        CSharpDB.Engine.Collection<JsonElement>
            collection =
                await database
                    .GetCollectionAsync<JsonElement>(
                        artifacts
                            .TargetCollectionName,
                        Cancellation);
        foreach (ExpectedDocument expected
                 in artifacts.Documents)
        {
            JsonElement? stored =
                await collection.GetAsync(
                    expected.Key,
                    Cancellation);
            Assert.True(stored.HasValue);
            Assert.Equal(
                expected.CanonicalDocument,
                stored.Value.GetRawText());
        }
    }

    [Fact]
    public async Task
        RetainedSnapshot_IntegrityCatalogAndPolicyFailuresOccurBeforeTargetCreation()
    {
        using var workspace = new TemporaryDirectory();
        string firstRoot =
            workspace.PathFor("first");
        string secondRoot =
            workspace.PathFor("second");
        Directory.CreateDirectory(firstRoot);
        Directory.CreateDirectory(secondRoot);
        LiteDbArtifacts first =
            await CreateApplyReadyArtifactsAsync(
                firstRoot);
        LiteDbArtifacts second =
            await CreateApplyReadyArtifactsAsync(
                secondRoot,
                numericId: 10);
        string targetPath =
            workspace.PathFor("staged.csdb");

        string wrongDigest =
            DifferentDigest(first.ManifestDigest);
        string wrongDigestReport =
            workspace.PathFor(
                "wrong-digest-run.json");
        var wrongDigestError =
            new StringWriter();
        int wrongDigestCode =
            await MigrationCommandRunner.RunAsync(
                ApplyArguments(
                    first,
                    targetPath,
                    wrongDigestReport,
                    [
                        "--source-package",
                        first.PackagePath,
                        "--expected-manifest-digest",
                        wrongDigest,
                        "--workspace",
                        workspace.Root,
                    ]),
                TextWriter.Null,
                wrongDigestError,
                Cancellation);
        Assert.Equal(
            InspectorCommandRunner.ExitError,
            wrongDigestCode);
        Assert.Contains(
            "digest",
            wrongDigestError.ToString(),
            StringComparison.OrdinalIgnoreCase);
        AssertTargetWasNotCreated(
            targetPath,
            wrongDigestReport);
        AssertNoLiteDbWorkspaceDirectories(
            workspace.Root);

        string sizeReport =
            workspace.PathFor("size-run.json");
        var sizeError = new StringWriter();
        int sizeCode =
            await MigrationCommandRunner.RunAsync(
                ApplyArguments(
                    first,
                    targetPath,
                    sizeReport,
                    [
                        "--source-package",
                        first.PackagePath,
                        "--expected-manifest-digest",
                        first.ManifestDigest,
                        "--workspace",
                        workspace.Root,
                        "--max-source-bytes", "1",
                    ]),
                TextWriter.Null,
                sizeError,
                Cancellation);
        Assert.Equal(
            InspectorCommandRunner.ExitError,
            sizeCode);
        Assert.Contains(
            "byte limit",
            sizeError.ToString(),
            StringComparison.OrdinalIgnoreCase);
        AssertTargetWasNotCreated(
            targetPath,
            sizeReport);
        AssertNoLiteDbWorkspaceDirectories(
            workspace.Root);

        await using (var package =
                     new FileStream(
                         first.PackagePath,
                         FileMode.Append,
                         FileAccess.Write,
                         FileShare.None))
        {
            await package.WriteAsync(
                new byte[] { 0x5a },
                Cancellation);
        }
        string tamperReport =
            workspace.PathFor("tamper-run.json");
        var tamperError = new StringWriter();
        int tamperCode =
            await MigrationCommandRunner.RunAsync(
                ApplyArguments(
                    first,
                    targetPath,
                    tamperReport,
                    [
                        "--source-package",
                        first.PackagePath,
                        "--expected-manifest-digest",
                        first.ManifestDigest,
                        "--workspace",
                        workspace.Root,
                    ]),
                TextWriter.Null,
                tamperError,
                Cancellation);
        Assert.Equal(
            InspectorCommandRunner.ExitError,
            tamperCode);
        Assert.Contains(
            "digest",
            tamperError.ToString(),
            StringComparison.OrdinalIgnoreCase);
        AssertTargetWasNotCreated(
            targetPath,
            tamperReport);
        AssertNoLiteDbWorkspaceDirectories(
            workspace.Root);

        string mismatchReport =
            workspace.PathFor(
                "catalog-mismatch-run.json");
        var mismatchError = new StringWriter();
        int mismatchCode =
            await MigrationCommandRunner.RunAsync(
                ApplyArguments(
                    first,
                    targetPath,
                    mismatchReport,
                    [
                        "--source-package",
                        second.PackagePath,
                        "--expected-manifest-digest",
                        second.ManifestDigest,
                        "--workspace",
                        workspace.Root,
                    ]),
                TextWriter.Null,
                mismatchError,
                Cancellation);
        Assert.Equal(
            InspectorCommandRunner.ExitError,
            mismatchCode);
        Assert.Contains(
            "catalog",
            mismatchError.ToString(),
            StringComparison.OrdinalIgnoreCase);
        AssertTargetWasNotCreated(
            targetPath,
            mismatchReport);
        AssertNoLiteDbWorkspaceDirectories(
            workspace.Root);

        string deterministicPlanPath =
            workspace.PathFor(
                "deterministic-plan.json");
        var deterministicError =
            new StringWriter();
        int deterministicCode =
            await MigrationCommandRunner.RunAsync(
                [
                    "migrate", "plan",
                    second.CatalogPath,
                    "--out", deterministicPlanPath,
                    "--accept-exclusions", "all",
                    "--reject-mode", "deterministic",
                    "--reject-rules", "all",
                    "--max-rejected-rows-per-batch",
                    "100",
                    "--max-rejected-rows-per-run",
                    "10000",
                    "--max-reject-evidence-value-bytes",
                    "4096",
                    "--max-reject-evidence-bytes-per-batch",
                    "65536",
                    "--max-reject-evidence-bytes-per-run",
                    "1048576",
                    "--max-reject-artifact-bytes",
                    "16777216",
                ],
                TextWriter.Null,
                deterministicError,
                Cancellation);
        Assert.Equal(
            InspectorCommandRunner.ExitUsage,
            deterministicCode);
        Assert.Contains(
            "Deterministic rejects are not supported",
            deterministicError.ToString(),
            StringComparison.Ordinal);
        Assert.False(
            File.Exists(deterministicPlanPath));

        MigrationCatalog supportedCatalog =
            MigrationArtifactSerializer
                .DeserializeCatalog(
                    await File.ReadAllTextAsync(
                        second.CatalogPath,
                        Cancellation));
        MigrationCatalog futureCatalog =
            supportedCatalog with
            {
                Objects = supportedCatalog.Objects
                    .Select(item =>
                        item.Kind !=
                            MigrationObjectKind.Namespace
                            ? item
                            : item with
                            {
                                Facets = item.Facets
                                    .Select(facet =>
                                        facet.Name !=
                                            "liteDbCatalogContract"
                                            ? facet
                                            : facet with
                                            {
                                                Value =
                                                    "csharpdb-litedb-catalog/v2",
                                            })
                                    .ToArray(),
                            })
                    .ToArray(),
            };
        string futureCatalogPath =
            workspace.PathFor("future-catalog.json");
        await File.WriteAllTextAsync(
            futureCatalogPath,
            MigrationArtifactSerializer
                .SerializeCatalog(futureCatalog),
            Cancellation);
        string futurePlanPath =
            workspace.PathFor("future-plan.json");
        var futurePlanError = new StringWriter();
        int futurePlanCode =
            await MigrationCommandRunner.RunAsync(
                [
                    "migrate", "plan",
                    futureCatalogPath,
                    "--out", futurePlanPath,
                ],
                TextWriter.Null,
                futurePlanError,
                Cancellation);
        Assert.Equal(
            InspectorCommandRunner.ExitUsage,
            futurePlanCode);
        Assert.Contains(
            "LiteDB catalog contract v1",
            futurePlanError.ToString(),
            StringComparison.Ordinal);
        Assert.False(File.Exists(futurePlanPath));

        var futurePreviewError =
            new StringWriter();
        int futurePreviewCode =
            await MigrationCommandRunner.RunAsync(
                [
                    "migrate", "preview",
                    second.PlanPath,
                    "--catalog",
                    futureCatalogPath,
                ],
                TextWriter.Null,
                futurePreviewError,
                Cancellation);
        Assert.Equal(
            InspectorCommandRunner.ExitUsage,
            futurePreviewCode);
        Assert.Contains(
            "LiteDB catalog contract v1",
            futurePreviewError.ToString(),
            StringComparison.Ordinal);
    }

    private static async ValueTask<int> RunInspectAsync(
        string sourcePath,
        string packagePath,
        string catalogPath) =>
        await MigrationCommandRunner.RunAsync(
            [
                "migrate", "inspect",
                "--source", "litedb",
                "--input", sourcePath,
                "--package", packagePath,
                "--out", catalogPath,
            ],
            TextWriter.Null,
            TextWriter.Null,
            Cancellation);

    private static async ValueTask<LiteDbArtifacts>
        CreateApplyReadyArtifactsAsync(
        string directory,
        int numericId = 1)
    {
        string sourcePath =
            Path.Combine(directory, "source.db");
        string packagePath =
            Path.Combine(
                directory,
                "retained.csdblitedb");
        string catalogPath =
            Path.Combine(directory, "catalog.json");
        string planPath =
            Path.Combine(directory, "plan.json");
        IReadOnlyList<ExpectedDocument> documents =
            CreateDatabase(
                sourcePath,
                numericId);

        var inspectOutput = new StringWriter();
        var inspectError = new StringWriter();
        int inspectCode =
            await MigrationCommandRunner.RunAsync(
                [
                    "migrate", "inspect",
                    "--source", "litedb",
                    "--input", sourcePath,
                    "--package", packagePath,
                    "--out", catalogPath,
                ],
                inspectOutput,
                inspectError,
                Cancellation);
        AssertSuccessfulCommand(
            inspectCode,
            inspectError);
        string digest =
            ReadManifestDigest(
                inspectOutput.ToString());

        var planError = new StringWriter();
        int planCode =
            await MigrationCommandRunner.RunAsync(
                [
                    "migrate", "plan", catalogPath,
                    "--out", planPath,
                    "--accept-exclusions", "all",
                ],
                TextWriter.Null,
                planError,
                Cancellation);
        AssertSuccessfulCommand(planCode, planError);

        MigrationCatalog catalog =
            MigrationArtifactSerializer
                .DeserializeCatalog(
                    await File.ReadAllTextAsync(
                        catalogPath,
                        Cancellation));
        MigrationPlan plan =
            MigrationArtifactSerializer
                .DeserializePlan(
                    await File.ReadAllTextAsync(
                        planPath,
                        Cancellation),
                    catalog);
        string collectionId =
            catalog.Objects.Single(item =>
                item.Kind ==
                    MigrationObjectKind.Collection &&
                item.SourceName == "documents")
                .ObjectId;
        string targetCollectionName =
            plan.Objects.Single(item =>
                item.SourceObjectId ==
                    collectionId)
                .TargetName ??
            throw new InvalidDataException(
                "The LiteDB fixture collection was excluded.");

        return new LiteDbArtifacts(
            sourcePath,
            packagePath,
            catalogPath,
            planPath,
            digest,
            targetCollectionName,
            documents);
    }

    private static IReadOnlyList<ExpectedDocument>
        CreateDatabase(
        string path,
        int numericId = 1)
    {
        var documents =
            new[]
            {
                new BsonDocument
                {
                    ["_id"] = numericId,
                    ["value"] = "first",
                    ["secret"] =
                        PrivateDocumentValue,
                    ["nested"] =
                        new BsonDocument
                        {
                            ["enabled"] = true,
                            ["amount"] = 42L,
                        },
                },
                new BsonDocument
                {
                    ["_id"] = numericId + 1,
                    ["value"] = "second",
                    ["binary"] =
                        new byte[] { 0x00, 0x7f, 0xff },
                    ["values"] =
                        new BsonArray
                        {
                            1,
                            "one",
                            BsonValue.Null,
                        },
                },
                new BsonDocument
                {
                    ["_id"] =
                        numericId.ToString(
                            System.Globalization
                                .CultureInfo.InvariantCulture),
                    ["value"] = "text-key",
                    ["guid"] =
                        Guid.Parse(
                            "d676a64c-29bd-4aaf-8f4f-14f5c37dd802"),
                },
            };
        using (var database =
               new LiteDatabase(
                   new ConnectionString
                   {
                       Filename = path,
                       Connection =
                           ConnectionType.Direct,
                   }))
        {
            ILiteCollection<BsonDocument>
                collection =
                    database.GetCollection<
                        BsonDocument>(
                        "documents");
            foreach (BsonDocument document
                     in documents)
            {
                collection.Insert(document);
            }
            database.Checkpoint();
        }

        return documents.Select(document =>
                new ExpectedDocument(
                    LiteDbCanonicalBsonCodec
                        .EncodeTypedKey(
                            document["_id"]),
                    LiteDbCanonicalBsonCodec
                        .EncodeDocument(document)))
            .ToArray();
    }

    private static string[] ApplyArguments(
        LiteDbArtifacts artifacts,
        string targetPath,
        string reportPath,
        IReadOnlyList<string> suffix)
    {
        var arguments = new List<string>
        {
            "migrate", "apply",
            artifacts.PlanPath,
            "--catalog", artifacts.CatalogPath,
            "--target", targetPath,
            "--out", reportPath,
        };
        arguments.AddRange(suffix);
        return arguments.ToArray();
    }

    private static string ReadManifestDigest(
        string output)
    {
        Match match =
            ManifestDigestPattern().Match(output);
        Assert.True(
            match.Success,
            $"No manifest digest was emitted: {output}");
        return match.Groups[1].Value;
    }

    private static string DifferentDigest(
        string digest)
    {
        Assert.Matches(
            "^sha256:[0-9a-f]{64}$",
            digest);
        char replacement =
            digest[7] == '0' ? '1' : '0';
        return digest[..7] +
            replacement +
            digest[8..];
    }

    private static void AssertSuccessfulCommand(
        int exitCode,
        StringWriter error)
    {
        Assert.True(
            exitCode is
                InspectorCommandRunner.ExitOk or
                InspectorCommandRunner.ExitWarn,
            error.ToString());
        Assert.True(
            string.IsNullOrWhiteSpace(
                error.ToString()),
            error.ToString());
    }

    private static void AssertTargetWasNotCreated(
        string targetPath,
        string reportPath)
    {
        Assert.False(File.Exists(targetPath));
        Assert.False(
            File.Exists(targetPath + ".wal"));
        Assert.False(
            File.Exists(
                targetPath +
                ".migration.lock"));
        Assert.False(File.Exists(reportPath));
    }

    private static void
        AssertNoLiteDbWorkspaceDirectories(
        string directory) =>
        Assert.Empty(
            Directory.EnumerateDirectories(
                directory,
                "csharpdb-litedb-*"));

    [GeneratedRegex(
        @"(?:^|\|\s*)manifestDigest=(sha256:[0-9a-f]{64})(?:\s*\||\s*$)",
        RegexOptions.CultureInvariant)]
    private static partial Regex
        ManifestDigestPattern();

    private sealed record ExpectedDocument(
        string Key,
        string CanonicalDocument);

    private sealed record LiteDbArtifacts(
        string SourcePath,
        string PackagePath,
        string CatalogPath,
        string PlanPath,
        string ManifestDigest,
        string TargetCollectionName,
        IReadOnlyList<ExpectedDocument>
            Documents);

    private sealed class TemporaryDirectory :
        IDisposable
    {
        internal TemporaryDirectory()
        {
            Root = Path.Combine(
                Path.GetTempPath(),
                "csharpdb_litedb_cli_" +
                Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(Root);
        }

        internal string Root { get; }

        internal string PathFor(string name) =>
            Path.Combine(Root, name);

        public void Dispose()
        {
            try
            {
                Directory.Delete(
                    Root,
                    recursive: true);
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
