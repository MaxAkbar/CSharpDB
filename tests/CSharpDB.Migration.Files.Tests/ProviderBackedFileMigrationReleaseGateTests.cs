using System.Globalization;
using System.Text;
using CSharpDB.Migration.CSharpDb;
using CSharpDB.Migration.Files.Csv;
using CSharpDB.Migration.Files.Json;
using CSharpDB.Migration.Validation;

namespace CSharpDB.Migration.Files.Tests;

public sealed class ProviderBackedFileMigrationReleaseGateTests
{
    private const int RowCount = 193;
    private const int BatchSize = 32;
    private const int ExpectedBatchCount = (RowCount + BatchSize - 1) / BatchSize;
    private const long MaxSourceBytes = 4L * 1024 * 1024;
    private const long MaxSpillBytes = 16L * 1024 * 1024;

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task RetainedCsv_CancelledApplyResumesAndChecksumActivatesRealTarget()
    {
        using var workspace = new ReleaseGateWorkspace("csv");
        CsvSnapshotPackageManifest manifest = await WriteCsvPackageAsync(workspace);
        File.Delete(workspace.SourcePath);

        MigrationCatalog catalog;
        MigrationPlan plan;
        string targetIdentity;
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        var injector = new CancelBatchBeforeCommitFaultInjector(
            batchOrdinal: 2,
            cancellation);

        await using (CsvSnapshotPackageSession session =
                     await OpenCsvPackageAsync(workspace, manifest.ManifestDigest))
        {
            catalog = session.Catalog;
            plan = CreateReadyPlan(catalog);
            await using CSharpDbStagedMigrationTarget target =
                await CSharpDbStagedMigrationTarget.CreateNewAsync(
                    workspace.TargetPath,
                    plan,
                    catalog,
                    session.DataSource.SnapshotIdentity,
                    injector,
                    Ct);
            targetIdentity = target.TargetIdentity;

            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                async () => await ApplyAsync(
                    plan,
                    catalog,
                    session.DataSource,
                    target,
                    cancellation.Token));
            Assert.True(injector.Fired);
        }

        Assert.False(File.Exists(workspace.TargetPath + ".migration.lock"));
        await using (CsvSnapshotPackageSession session =
                     await OpenCsvPackageAsync(workspace, manifest.ManifestDigest))
        await using (CSharpDbStagedMigrationTarget target =
                     await CSharpDbStagedMigrationTarget.OpenResumeAsync(
                         workspace.TargetPath,
                         plan,
                         catalog,
                         session.DataSource.SnapshotIdentity,
                         cancellationToken: Ct))
        {
            Assert.Equal(targetIdentity, target.TargetIdentity);
            Assert.Equal(
                MigrationArtifactSerializer.ComputeCatalogDigest(catalog),
                MigrationArtifactSerializer.ComputeCatalogDigest(session.Catalog));

            MigrationApplyResult resumed = await ApplyAsync(
                plan,
                catalog,
                session.DataSource,
                target,
                Ct);

            Assert.Equal(MigrationApplyStatus.AwaitingValidation, resumed.Status);
            Assert.Equal(2, resumed.BatchesSkipped);
            Assert.Equal(ExpectedBatchCount - 2, resumed.BatchesWritten);
            Assert.Equal(2 * BatchSize, resumed.RowsSkipped);
            Assert.Equal(RowCount - (2 * BatchSize), resumed.RowsWritten);

            await AssertChecksumActivationAsync(
                workspace,
                plan,
                catalog,
                session.DataSource,
                target,
                CsvMigrationObjectIds.Table);
        }

        AssertReleaseArtifactsClosed(workspace);
    }

    [Theory]
    [InlineData(JsonInputFraming.RootArray)]
    [InlineData(JsonInputFraming.MultipleValues)]
    public async Task RetainedJson_ApplyResumeAndChecksumActivateRealTarget(
        JsonInputFraming framing)
    {
        using var workspace = new ReleaseGateWorkspace(
            framing == JsonInputFraming.RootArray ? "json" : "ndjson");
        JsonSnapshotPackageManifest manifest =
            await WriteJsonPackageAsync(workspace, framing);
        File.Delete(workspace.SourcePath);

        MigrationCatalog catalog;
        MigrationPlan plan;
        string targetIdentity;

        await using (JsonSnapshotPackageSession session =
                     await OpenJsonPackageAsync(workspace, manifest.ManifestDigest))
        {
            catalog = session.Catalog;
            plan = CreateReadyPlan(catalog);
            await using CSharpDbStagedMigrationTarget target =
                await CSharpDbStagedMigrationTarget.CreateNewAsync(
                    workspace.TargetPath,
                    plan,
                    catalog,
                    session.DataSource.SnapshotIdentity,
                    cancellationToken: Ct);
            targetIdentity = target.TargetIdentity;

            MigrationApplyResult applied = await ApplyAsync(
                plan,
                catalog,
                session.DataSource,
                target,
                Ct);

            Assert.Equal(MigrationApplyStatus.AwaitingValidation, applied.Status);
            Assert.Equal(ExpectedBatchCount, applied.BatchesWritten);
            Assert.Equal(0, applied.BatchesSkipped);
            Assert.Equal(RowCount, applied.RowsWritten);
            Assert.Equal(0, applied.RowsSkipped);
        }

        Assert.False(File.Exists(workspace.TargetPath + ".migration.lock"));
        await using (JsonSnapshotPackageSession session =
                     await OpenJsonPackageAsync(workspace, manifest.ManifestDigest))
        await using (CSharpDbStagedMigrationTarget target =
                     await CSharpDbStagedMigrationTarget.OpenResumeAsync(
                         workspace.TargetPath,
                         plan,
                         catalog,
                         session.DataSource.SnapshotIdentity,
                         cancellationToken: Ct))
        {
            Assert.Equal(targetIdentity, target.TargetIdentity);
            Assert.Equal(
                MigrationArtifactSerializer.ComputeCatalogDigest(catalog),
                MigrationArtifactSerializer.ComputeCatalogDigest(session.Catalog));

            MigrationApplyResult replayed = await ApplyAsync(
                plan,
                catalog,
                session.DataSource,
                target,
                Ct);

            Assert.Equal(MigrationApplyStatus.AwaitingValidation, replayed.Status);
            Assert.Equal(0, replayed.BatchesWritten);
            Assert.Equal(ExpectedBatchCount, replayed.BatchesSkipped);
            Assert.Equal(0, replayed.RowsWritten);
            Assert.Equal(RowCount, replayed.RowsSkipped);

            await AssertChecksumActivationAsync(
                workspace,
                plan,
                catalog,
                session.DataSource,
                target,
                JsonMigrationObjectIds.Table);
        }

        AssertReleaseArtifactsClosed(workspace);
    }

    private static async ValueTask<CsvSnapshotPackageManifest> WriteCsvPackageAsync(
        ReleaseGateWorkspace workspace)
    {
        await File.WriteAllTextAsync(
            workspace.SourcePath,
            BuildCsv(),
            new UTF8Encoding(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true),
            Ct);
        await using CsvSourceSnapshot snapshot =
            await CsvSourceSnapshot.CreateFromFileAsync(
                workspace.SourcePath,
                new CsvSourceSnapshotOptions
                {
                    WorkspacePath = workspace.Root,
                    MaxSourceBytes = MaxSourceBytes,
                },
                Ct);
        var readerOptions = new CsvReaderOptions();
        CsvFormatInspection inspection = await CsvFormatInspector.InspectAsync(
            snapshot,
            readerOptions,
            new CsvInspectionOptions { DelimiterCandidates = [","] },
            Ct);
        CsvSourceBinding binding = await CsvSourceBinding.CreateAsync(
            snapshot,
            inspection,
            "release-gate/csv",
            Ct);
        CsvSchemaInferenceResult schema = await CsvSchemaInferer.InferAsync(
            binding,
            snapshot,
            RowCount + 1,
            new CsvSchemaInferenceOptions { TableName = "release_csv" },
            Ct);
        return await CsvSnapshotPackage.WriteAsync(
            workspace.PackagePath,
            snapshot,
            schema,
            CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
            Ct);
    }

    private static async ValueTask<JsonSnapshotPackageManifest> WriteJsonPackageAsync(
        ReleaseGateWorkspace workspace,
        JsonInputFraming framing)
    {
        await File.WriteAllTextAsync(
            workspace.SourcePath,
            BuildJson(framing),
            new UTF8Encoding(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true),
            Ct);
        await using JsonSourceSnapshot snapshot =
            await JsonSourceSnapshot.CreateFromFileAsync(
                workspace.SourcePath,
                new JsonSourceSnapshotOptions
                {
                    WorkspacePath = workspace.Root,
                    MaxSourceBytes = MaxSourceBytes,
                },
                Ct);
        JsonSourceBinding binding = await JsonSourceBinding.CreateAsync(
            snapshot,
            new JsonStreamingReaderOptions
            {
                Framing = framing,
                MaxValueBytes = 16 * 1024,
                MaxDepth = 16,
                MaxPropertiesPerObject = 16,
                MaxArrayElements = 32,
                MaxTotalNodes = 64,
                MaxPropertyNameBytes = 1024,
                MaxStringBytes = 8 * 1024,
                MaxNumberBytes = 1024,
                LeaveOpen = true,
            },
            "release-gate/" +
            (framing == JsonInputFraming.RootArray ? "json" : "ndjson"),
            Ct);
        JsonTableSchemaInferenceResult schema =
            await JsonTableSchemaInferer.InferAsync(
                binding,
                snapshot,
                RowCount + 1,
                new JsonTableSchemaInferenceOptions
                {
                    TableName = "release_json",
                    MaxColumns = 16,
                    MaxTotalColumnNameBytes = 4 * 1024,
                    MaxProfileBytes = 256 * 1024,
                },
                Ct);
        return await JsonSnapshotPackage.WriteAsync(
            workspace.PackagePath,
            snapshot,
            schema,
            CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
            Ct);
    }

    private static async ValueTask<CsvSnapshotPackageSession> OpenCsvPackageAsync(
        ReleaseGateWorkspace workspace,
        string manifestDigest) =>
        await CsvSnapshotPackage.OpenAsync(
            workspace.PackagePath,
            new CsvSnapshotPackageOpenOptions
            {
                WorkspacePath = workspace.Root,
                MaxSourceBytes = MaxSourceBytes,
                ExpectedManifestDigest = manifestDigest,
            },
            Ct);

    private static async ValueTask<JsonSnapshotPackageSession> OpenJsonPackageAsync(
        ReleaseGateWorkspace workspace,
        string manifestDigest) =>
        await JsonSnapshotPackage.OpenAsync(
            workspace.PackagePath,
            new JsonSnapshotPackageOpenOptions
            {
                WorkspacePath = workspace.Root,
                MaxSourceBytes = MaxSourceBytes,
                ExpectedManifestDigest = manifestDigest,
            },
            Ct);

    private static MigrationPlan CreateReadyPlan(MigrationCatalog catalog)
    {
        MigrationPlan draft = new MigrationPlanner().CreatePlan(
            catalog,
            new MigrationPlanningOptions
            {
                AcceptAllExclusions = true,
                Load = new MigrationLoadPolicy
                {
                    BatchSize = BatchSize,
                    MaxBatchBytes = 64 * 1024,
                    MaxValueBytes = 16 * 1024,
                },
            });
        return CSharpDbDdlPreviewBuilder.BuildAndAttachGeneratedDdlDigestBounded(
            draft,
            catalog,
            cancellationToken: Ct);
    }

    private static async ValueTask<MigrationApplyResult> ApplyAsync(
        MigrationPlan plan,
        MigrationCatalog catalog,
        IMigrationDataSource source,
        CSharpDbStagedMigrationTarget target,
        CancellationToken cancellationToken) =>
        await new MigrationApplyRunner().ApplyAsync(
            new MigrationApplyRequest
            {
                Plan = plan,
                Catalog = catalog,
                Source = source,
                Target = target,
            },
            cancellationToken);

    private static async ValueTask AssertChecksumActivationAsync(
        ReleaseGateWorkspace workspace,
        MigrationPlan plan,
        MigrationCatalog catalog,
        IMigrationDataSource source,
        CSharpDbStagedMigrationTarget target,
        string sourceObjectId)
    {
        await using var sourceSnapshot =
            new MigrationDataSourceValidationSnapshot(plan, catalog, source);
        MigrationValidationRunResult validation =
            await new MigrationValidationRunner().ValidateAsync(
                new MigrationValidationRunRequest
                {
                    Plan = plan,
                    Catalog = catalog,
                    SourceSnapshot = sourceSnapshot,
                    Target = target,
                    Level = MigrationValidationLevel.Checksum,
                    ReportOutputPath = workspace.ReportPath,
                    ChecksumOptions = new PartitionedChecksumValidatorOptions
                    {
                        SpillRootDirectory = workspace.Root,
                        SortMemoryBudgetBytes = 256 * 1024,
                        MaxSpillBytes = MaxSpillBytes,
                        MaxOpenPartitionWriters = 8,
                    },
                },
                Ct);

        Assert.Equal(MigrationValidationStatus.Passed, validation.Report.Outcome);
        Assert.Equal(MigrationValidationLevel.Checksum, validation.Report.Level);
        Assert.True(validation.Activated);
        Assert.InRange(validation.PeakSpillBytes, 0, MaxSpillBytes);
        Assert.True(File.Exists(workspace.ReportPath));

        MigrationObjectValidationEvidence evidence = Assert.Single(
            validation.Report.Objects,
            item => string.Equals(
                item.SourceObjectId,
                sourceObjectId,
                StringComparison.Ordinal));
        Assert.Equal(MigrationValidationStatus.Passed, evidence.Status);
        Assert.Equal(RowCount, evidence.SourceRowCount);
        Assert.Equal(RowCount, evidence.TargetRowCount);
        Assert.NotNull(evidence.SourceChecksum);
        Assert.Equal(evidence.SourceChecksum, evidence.TargetChecksum);
        Assert.Equal(256, evidence.Partitions.Count);
        Assert.All(
            evidence.Partitions,
            partition => Assert.Equal(
                MigrationValidationStatus.Passed,
                partition.Status));
    }

    private static string BuildCsv()
    {
        var text = new StringBuilder("id,name,score\n");
        for (int id = 1; id <= RowCount; id++)
        {
            text.Append(id.ToString(CultureInfo.InvariantCulture));
            if (id == 17)
                text.Append(",\"multiline\nname\",");
            else if (id == 29)
                text.Append(",\"name, with comma\",");
            else
                text.Append(",name-").Append(id.ToString("D4", CultureInfo.InvariantCulture)).Append(',');
            text.Append((id * 3).ToString(CultureInfo.InvariantCulture)).Append('\n');
        }
        return text.ToString();
    }

    private static string BuildJson(JsonInputFraming framing)
    {
        var values = new string[RowCount];
        for (int index = 0; index < values.Length; index++)
        {
            int id = index + 1;
            values[index] =
                "{\"id\":" + id.ToString(CultureInfo.InvariantCulture) +
                ",\"name\":\"name-" + id.ToString("D4", CultureInfo.InvariantCulture) +
                "\",\"score\":" + (id * 3).ToString(CultureInfo.InvariantCulture) +
                "}";
        }

        return framing switch
        {
            JsonInputFraming.RootArray =>
                "[\n" + string.Join(",\n", values) + "\n]\n",
            JsonInputFraming.MultipleValues =>
                string.Join("\n", values) + "\n",
            _ => throw new ArgumentOutOfRangeException(nameof(framing)),
        };
    }

    private static void AssertReleaseArtifactsClosed(ReleaseGateWorkspace workspace)
    {
        Assert.False(File.Exists(workspace.TargetPath + ".migration.lock"));
        Assert.Empty(
            Directory.EnumerateDirectories(
                workspace.Root,
                "csharpdb-validation-*",
                SearchOption.TopDirectoryOnly));
        Assert.True(File.Exists(workspace.PackagePath));
        Assert.True(File.Exists(workspace.TargetPath));
        Assert.True(File.Exists(workspace.ReportPath));
    }

    private sealed class CancelBatchBeforeCommitFaultInjector(
        long batchOrdinal,
        CancellationTokenSource cancellation) : ICSharpDbMigrationFaultInjector
    {
        private int _fired;

        public bool Fired => Volatile.Read(ref _fired) != 0;

        public ValueTask InjectAsync(
            CSharpDbMigrationFaultPoint point,
            MigrationTargetBatch batch,
            CancellationToken cancellationToken = default)
        {
            if (point == CSharpDbMigrationFaultPoint.AfterReceiptBeforeCommit &&
                batch.BatchOrdinal == batchOrdinal &&
                Interlocked.Exchange(ref _fired, 1) == 0)
            {
                cancellation.Cancel();
            }
            return ValueTask.CompletedTask;
        }
    }

    private sealed class ReleaseGateWorkspace : IDisposable
    {
        public ReleaseGateWorkspace(string sourceKind)
        {
            Root = Path.Combine(
                Path.GetTempPath(),
                $"csharpdb-file-release-gate-{sourceKind}-{Guid.NewGuid():N}");
            Directory.CreateDirectory(Root);
            SourcePath = Path.Combine(
                Root,
                sourceKind == "ndjson" ? "source.ndjson" : $"source.{sourceKind}");
            PackagePath = Path.Combine(
                Root,
                sourceKind == "csv"
                    ? "source" + CsvSnapshotPackage.FileExtension
                    : "source" + JsonSnapshotPackage.FileExtension);
            TargetPath = Path.Combine(Root, "staged.csdb");
            ReportPath = Path.Combine(Root, "validation.json");
        }

        public string Root { get; }

        public string SourcePath { get; }

        public string PackagePath { get; }

        public string TargetPath { get; }

        public string ReportPath { get; }

        public void Dispose()
        {
            try
            {
                Directory.Delete(Root, recursive: true);
            }
            catch (DirectoryNotFoundException)
            {
            }
        }
    }
}
