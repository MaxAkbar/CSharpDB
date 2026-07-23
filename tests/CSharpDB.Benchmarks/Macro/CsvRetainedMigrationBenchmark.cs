using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;
using CSharpDB.Benchmarks.Infrastructure;
using CSharpDB.Migration;
using CSharpDB.Migration.CSharpDb;
using CSharpDB.Migration.Files.Csv;
using CSharpDB.Migration.Validation;
using CSharpDB.Primitives;

namespace CSharpDB.Benchmarks.Macro;

/// <summary>
/// Qualifies the retained CSV path from immutable inspection and packaging
/// through full replay, staged apply, and a fresh-session resume. Timing and
/// process-memory values are diagnostic only; deterministic artifact, row,
/// receipt, cursor, and live-batch invariants are the pass/fail contract.
/// </summary>
public static class CsvRetainedMigrationBenchmark
{
    private const int BatchSize = 1_000;
    private const int InspectionRows = 1_000;
    private const long MaxBatchBytes = 4L * 1024 * 1024;
    private const int MaxValueBytes = 1_024;
    private const int CopyBufferBytes = 128 * 1024;
    private const string SourceIdentity = "benchmark/csv-retained-v1";

    // Exactly 64 UTF-8 bytes after CSV decoding. Every logical row exercises
    // both a quoted multiline field and an escaped quote.
    private static readonly string s_payload =
        new string('m', 29) + "\r\n\"" + new string('m', 32);

    private static readonly string s_encodedRow =
        '"' + s_payload.Replace("\"", "\"\"") + "\"\r\n";

    private static readonly int s_canonicalValueBytes =
        MigrationValueConverter.GetCanonicalByteCount(DbValue.FromText(s_payload));

    private static readonly Scenario[] s_scenarios =
    [
        new("Rows100K_Batch1000_Text64", RowCount: 100_000),
        new("Rows1M_Batch1000_Text64", RowCount: 1_000_000),
    ];

    public static async Task<List<BenchmarkResult>> RunAsync()
    {
        var results = new List<BenchmarkResult>(s_scenarios.Length * 6);
        foreach (Scenario scenario in s_scenarios)
            results.AddRange(await RunScenarioAsync(scenario).ConfigureAwait(false));
        return results;
    }

    public static Task<List<BenchmarkResult>> RunNamedScenarioAsync(string scenarioName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(scenarioName);
        Scenario? scenario = s_scenarios.FirstOrDefault(
            item => item.Name.Equals(scenarioName, StringComparison.OrdinalIgnoreCase));
        if (scenario is null)
        {
            throw new ArgumentException(
                $"Unknown retained CSV migration scenario '{scenarioName}'.",
                nameof(scenarioName));
        }

        return RunScenarioAsync(scenario);
    }

    private static async Task<List<BenchmarkResult>> RunScenarioAsync(Scenario scenario)
    {
        CancellationToken cancellationToken = CancellationToken.None;
        string rootPath = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_csv_retained_benchmark_{Guid.NewGuid():N}");
        string workspacePath = Path.Combine(rootPath, "workspace");
        string sourcePath = Path.Combine(rootPath, "source.csv");
        string packagePath = Path.Combine(rootPath, "source.csdbcsv");
        string targetPath = Path.Combine(rootPath, "target.csdb");
        Directory.CreateDirectory(rootPath);
        Directory.CreateDirectory(workspacePath);

        try
        {
            await WriteSourceAsync(sourcePath, scenario.RowCount, cancellationToken)
                .ConfigureAwait(false);
            long sourceBytes = new FileInfo(sourcePath).Length;
            long expectedSourceBytes = checked(7L + (long)scenario.RowCount * s_encodedRow.Length);
            Require(
                sourceBytes == expectedSourceBytes,
                $"Generated CSV length {sourceBytes} did not match {expectedSourceBytes} bytes.");

            Timed<InspectPackageArtifacts> inspectPackage = await MeasureAsync(
                    () => InspectAndPackageAsync(
                        sourcePath,
                        packagePath,
                        workspacePath,
                        sourceBytes,
                        cancellationToken))
                .ConfigureAwait(false);
            InspectPackageArtifacts artifacts = inspectPackage.Value;
            Require(artifacts.Manifest.ContentLength == sourceBytes,
                "The retained manifest content length does not match the raw CSV.");
            Require(
                string.Equals(
                    artifacts.Manifest.CatalogDigest,
                    MigrationArtifactSerializer.ComputeCatalogDigest(artifacts.Catalog),
                    StringComparison.Ordinal),
                "The retained manifest catalog digest does not match inspection.");

            long packageBytes = new FileInfo(packagePath).Length;
            Require(packageBytes > sourceBytes, "The retained package is missing its envelope.");
            string packageDigestBefore = await ComputeFileDigestAsync(packagePath, cancellationToken)
                .ConfigureAwait(false);

            File.Delete(sourcePath);
            Require(!File.Exists(sourcePath), "The raw CSV still exists before retained-package replay.");

            var results = new List<BenchmarkResult>(6)
            {
                Result(
                    scenario,
                    "InspectPackage",
                    inspectPackage,
                    $"sourceBytes={sourceBytes}, packageBytes={packageBytes}, " +
                    $"packageOverheadBytes={packageBytes - sourceBytes}, " +
                    $"sampleRows={InspectionRows}"),
            };

            Timed<CsvSnapshotPackageSession> packageOpen = await MeasureAsync(
                    () => OpenPackageAsync(
                        packagePath,
                        workspacePath,
                        sourceBytes,
                        artifacts.Manifest.ManifestDigest,
                        cancellationToken))
                .ConfigureAwait(false);
            await using (CsvSnapshotPackageSession session = packageOpen.Value)
            {
                ValidateOpenedSession(session, artifacts);
                results.Add(Result(
                    scenario,
                    "PackageOpen",
                    packageOpen,
                    $"sourceBytes={sourceBytes}, packageBytes={packageBytes}"));

                Timed<ReplaySummary> replay = await MeasureAsync(
                        () => ReplayAsync(
                            session.DataSource,
                            scenario,
                            cancellationToken))
                    .ConfigureAwait(false);
                ValidateReplay(replay.Value, scenario);
                results.Add(Result(
                    scenario,
                    "Replay",
                    replay,
                    $"batches={replay.Value.Batches}, rows={replay.Value.Rows}, " +
                    $"peakSourceBatchRows={replay.Value.PeakBatchRows}"));

                Timed<ApplySummary> apply = await MeasureAsync(
                        () => ApplyNewTargetAsync(
                            targetPath,
                            artifacts.Plan,
                            artifacts.Catalog,
                            session.DataSource,
                            cancellationToken))
                    .ConfigureAwait(false);
                ValidateFirstApply(apply.Value, scenario);
                results.Add(Result(
                    scenario,
                    "Apply",
                    apply,
                    ApplyExtraInfo(apply.Value)));
            }

            RequireWorkspaceClean(workspacePath);
            Require(
                !File.Exists(targetPath + ".migration.lock"),
                "The staged target lease remains after the first apply session.");

            Timed<ApplySummary> resume = await MeasureAsync(
                    () => ResumeFreshSessionAsync(
                        packagePath,
                        workspacePath,
                        sourceBytes,
                        artifacts,
                        targetPath,
                        cancellationToken))
                .ConfigureAwait(false);
            ValidateResume(resume.Value, scenario);
            results.Add(Result(
                scenario,
                "ResumeFreshSession",
                resume,
                ApplyExtraInfo(resume.Value)));

            RequireWorkspaceClean(workspacePath);
            Require(
                !File.Exists(targetPath + ".migration.lock"),
                "The staged target lease remains after the resume session.");

            Timed<ValidationSummary> validation = await MeasureAsync(
                    () => ValidateFreshSessionAsync(
                        packagePath,
                        workspacePath,
                        sourceBytes,
                        artifacts,
                        targetPath,
                        rootPath,
                        cancellationToken))
                .ConfigureAwait(false);
            ValidateChecksum(validation.Value, scenario);
            results.Add(Result(
                scenario,
                "ChecksumValidate",
                validation,
                $"sourceRows={validation.Value.Evidence.SourceRowCount}, " +
                $"targetRows={validation.Value.Evidence.TargetRowCount}, " +
                $"checksum={validation.Value.Evidence.SourceChecksum}, " +
                $"peakSpillBytes={validation.Value.Result.PeakSpillBytes}"));

            RequireWorkspaceClean(workspacePath);
            Require(
                !File.Exists(targetPath + ".migration.lock"),
                "The staged target lease remains after checksum validation.");
            Require(
                !Directory.EnumerateFiles(rootPath, ".csdbcsv-*.tmp").Any(),
                "A retained-package temporary file remains after publication.");

            string packageDigestAfter = await ComputeFileDigestAsync(packagePath, cancellationToken)
                .ConfigureAwait(false);
            Require(
                string.Equals(packageDigestBefore, packageDigestAfter, StringComparison.Ordinal),
                "The retained package changed during replay, apply, or resume.");

            foreach (BenchmarkResult result in results)
            {
                Console.WriteLine(
                    $"  {result.Name}: {result.OpsPerSecond:N0} rows/sec, " +
                    $"elapsed={result.ElapsedMs:N1}ms");
                Console.WriteLine($"    {result.ExtraInfo}");
            }

            return results;
        }
        finally
        {
            if (Directory.Exists(rootPath))
                Directory.Delete(rootPath, recursive: true);
        }
    }

    private static async Task<InspectPackageArtifacts> InspectAndPackageAsync(
        string sourcePath,
        string packagePath,
        string workspacePath,
        long sourceBytes,
        CancellationToken cancellationToken)
    {
        await using CsvSourceSnapshot snapshot = await CsvSourceSnapshot.CreateFromFileAsync(
                sourcePath,
                new CsvSourceSnapshotOptions
                {
                    WorkspacePath = workspacePath,
                    MaxSourceBytes = sourceBytes,
                    CopyBufferBytes = CopyBufferBytes,
                },
                cancellationToken)
            .ConfigureAwait(false);
        var readerOptions = new CsvReaderOptions();
        CsvFormatInspection inspection = await CsvFormatInspector.InspectAsync(
                snapshot,
                readerOptions,
                new CsvInspectionOptions { DelimiterCandidates = [","] },
                cancellationToken)
            .ConfigureAwait(false);
        CsvSourceBinding binding = await CsvSourceBinding.CreateAsync(
                snapshot,
                inspection,
                SourceIdentity,
                cancellationToken)
            .ConfigureAwait(false);
        CsvSchemaInferenceResult schema = await CsvSchemaInferer.InferAsync(
                binding,
                snapshot,
                InspectionRows,
                cancellationToken: cancellationToken)
            .ConfigureAwait(false);
        Require(schema.Columns.Count == 1, "CSV inspection did not produce exactly one column.");
        Require(
            schema.Columns[0].LogicalType == CsvColumnLogicalType.Text,
            "The fixed CSV payload was not inferred as TEXT.");

        MigrationCatalog catalog = schema.CreateCatalog(
            CSharpDbCapabilityCatalogLoader.CurrentTargetVersion);
        MigrationPlan plan = ReadyPlan(catalog);
        MigrationPlanReadinessValidator.ValidateForApply(plan, catalog);
        CsvSnapshotPackageManifest manifest = await CsvSnapshotPackage.WriteAsync(
                packagePath,
                snapshot,
                schema,
                CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
                cancellationToken)
            .ConfigureAwait(false);
        return new InspectPackageArtifacts(manifest, catalog, plan);
    }

    private static MigrationPlan ReadyPlan(MigrationCatalog catalog)
    {
        MigrationPlan draft = new MigrationPlanner().CreatePlan(catalog);
        return draft with
        {
            AcceptedExclusionObjectIds = draft.Objects
                .Where(item => !item.Included)
                .Select(item => item.SourceObjectId)
                .OrderBy(item => item, StringComparer.Ordinal)
                .ToArray(),
            Load = draft.Load with
            {
                BatchSize = BatchSize,
                MaxBatchBytes = MaxBatchBytes,
                MaxValueBytes = MaxValueBytes,
            },
        };
    }

    private static async Task<CsvSnapshotPackageSession> OpenPackageAsync(
        string packagePath,
        string workspacePath,
        long sourceBytes,
        string expectedManifestDigest,
        CancellationToken cancellationToken) =>
        await CsvSnapshotPackage.OpenAsync(
                packagePath,
                new CsvSnapshotPackageOpenOptions
                {
                    WorkspacePath = workspacePath,
                    MaxSourceBytes = sourceBytes,
                    CopyBufferBytes = CopyBufferBytes,
                    ExpectedManifestDigest = expectedManifestDigest,
                },
                cancellationToken)
            .ConfigureAwait(false);

    private static void ValidateOpenedSession(
        CsvSnapshotPackageSession session,
        InspectPackageArtifacts artifacts)
    {
        Require(
            string.Equals(
                session.Manifest.ManifestDigest,
                artifacts.Manifest.ManifestDigest,
                StringComparison.Ordinal),
            "The reopened package manifest digest changed.");
        Require(
            string.Equals(
                session.Manifest.SnapshotIdentity,
                artifacts.Manifest.SnapshotIdentity,
                StringComparison.Ordinal),
            "The reopened package snapshot identity changed.");
        Require(
            string.Equals(
                MigrationArtifactSerializer.ComputeCatalogDigest(session.Catalog),
                artifacts.Plan.CatalogDigest,
                StringComparison.Ordinal),
            "The reopened package catalog does not match the retained plan.");
        Require(
            session.DataSource.Source == artifacts.Plan.Source,
            "The reopened package source identity does not match the retained plan.");
    }

    private static async Task<ReplaySummary> ReplayAsync(
        CsvMigrationDataSource source,
        Scenario scenario,
        CancellationToken cancellationToken)
    {
        var request = new MigrationReadRequest
        {
            SourceObjectId = CsvMigrationObjectIds.Table,
            ColumnObjectIds = [CsvMigrationObjectIds.Column(0)],
            BatchSize = BatchSize,
            MaxBatchBytes = MaxBatchBytes,
            MaxValueBytes = MaxValueBytes,
            SnapshotToken = source.SnapshotIdentity,
        };
        long batches = 0;
        long rows = 0;
        int peakBatchRows = 0;
        string? expectedStartCursor = null;
        string? terminalCursor = null;
        await foreach (MigrationDataBatch batch in source
                           .ReadAsync(request, cancellationToken)
                           .WithCancellation(cancellationToken)
                           .ConfigureAwait(false))
        {
            Require(batch.BatchOrdinal == batches, "The CSV replay batch ordinal is not contiguous.");
            Require(
                string.Equals(batch.StartCursor, expectedStartCursor, StringComparison.Ordinal),
                "The CSV replay cursor chain is not contiguous.");
            Require(batch.Rows.Count is > 0 and <= BatchSize, "The CSV replay batch row bound changed.");
            if (batches > 0)
                Require(batch.StartCursor is not null, "A noninitial CSV replay batch has no start cursor.");

            foreach (MigrationDataRow row in batch.Rows)
            {
                MigrationSourceValue value = AssertSingle(row.Values);
                Require(
                    value.Kind == MigrationSourceValueKind.Text &&
                    string.Equals(value.CanonicalText, s_payload, StringComparison.Ordinal),
                    "CSV replay changed the fixed payload.");
            }

            batches++;
            rows += batch.Rows.Count;
            peakBatchRows = Math.Max(peakBatchRows, batch.Rows.Count);
            expectedStartCursor = batch.NextCursor;
            terminalCursor = batch.NextCursor;
        }

        return new ReplaySummary(batches, rows, peakBatchRows, terminalCursor);
    }

    private static async Task<ApplySummary> ApplyNewTargetAsync(
        string targetPath,
        MigrationPlan plan,
        MigrationCatalog catalog,
        CsvMigrationDataSource source,
        CancellationToken cancellationToken)
    {
        await using CSharpDbStagedMigrationTarget target =
            await CSharpDbStagedMigrationTarget.CreateNewAsync(
                    targetPath,
                    plan,
                    catalog,
                    source.SnapshotIdentity,
                    cancellationToken: cancellationToken)
                .ConfigureAwait(false);
        return await ApplyAndInspectTargetAsync(
                target,
                plan,
                catalog,
                source,
                cancellationToken)
            .ConfigureAwait(false);
    }

    private static async Task<ApplySummary> ResumeFreshSessionAsync(
        string packagePath,
        string workspacePath,
        long sourceBytes,
        InspectPackageArtifacts artifacts,
        string targetPath,
        CancellationToken cancellationToken)
    {
        await using CsvSnapshotPackageSession session = await OpenPackageAsync(
                packagePath,
                workspacePath,
                sourceBytes,
                artifacts.Manifest.ManifestDigest,
                cancellationToken)
            .ConfigureAwait(false);
        ValidateOpenedSession(session, artifacts);
        await using CSharpDbStagedMigrationTarget target =
            await CSharpDbStagedMigrationTarget.OpenResumeAsync(
                    targetPath,
                    artifacts.Plan,
                    artifacts.Catalog,
                    session.DataSource.SnapshotIdentity,
                    cancellationToken: cancellationToken)
                .ConfigureAwait(false);
        return await ApplyAndInspectTargetAsync(
                target,
                artifacts.Plan,
                artifacts.Catalog,
                session.DataSource,
                cancellationToken)
            .ConfigureAwait(false);
    }

    private static async Task<ApplySummary> ApplyAndInspectTargetAsync(
        CSharpDbStagedMigrationTarget target,
        MigrationPlan plan,
        MigrationCatalog catalog,
        CsvMigrationDataSource source,
        CancellationToken cancellationToken)
    {
        MigrationApplyResult result = await new MigrationApplyRunner().ApplyAsync(
                new MigrationApplyRequest
                {
                    Plan = plan,
                    Catalog = catalog,
                    Source = source,
                    Target = target,
                },
                cancellationToken)
            .ConfigureAwait(false);
        long receiptCount = 0;
        long receiptRows = 0;
        await foreach (MigrationBatchReceipt receipt in target
                           .ReadReceiptsAsync(
                               result.PlanDigest,
                               CsvMigrationObjectIds.Table,
                               cancellationToken)
                           .WithCancellation(cancellationToken)
                           .ConfigureAwait(false))
        {
            receiptCount++;
            receiptRows += receipt.RowCount;
        }

        await using IValidationSnapshot validation = await target
            .OpenValidationSnapshotAsync(cancellationToken)
            .ConfigureAwait(false);
        long targetRows = await validation.CountAsync(
                CsvMigrationObjectIds.Table,
                cancellationToken)
            .ConfigureAwait(false);
        return new ApplySummary(result, receiptCount, receiptRows, targetRows);
    }

    private static async Task<ValidationSummary> ValidateFreshSessionAsync(
        string packagePath,
        string workspacePath,
        long sourceBytes,
        InspectPackageArtifacts artifacts,
        string targetPath,
        string rootPath,
        CancellationToken cancellationToken)
    {
        await using CsvSnapshotPackageSession session = await OpenPackageAsync(
                packagePath,
                workspacePath,
                sourceBytes,
                artifacts.Manifest.ManifestDigest,
                cancellationToken)
            .ConfigureAwait(false);
        ValidateOpenedSession(session, artifacts);
        await using var sourceSnapshot = new MigrationDataSourceValidationSnapshot(
            artifacts.Plan,
            artifacts.Catalog,
            session.DataSource);
        await using CSharpDbStagedMigrationTarget target =
            await CSharpDbStagedMigrationTarget.OpenResumeAsync(
                    targetPath,
                    artifacts.Plan,
                    artifacts.Catalog,
                    session.DataSource.SnapshotIdentity,
                    cancellationToken: cancellationToken)
                .ConfigureAwait(false);
        string reportPath = Path.Combine(rootPath, "validation-report.json");
        MigrationValidationRunResult result = await new MigrationValidationRunner()
            .ValidateAsync(
                new MigrationValidationRunRequest
                {
                    Plan = artifacts.Plan,
                    Catalog = artifacts.Catalog,
                    SourceSnapshot = sourceSnapshot,
                    Target = target,
                    Level = MigrationValidationLevel.Checksum,
                    ReportOutputPath = reportPath,
                    ChecksumOptions = new PartitionedChecksumValidatorOptions
                    {
                        SpillRootDirectory = workspacePath,
                    },
                    ActivateOnSuccess = false,
                },
                cancellationToken)
            .ConfigureAwait(false);
        Require(File.Exists(reportPath), "Checksum validation did not publish its report.");
        Require(!result.Activated, "The diagnostic checksum run unexpectedly activated the target.");
        Require(result.Report.Objects.Count == 1, "Checksum validation returned an unexpected object count.");
        return new ValidationSummary(result, result.Report.Objects[0]);
    }

    private static void ValidateReplay(ReplaySummary replay, Scenario scenario)
    {
        long expectedBatches = ExpectedBatchCount(scenario);
        Require(replay.Rows == scenario.RowCount, "CSV replay row count is incomplete.");
        Require(replay.Batches == expectedBatches, "CSV replay batch count is incomplete.");
        Require(replay.PeakBatchRows == BatchSize, "CSV replay did not retain the fixed row bound.");
        Require(replay.TerminalCursor is null, "The terminal CSV replay batch has a next cursor.");
    }

    private static void ValidateFirstApply(ApplySummary apply, Scenario scenario)
    {
        long expectedBatches = ExpectedBatchCount(scenario);
        MigrationApplyResult result = apply.Result;
        Require(result.RowsWritten == scenario.RowCount, "The first CSV apply row count is incomplete.");
        Require(result.RowsSkipped == 0, "The first CSV apply unexpectedly skipped rows.");
        Require(result.BatchesWritten == expectedBatches, "The first CSV apply batch count is incomplete.");
        Require(result.BatchesSkipped == 0, "The first CSV apply unexpectedly skipped batches.");
        ValidateApplyBoundsAndTarget(apply, scenario, expectedBatches);
    }

    private static void ValidateResume(ApplySummary resume, Scenario scenario)
    {
        long expectedBatches = ExpectedBatchCount(scenario);
        MigrationApplyResult result = resume.Result;
        Require(result.RowsWritten == 0, "Fresh-session CSV resume rewrote rows.");
        Require(result.RowsSkipped == scenario.RowCount, "Fresh-session CSV resume skipped-row count is incomplete.");
        Require(result.BatchesWritten == 0, "Fresh-session CSV resume rewrote batches.");
        Require(result.BatchesSkipped == expectedBatches, "Fresh-session CSV resume skipped-batch count is incomplete.");
        ValidateApplyBoundsAndTarget(resume, scenario, expectedBatches);
    }

    private static void ValidateChecksum(ValidationSummary validation, Scenario scenario)
    {
        MigrationObjectValidationEvidence evidence = validation.Evidence;
        Require(
            validation.Result.Report.Outcome == MigrationValidationStatus.Passed,
            "The retained CSV checksum validation did not pass.");
        Require(
            validation.Result.Report.Level == MigrationValidationLevel.Checksum,
            "The retained CSV validation did not run at checksum level.");
        Require(
            evidence.Status == MigrationValidationStatus.Passed,
            "The retained CSV object checksum did not pass.");
        Require(
            evidence.SourceRowCount == scenario.RowCount &&
            evidence.TargetRowCount == scenario.RowCount,
            "The retained CSV checksum row counts are incomplete.");
        Require(
            evidence.SourceChecksum is not null &&
            string.Equals(evidence.SourceChecksum, evidence.TargetChecksum, StringComparison.Ordinal),
            "The retained CSV source and target checksums differ.");
    }

    private static void ValidateApplyBoundsAndTarget(
        ApplySummary apply,
        Scenario scenario,
        long expectedBatches)
    {
        int expectedPeakBytes = checked(BatchSize * s_canonicalValueBytes);
        Require(apply.Result.PeakBufferedRows == BatchSize, "The apply live-row bound changed.");
        Require(apply.Result.PeakBufferedBytes == expectedPeakBytes, "The apply live-byte bound changed.");
        Require(apply.ReceiptCount == expectedBatches, "The staged target receipt count is incomplete.");
        Require(apply.ReceiptRows == scenario.RowCount, "The staged target receipt row total is incomplete.");
        Require(apply.TargetRows == scenario.RowCount, "The staged target row count is incomplete.");
    }

    private static long ExpectedBatchCount(Scenario scenario) =>
        checked((scenario.RowCount + BatchSize - 1L) / BatchSize);

    private static string ApplyExtraInfo(ApplySummary apply) =>
        $"batchesWritten={apply.Result.BatchesWritten}, " +
        $"batchesSkipped={apply.Result.BatchesSkipped}, " +
        $"rowsWritten={apply.Result.RowsWritten}, rowsSkipped={apply.Result.RowsSkipped}, " +
        $"peakBufferedRows={apply.Result.PeakBufferedRows}, " +
        $"peakBufferedBytes={apply.Result.PeakBufferedBytes}, " +
        $"receipts={apply.ReceiptCount}, receiptRows={apply.ReceiptRows}, " +
        $"targetRows={apply.TargetRows}";

    private static BenchmarkResult Result<T>(
        Scenario scenario,
        string phase,
        Timed<T> timed,
        string phaseInfo) => new()
        {
            Name = $"CsvRetained_{phase}_{scenario.Name}",
            TotalOps = scenario.RowCount,
            ElapsedMs = timed.ElapsedMs,
            ExtraInfo =
                $"{phaseInfo}, allocatedBytes={timed.AllocatedBytes}, " +
                $"heapSizeAfter={timed.HeapSizeAfter}, workingSetBytes={timed.WorkingSetBytes}, " +
                $"peakWorkingSetBytes={timed.PeakWorkingSetBytes}, " +
                $"peakWorkingSetDeltaBytes={timed.PeakWorkingSetDeltaBytes}",
        };

    private static async Task<Timed<T>> MeasureAsync<T>(Func<Task<T>> operation)
    {
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        using Process process = Process.GetCurrentProcess();
        process.Refresh();
        long peakWorkingSetBefore = process.PeakWorkingSet64;
        long allocatedBefore = GC.GetTotalAllocatedBytes(precise: true);
        var elapsed = Stopwatch.StartNew();
        T value = await operation().ConfigureAwait(false);
        elapsed.Stop();
        long allocatedBytes = GC.GetTotalAllocatedBytes(precise: true) - allocatedBefore;
        process.Refresh();
        long peakWorkingSetAfter = process.PeakWorkingSet64;
        return new Timed<T>(
            value,
            elapsed.Elapsed.TotalMilliseconds,
            allocatedBytes,
            GC.GetGCMemoryInfo().HeapSizeBytes,
            process.WorkingSet64,
            peakWorkingSetAfter,
            Math.Max(0, peakWorkingSetAfter - peakWorkingSetBefore));
    }

    private static async Task WriteSourceAsync(
        string path,
        int rowCount,
        CancellationToken cancellationToken)
    {
        const int rowsPerChunk = 1_024;
        string chunk = string.Concat(Enumerable.Repeat(s_encodedRow, rowsPerChunk));
        var options = new FileStreamOptions
        {
            Mode = FileMode.CreateNew,
            Access = FileAccess.Write,
            Share = FileShare.None,
            BufferSize = CopyBufferBytes,
            Options = FileOptions.Asynchronous | FileOptions.SequentialScan,
        };
        await using var stream = new FileStream(path, options);
        await using var writer = new StreamWriter(
            stream,
            new UTF8Encoding(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true),
            CopyBufferBytes,
            leaveOpen: false);
        await writer.WriteAsync("value\r\n".AsMemory(), cancellationToken).ConfigureAwait(false);
        int remaining = rowCount;
        while (remaining > 0)
        {
            int rows = Math.Min(rowsPerChunk, remaining);
            await writer.WriteAsync(
                    chunk.AsMemory(0, checked(rows * s_encodedRow.Length)),
                    cancellationToken)
                .ConfigureAwait(false);
            remaining -= rows;
        }

        await writer.FlushAsync(cancellationToken).ConfigureAwait(false);
    }

    private static async Task<string> ComputeFileDigestAsync(
        string path,
        CancellationToken cancellationToken)
    {
        await using var stream = new FileStream(
            path,
            FileMode.Open,
            FileAccess.Read,
            FileShare.Read,
            CopyBufferBytes,
            FileOptions.Asynchronous | FileOptions.SequentialScan);
        byte[] digest = await SHA256.HashDataAsync(stream, cancellationToken).ConfigureAwait(false);
        return Convert.ToHexString(digest).ToLowerInvariant();
    }

    private static MigrationSourceValue AssertSingle(IReadOnlyList<MigrationSourceValue> values)
    {
        Require(values.Count == 1, "CSV replay row width changed.");
        return values[0];
    }

    private static void RequireWorkspaceClean(string workspacePath) =>
        Require(
            !Directory.EnumerateFileSystemEntries(workspacePath).Any(),
            "A retained CSV snapshot workspace remains after disposal.");

    private static void Require(bool condition, string message)
    {
        if (!condition)
            throw new InvalidDataException(message);
    }

    private sealed record Scenario(string Name, int RowCount);

    private sealed record InspectPackageArtifacts(
        CsvSnapshotPackageManifest Manifest,
        MigrationCatalog Catalog,
        MigrationPlan Plan);

    private sealed record ReplaySummary(
        long Batches,
        long Rows,
        int PeakBatchRows,
        string? TerminalCursor);

    private sealed record ApplySummary(
        MigrationApplyResult Result,
        long ReceiptCount,
        long ReceiptRows,
        long TargetRows);

    private sealed record ValidationSummary(
        MigrationValidationRunResult Result,
        MigrationObjectValidationEvidence Evidence);

    private sealed record Timed<T>(
        T Value,
        double ElapsedMs,
        long AllocatedBytes,
        long HeapSizeAfter,
        long WorkingSetBytes,
        long PeakWorkingSetBytes,
        long PeakWorkingSetDeltaBytes);
}
