using System.Diagnostics;
using CSharpDB.Benchmarks.Infrastructure;
using CSharpDB.Migration;
using CSharpDB.Migration.CSharpDb;
using CSharpDB.Primitives;

namespace CSharpDB.Benchmarks.Macro;

/// <summary>
/// Measures the actual Phase 2 staged-target path: converted target batches,
/// canonical batch digests, prepared inserts, and an atomic receipt per batch.
/// The two fixed scenarios differ by 10x in total rows while retaining the
/// same bounded batch shape so memory behavior can be compared directly.
/// </summary>
public static class MigrationTargetThroughputBenchmark
{
    private const string SourceTableId = "syn:table:reserved";
    private const string SourceColumnId = "syn:column:reserved:value";
    private const string SnapshotIdentity = "benchmark:synthetic-snapshot/v1";

    private static readonly Scenario[] s_scenarios =
    [
        new("Rows100K_Batch1000_Text64", RowCount: 100_000, BatchSize: 1_000, TextCharacters: 64),
        new("Rows1M_Batch1000_Text64", RowCount: 1_000_000, BatchSize: 1_000, TextCharacters: 64),
    ];

    public static async Task<List<BenchmarkResult>> RunAsync()
    {
        var results = new List<BenchmarkResult>(s_scenarios.Length);
        foreach (Scenario scenario in s_scenarios)
            results.Add(await RunScenarioAsync(scenario));
        return results;
    }

    public static Task<BenchmarkResult> RunNamedScenarioAsync(string scenarioName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(scenarioName);
        Scenario? scenario = s_scenarios.FirstOrDefault(
            item => item.Name.Equals(scenarioName, StringComparison.OrdinalIgnoreCase));
        if (scenario is null)
        {
            throw new ArgumentException(
                $"Unknown migration target scenario '{scenarioName}'.",
                nameof(scenarioName));
        }

        return RunScenarioAsync(scenario);
    }

    private static async Task<BenchmarkResult> RunScenarioAsync(Scenario scenario)
    {
        MigrationCatalog catalog = await new SyntheticMigrationSourceInspector().InspectAsync(
            new MigrationInspectionRequest
            {
                TargetCSharpDbVersion = CSharpDbCapabilityCatalogLoader.CurrentTargetVersion,
                IncludeProfile = true,
                ProfileSampleSize = 5,
            });
        MigrationPlan planned = new MigrationPlanner().CreatePlan(catalog);
        MigrationPlan plan = planned with
        {
            AcceptedExclusionObjectIds = planned.Objects
                .Where(item => !item.Included)
                .Select(item => item.SourceObjectId)
                .OrderBy(item => item, StringComparer.Ordinal)
                .ToArray(),
            Load = planned.Load with
            {
                BatchSize = scenario.BatchSize,
                MaxBatchBytes = 4L * 1024 * 1024,
                MaxValueBytes = 1024,
            },
        };
        MigrationPlanReadinessValidator.ValidateForApply(plan, catalog);

        string targetPath = Path.Combine(
            Path.GetTempPath(),
            $"csharpdb_migration_target_benchmark_{Guid.NewGuid():N}.csdb");
        string payload = new('m', scenario.TextCharacters);
        DbValue value = DbValue.FromText(payload);
        int valueBytes = MigrationValueConverter.GetCanonicalByteCount(value);
        int batchCount = checked((scenario.RowCount + scenario.BatchSize - 1) / scenario.BatchSize);
        int peakBatchRows = 0;
        long peakBatchBytes = 0;
        long receiptRows = 0;
        int receiptCount = 0;

        try
        {
            await using (CSharpDbStagedMigrationTarget target =
                await CSharpDbStagedMigrationTarget.CreateNewAsync(
                    targetPath,
                    plan,
                    catalog,
                    SnapshotIdentity))
            {
                await target.ApplySchemaAsync(
                    plan,
                    catalog,
                    MigrationSchemaStage.LoadEssential);

                GC.Collect();
                GC.WaitForPendingFinalizers();
                GC.Collect();
                long allocatedBefore = GC.GetTotalAllocatedBytes(precise: true);
                using Process process = Process.GetCurrentProcess();
                process.Refresh();
                long peakWorkingSetBefore = process.PeakWorkingSet64;
                var latency = new LatencyHistogram();
                var elapsed = Stopwatch.StartNew();

                long sourceRowOrdinal = 0;
                string? startCursor = null;
                string planDigest = MigrationArtifactSerializer.ComputePlanDigest(plan);
                for (int batchOrdinal = 0; batchOrdinal < batchCount; batchOrdinal++)
                {
                    int rowsInBatch = Math.Min(
                        scenario.BatchSize,
                        scenario.RowCount - checked(batchOrdinal * scenario.BatchSize));
                    var rows = new MigrationTargetRow[rowsInBatch];
                    for (int rowIndex = 0; rowIndex < rowsInBatch; rowIndex++)
                    {
                        rows[rowIndex] = new MigrationTargetRow
                        {
                            SourceRowOrdinal = sourceRowOrdinal++,
                            Values = [value],
                        };
                    }

                    string nextCursor = $"row:{sourceRowOrdinal}";
                    var batch = new MigrationTargetBatch
                    {
                        PlanDigest = planDigest,
                        CatalogDigest = plan.CatalogDigest,
                        SourceFingerprint = plan.Source.Fingerprint,
                        SourceSnapshotIdentity = SnapshotIdentity,
                        SourceObjectId = SourceTableId,
                        ColumnObjectIds = [SourceColumnId],
                        BatchOrdinal = batchOrdinal,
                        StartCursor = startCursor,
                        NextCursor = nextCursor,
                        BatchDigest = string.Empty,
                        Rows = rows,
                    };
                    batch = batch with
                    {
                        RejectDigest = MigrationRejectDigest.Compute(batch),
                    };
                    batch = batch with { BatchDigest = MigrationBatchDigest.Compute(batch) };

                    var batchLatency = Stopwatch.StartNew();
                    MigrationBatchReceipt receipt = await target.WriteBatchAsync(batch);
                    batchLatency.Stop();
                    latency.Record(batchLatency.Elapsed.TotalMilliseconds);
                    if (receipt.RowCount != rowsInBatch || receipt.RejectedRowCount != 0)
                        throw new InvalidDataException("Migration benchmark receipt does not match the written batch.");

                    peakBatchRows = Math.Max(peakBatchRows, rowsInBatch);
                    peakBatchBytes = Math.Max(peakBatchBytes, checked((long)rowsInBatch * valueBytes));
                    startCursor = nextCursor;
                }

                elapsed.Stop();
                long allocatedBytes = GC.GetTotalAllocatedBytes(precise: true) - allocatedBefore;
                process.Refresh();
                long peakWorkingSetAfter = process.PeakWorkingSet64;
                long heapSizeAfter = GC.GetGCMemoryInfo().HeapSizeBytes;

                await foreach (MigrationBatchReceipt receipt in target.ReadReceiptsAsync(
                                   planDigest,
                                   SourceTableId))
                {
                    receiptCount++;
                    receiptRows += receipt.RowCount;
                }
                if (receiptCount != batchCount || receiptRows != scenario.RowCount)
                    throw new InvalidDataException("Migration benchmark receipt set is incomplete.");

                foreach (MigrationSchemaStage stage in new[]
                         {
                             MigrationSchemaStage.SecondaryIndexes,
                             MigrationSchemaStage.Constraints,
                             MigrationSchemaStage.Views,
                             MigrationSchemaStage.Triggers,
                         })
                {
                    await target.ApplySchemaAsync(plan, catalog, stage);
                }

                await using IValidationSnapshot snapshot = await target.OpenValidationSnapshotAsync();
                long targetRows = await snapshot.CountAsync(SourceTableId);
                if (targetRows != scenario.RowCount)
                    throw new InvalidDataException(
                        $"Migration benchmark expected {scenario.RowCount} rows, observed {targetRows}.");

                double rowsPerSecond = scenario.RowCount / elapsed.Elapsed.TotalSeconds;
                var result = new BenchmarkResult
                {
                    Name = $"MigrationTarget_{scenario.Name}",
                    TotalOps = scenario.RowCount,
                    ElapsedMs = elapsed.Elapsed.TotalMilliseconds,
                    P50Ms = latency.Percentile(0.50),
                    P90Ms = latency.Percentile(0.90),
                    P95Ms = latency.Percentile(0.95),
                    P99Ms = latency.Percentile(0.99),
                    P999Ms = latency.Percentile(0.999),
                    MinMs = latency.Min,
                    MaxMs = latency.Max,
                    MeanMs = latency.Mean,
                    StdDevMs = latency.StdDev,
                    ExtraInfo =
                        $"rowsPerSec={rowsPerSecond:F1}, batches={batchCount}, batchSize={scenario.BatchSize}, " +
                        $"peakBatchRows={peakBatchRows}, peakBatchBytes={peakBatchBytes}, " +
                        $"allocatedBytes={allocatedBytes}, heapSizeAfter={heapSizeAfter}, " +
                        $"peakWorkingSetBytes={peakWorkingSetAfter}, " +
                        $"peakWorkingSetDeltaBytes={Math.Max(0, peakWorkingSetAfter - peakWorkingSetBefore)}, " +
                        $"receipts={receiptCount}, receiptRows={receiptRows}",
                };

                Console.WriteLine(
                    $"  {result.Name}: {result.OpsPerSecond:N0} rows/sec, " +
                    $"P50={result.P50Ms:F3}ms/batch, P99={result.P99Ms:F3}ms/batch");
                Console.WriteLine($"    {result.ExtraInfo}");
                return result;
            }
        }
        finally
        {
            if (File.Exists(targetPath))
                File.Delete(targetPath);
            if (File.Exists(targetPath + ".wal"))
                File.Delete(targetPath + ".wal");
            if (File.Exists(targetPath + ".migration.lock"))
                File.Delete(targetPath + ".migration.lock");
        }
    }

    private sealed record Scenario(
        string Name,
        int RowCount,
        int BatchSize,
        int TextCharacters);
}
