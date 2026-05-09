using System.Diagnostics;
using CSharpDB.Benchmarks.Infrastructure;
using CSharpDB.Engine;
using CSharpDB.Primitives;
using CSharpDB.Storage.Checkpointing;
using CSharpDB.Storage.Diagnostics;
using CSharpDB.Storage.Paging;

namespace CSharpDB.Benchmarks.Macro;

/// <summary>
/// Measures the audited non-hot async I/O paths used to close the batching phase.
/// Rows are diagnostic: they validate coverage and track throughput, but do not
/// imply these paths should block releases without an explicit threshold.
/// </summary>
public static class AsyncIoCloseOutBenchmark
{
    private const int MaintenanceRows = 12_000;
    private const int ForeignKeyRows = 8_000;
    private const int WalRows = 1_200;

    public static async Task<List<BenchmarkResult>> RunAsync()
    {
        string maintenanceSeedPath = await CreateMaintenanceSeedDatabaseAsync();
        try
        {
            return
            [
                await RunSaveToFileAsync(maintenanceSeedPath),
                await RunBackupAsync(maintenanceSeedPath),
                await RunRestoreStagingAsync(maintenanceSeedPath),
                await RunVacuumAsync(maintenanceSeedPath),
                await RunForeignKeyMigrationAsync(),
                await RunDatabaseInspectorAsync(maintenanceSeedPath),
                await RunWalInspectorAsync(),
            ];
        }
        finally
        {
            DeleteDatabaseFiles(maintenanceSeedPath);
        }
    }

    private static async Task<BenchmarkResult> RunSaveToFileAsync(string seedPath)
    {
        string sourcePath = CloneDatabaseFiles(seedPath, "async-io-save-source");
        string destinationPath = Path.Combine(Path.GetTempPath(), $"async_io_save_{Guid.NewGuid():N}.db");
        try
        {
            await using var db = await Database.OpenAsync(sourcePath, BenchmarkDurability.Apply());
            var sw = Stopwatch.StartNew();
            await db.SaveToFileAsync(destinationPath);
            sw.Stop();

            long bytes = GetFileLength(destinationPath);
            return PrintFileResult(
                "AsyncIoCloseOut_SaveToFile",
                bytes,
                sw.Elapsed.TotalMilliseconds,
                "alreadyBatched=StorageDeviceCopyBatcher, path=Database.SaveToFileAsync");
        }
        finally
        {
            DeleteDatabaseFiles(sourcePath);
            DeleteDatabaseFiles(destinationPath);
        }
    }

    private static async Task<BenchmarkResult> RunBackupAsync(string seedPath)
    {
        string sourcePath = CloneDatabaseFiles(seedPath, "async-io-backup-source");
        string destinationPath = Path.Combine(Path.GetTempPath(), $"async_io_backup_{Guid.NewGuid():N}.db");
        try
        {
            await using var db = await Database.OpenAsync(sourcePath, BenchmarkDurability.Apply());
            var sw = Stopwatch.StartNew();
            DatabaseBackupResult backup = await DatabaseBackupCoordinator.BackupAsync(
                db,
                sourcePath,
                destinationPath,
                withManifest: false);
            sw.Stop();

            return PrintFileResult(
                "AsyncIoCloseOut_Backup",
                backup.DatabaseFileBytes,
                sw.Elapsed.TotalMilliseconds,
                $"alreadyBatched=StorageDeviceCopyBatcher, path=DatabaseBackupCoordinator.BackupAsync, pages={backup.PhysicalPageCount}");
        }
        finally
        {
            DeleteDatabaseFiles(sourcePath);
            DeleteDatabaseFiles(destinationPath);
        }
    }

    private static async Task<BenchmarkResult> RunRestoreStagingAsync(string seedPath)
    {
        string sourcePath = CloneDatabaseFiles(seedPath, "async-io-restore-source");
        string destinationPath = Path.Combine(Path.GetTempPath(), $"async_io_restore_{Guid.NewGuid():N}.db");
        try
        {
            var sw = Stopwatch.StartNew();
            DatabaseRestoreResult restore = await DatabaseBackupCoordinator.RestoreAsync(
                sourcePath,
                destinationPath,
                static _ => ValueTask.CompletedTask);
            sw.Stop();

            long bytes = GetFileLength(destinationPath);
            return PrintFileResult(
                "AsyncIoCloseOut_RestoreStaging",
                bytes,
                sw.Elapsed.TotalMilliseconds,
                $"alreadyBatched=LoadIntoMemory+SaveToFile staging, path=DatabaseBackupCoordinator.RestoreAsync, pages={restore.PhysicalPageCount}");
        }
        finally
        {
            DeleteDatabaseFiles(sourcePath);
            DeleteDatabaseFiles(destinationPath);
        }
    }

    private static async Task<BenchmarkResult> RunVacuumAsync(string seedPath)
    {
        string sourcePath = CloneDatabaseFiles(seedPath, "async-io-vacuum-source");
        try
        {
            var sw = Stopwatch.StartNew();
            DatabaseVacuumResult vacuum = await DatabaseMaintenanceCoordinator.VacuumAsync(sourcePath);
            sw.Stop();

            int totalOps = Math.Max(1, vacuum.PhysicalPageCountBefore);
            var result = BuildSingleOperationResult(
                "AsyncIoCloseOut_VacuumLogicalRewrite",
                totalOps,
                sw.Elapsed.TotalMilliseconds,
                $"intentionallyLogical=BTreeCopyUtility, path=DatabaseMaintenanceCoordinator.VacuumAsync, pagesBefore={vacuum.PhysicalPageCountBefore}, pagesAfter={vacuum.PhysicalPageCountAfter}, bytesBefore={vacuum.DatabaseFileBytesBefore}, bytesAfter={vacuum.DatabaseFileBytesAfter}");

            PrintResult(result, "pages/sec");
            return result;
        }
        finally
        {
            DeleteDatabaseFiles(sourcePath);
        }
    }

    private static async Task<BenchmarkResult> RunForeignKeyMigrationAsync()
    {
        string sourcePath = await CreateForeignKeyMigrationSeedDatabaseAsync();
        string backupPath = Path.Combine(Path.GetTempPath(), $"async_io_fk_backup_{Guid.NewGuid():N}.db");
        try
        {
            var request = new DatabaseForeignKeyMigrationRequest
            {
                BackupDestinationPath = backupPath,
                Constraints =
                [
                    new DatabaseForeignKeyMigrationConstraintSpec
                    {
                        TableName = "child_rows",
                        ColumnName = "parent_id",
                        ReferencedTableName = "parent_rows",
                        ReferencedColumnName = "id",
                        OnDelete = ForeignKeyOnDeleteAction.Restrict,
                    },
                ],
            };

            var sw = Stopwatch.StartNew();
            DatabaseForeignKeyMigrationResult migration =
                await DatabaseMaintenanceCoordinator.MigrateForeignKeysAsync(sourcePath, request);
            sw.Stop();

            int copiedRows = (int)Math.Clamp(migration.CopiedRows, 1, int.MaxValue);
            var result = BuildSingleOperationResult(
                "AsyncIoCloseOut_ForeignKeyMigrationRewrite",
                copiedRows,
                sw.Elapsed.TotalMilliseconds,
                $"intentionallyLogical=BTreeCopyUtility, path=DatabaseMaintenanceCoordinator.MigrateForeignKeysAsync, affectedTables={migration.AffectedTables}, appliedForeignKeys={migration.AppliedForeignKeys}, copiedRows={migration.CopiedRows}, backup=true");

            PrintResult(result, "rows/sec");
            return result;
        }
        finally
        {
            DeleteDatabaseFiles(sourcePath);
            DeleteDatabaseFiles(backupPath);
        }
    }

    private static async Task<BenchmarkResult> RunDatabaseInspectorAsync(string seedPath)
    {
        string sourcePath = CloneDatabaseFiles(seedPath, "async-io-inspect-source");
        try
        {
            var sw = Stopwatch.StartNew();
            DatabaseInspectReport report = await DatabaseInspector.InspectAsync(
                sourcePath,
                new DatabaseInspectOptions { IncludePages = true });
            sw.Stop();

            var result = BuildSingleOperationResult(
                "AsyncIoCloseOut_DatabaseInspectorScan",
                Math.Max(1, report.Header.PhysicalPageCount),
                sw.Elapsed.TotalMilliseconds,
                $"auditStatus=specializedDiagnostics, path=DatabaseInspector.InspectAsync, pages={report.Header.PhysicalPageCount}, issues={report.Issues.Count}");

            PrintResult(result, "pages/sec");
            return result;
        }
        finally
        {
            DeleteDatabaseFiles(sourcePath);
        }
    }

    private static async Task<BenchmarkResult> RunWalInspectorAsync()
    {
        string sourcePath = Path.Combine(Path.GetTempPath(), $"async_io_wal_seed_{Guid.NewGuid():N}.db");
        try
        {
            var options = BenchmarkDurability.Apply(new DatabaseOptions().ConfigureStorageEngine(builder =>
            {
                builder.UsePagerOptions(new PagerOptions
                {
                    CheckpointPolicy = new FrameCountCheckpointPolicy(int.MaxValue),
                });
            }));

            await using var db = await Database.OpenAsync(sourcePath, options);
            await db.ExecuteAsync("CREATE TABLE wal_rows (id INTEGER PRIMARY KEY, payload TEXT)");

            await db.BeginTransactionAsync();
            for (int id = 1; id <= WalRows; id++)
                await db.ExecuteAsync($"INSERT INTO wal_rows VALUES ({id}, 'wal_payload_{id}')");
            await db.CommitAsync();

            var sw = Stopwatch.StartNew();
            WalInspectReport report = await WalInspector.InspectAsync(sourcePath, options: null);
            sw.Stop();

            var result = BuildSingleOperationResult(
                "AsyncIoCloseOut_WalInspectorScan",
                Math.Max(1, report.FullFrameCount),
                sw.Elapsed.TotalMilliseconds,
                $"auditStatus=specializedDiagnostics, path=WalInspector.InspectAsync, frames={report.FullFrameCount}, commitFrames={report.CommitFrameCount}, walBytes={report.FileLengthBytes}, issues={report.Issues.Count}");

            PrintResult(result, "frames/sec");
            return result;
        }
        finally
        {
            DeleteDatabaseFiles(sourcePath);
        }
    }

    private static async Task<string> CreateMaintenanceSeedDatabaseAsync()
    {
        string filePath = Path.Combine(Path.GetTempPath(), $"async_io_closeout_seed_{Guid.NewGuid():N}.db");
        await using var db = await Database.OpenAsync(filePath, BenchmarkDurability.Apply());
        await db.ExecuteAsync("CREATE TABLE bench (id INTEGER PRIMARY KEY, value INTEGER, text_col TEXT, category TEXT)");
        await db.ExecuteAsync("CREATE INDEX idx_bench_category_value ON bench(category, value)");

        const int batchSize = 500;
        for (int batchStart = 1; batchStart <= MaintenanceRows; batchStart += batchSize)
        {
            int batchEnd = Math.Min(MaintenanceRows, batchStart + batchSize - 1);
            await db.BeginTransactionAsync();
            for (int id = batchStart; id <= batchEnd; id++)
            {
                string category = GetCategory(id);
                await db.ExecuteAsync(
                    $"INSERT INTO bench VALUES ({id}, {id * 10L}, 'maintenance_payload_{id}', '{category}')");
            }

            await db.CommitAsync();
        }

        await db.BeginTransactionAsync();
        for (int id = 4; id <= MaintenanceRows; id += 4)
            await db.ExecuteAsync($"DELETE FROM bench WHERE id = {id}");
        await db.CommitAsync();
        await db.CheckpointAsync();

        return filePath;
    }

    private static async Task<string> CreateForeignKeyMigrationSeedDatabaseAsync()
    {
        string filePath = Path.Combine(Path.GetTempPath(), $"async_io_fk_seed_{Guid.NewGuid():N}.db");
        await using var db = await Database.OpenAsync(filePath, BenchmarkDurability.Apply());
        await db.ExecuteAsync("CREATE TABLE parent_rows (id INTEGER PRIMARY KEY, payload TEXT)");
        await db.ExecuteAsync("CREATE TABLE child_rows (id INTEGER PRIMARY KEY, parent_id INTEGER NOT NULL, payload TEXT)");
        await db.ExecuteAsync("CREATE INDEX idx_child_rows_parent_id ON child_rows(parent_id)");

        const int batchSize = 500;
        for (int batchStart = 1; batchStart <= ForeignKeyRows; batchStart += batchSize)
        {
            int batchEnd = Math.Min(ForeignKeyRows, batchStart + batchSize - 1);
            await db.BeginTransactionAsync();
            for (int id = batchStart; id <= batchEnd; id++)
            {
                await db.ExecuteAsync($"INSERT INTO parent_rows VALUES ({id}, 'parent_{id}')");
                await db.ExecuteAsync($"INSERT INTO child_rows VALUES ({id}, {id}, 'child_{id}')");
            }

            await db.CommitAsync();
        }

        await db.CheckpointAsync();
        return filePath;
    }

    private static BenchmarkResult PrintFileResult(
        string name,
        long bytes,
        double elapsedMs,
        string extraInfo)
    {
        int pages = Math.Max(1, (int)Math.Ceiling(bytes / (double)PageConstants.PageSize));
        var result = BuildSingleOperationResult(
            name,
            pages,
            elapsedMs,
            $"{extraInfo}, bytes={bytes}, pages={pages}");

        PrintResult(result, "pages/sec");
        return result;
    }

    private static BenchmarkResult BuildSingleOperationResult(
        string name,
        int totalOps,
        double elapsedMs,
        string extraInfo)
    {
        return new BenchmarkResult
        {
            Name = name,
            TotalOps = totalOps,
            ElapsedMs = elapsedMs,
            P50Ms = elapsedMs,
            P90Ms = elapsedMs,
            P95Ms = elapsedMs,
            P99Ms = elapsedMs,
            P999Ms = elapsedMs,
            MinMs = elapsedMs,
            MaxMs = elapsedMs,
            MeanMs = elapsedMs,
            StdDevMs = 0,
            ExtraInfo = extraInfo,
        };
    }

    private static void PrintResult(BenchmarkResult result, string unit)
    {
        Console.WriteLine(
            $"  {result.Name}: {result.OpsPerSecond:N0} {unit}, elapsed={result.ElapsedMs:F3}ms");
        Console.WriteLine($"    {result.ExtraInfo}");
    }

    private static string CloneDatabaseFiles(string sourceFilePath, string prefix)
    {
        string destinationFilePath = Path.Combine(Path.GetTempPath(), $"{prefix}_{Guid.NewGuid():N}.db");
        File.Copy(sourceFilePath, destinationFilePath, overwrite: true);

        string sourceWalPath = sourceFilePath + ".wal";
        if (File.Exists(sourceWalPath))
            File.Copy(sourceWalPath, destinationFilePath + ".wal", overwrite: true);

        return destinationFilePath;
    }

    private static void DeleteDatabaseFiles(string? filePath)
    {
        if (string.IsNullOrWhiteSpace(filePath))
            return;

        try { if (File.Exists(filePath)) File.Delete(filePath); } catch { }
        try { if (File.Exists(filePath + ".wal")) File.Delete(filePath + ".wal"); } catch { }
        try { if (File.Exists(filePath + ".manifest.json")) File.Delete(filePath + ".manifest.json"); } catch { }
    }

    private static long GetFileLength(string path)
        => File.Exists(path) ? new FileInfo(path).Length : 0;

    private static string GetCategory(int id)
        => (id % 4) switch
        {
            0 => "Alpha",
            1 => "Beta",
            2 => "Gamma",
            _ => "Delta",
        };
}
