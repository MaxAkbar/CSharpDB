using System.Diagnostics;
using CSharpDB.Client.Models;
using CSharpDB.Engine;
using CSharpDB.Sql;
using CSharpDB.Storage.Diagnostics;

namespace CSharpDB.Client.Internal;

internal sealed partial class EngineTransportClient
{
    public async Task<BackupResult> BackupAsync(BackupRequest request, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentException.ThrowIfNullOrWhiteSpace(request.DestinationPath);

        await _lock.WaitAsync(ct);
        try
        {
            if (!_transactions.IsEmpty)
            {
                throw new CSharpDbClientException(
                    "Backup requires committed state. Commit or rollback active client-managed transactions and retry.");
            }

            var database = await GetDatabaseAsync(ct);
            return MapBackupResult(await DatabaseBackupCoordinator.BackupAsync(
                database,
                _databasePath,
                request.DestinationPath,
                request.WithManifest,
                ct));
        }
        finally
        {
            _lock.Release();
        }
    }

    public async Task<RestoreResult> RestoreAsync(RestoreRequest request, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentException.ThrowIfNullOrWhiteSpace(request.SourcePath);

        if (request.ValidateOnly)
        {
            return MapRestoreResult(await DatabaseBackupCoordinator.ValidateRestoreSourceAsync(
                request.SourcePath,
                ct));
        }

        await using var _ = await AcquireExclusiveDatabaseAccessAsync(
            ct,
            "Restore requires exclusive access. Close active snapshot readers and retry.");

        if (!_transactions.IsEmpty)
        {
            throw new CSharpDbClientException(
                "Restore requires exclusive access. Commit or rollback active client-managed transactions and retry.");
        }

        return MapRestoreResult(await DatabaseBackupCoordinator.RestoreAsync(
            request.SourcePath,
            _databasePath,
            static _ => ValueTask.CompletedTask,
            ct));
    }

    public async Task<ForeignKeyMigrationResult> MigrateForeignKeysAsync(ForeignKeyMigrationRequest request, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        await using var _ = await AcquireExclusiveDatabaseAccessAsync(
            ct,
            "Foreign key migration requires exclusive access. Close active snapshot readers and retry.");

        if (!_transactions.IsEmpty)
        {
            throw new CSharpDbClientException(
                "Foreign key migration requires exclusive access. Commit or rollback active client-managed transactions and retry.");
        }

        ForeignKeyMigrationResult result = MapForeignKeyMigrationResult(
            await DatabaseMaintenanceCoordinator.MigrateForeignKeysAsync(
                _databasePath,
                MapForeignKeyMigrationRequest(request),
                ct));

        return result;
    }

    public async Task<CSharpDB.Client.Models.DatabaseMaintenanceReport> GetMaintenanceReportAsync(CancellationToken ct = default)
        => MapMaintenanceReport(await DatabaseMaintenanceCoordinator.GetMaintenanceReportAsync(_databasePath, ct));

    public Task<ReindexResult> ReindexAsync(ReindexRequest request, CancellationToken ct = default)
        => ReindexCoreAsync(request, ct);

    public Task<VacuumResult> VacuumAsync(CancellationToken ct = default)
        => VacuumCoreAsync(ct);

    public Task<DatabaseInspectReport> InspectStorageAsync(string? databasePath = null, bool includePages = false, CancellationToken ct = default)
        => InspectStorageCoreAsync(databasePath, includePages);

    public Task<WalInspectReport> CheckWalAsync(string? databasePath = null, CancellationToken ct = default)
        => WalInspector.InspectAsync(ResolveDatabasePath(databasePath)).AsTask();

    public Task<PageInspectReport> InspectPageAsync(uint pageId, bool includeHex = false, string? databasePath = null, CancellationToken ct = default)
        => DatabaseInspector.InspectPageAsync(ResolveDatabasePath(databasePath), pageId, includeHex).AsTask();

    public Task<IndexInspectReport> CheckIndexesAsync(string? databasePath = null, string? indexName = null, int? sampleSize = null, CancellationToken ct = default)
        => IndexInspector.CheckAsync(ResolveDatabasePath(databasePath), indexName, sampleSize).AsTask();

    private async Task<DatabaseInspectReport> InspectStorageCoreAsync(string? databasePath, bool includePages)
    {
        string dbPath = ResolveDatabasePath(databasePath);
        return await DatabaseInspector.InspectAsync(dbPath, new DatabaseInspectOptions { IncludePages = includePages });
    }

    private async Task<ReindexResult> ReindexCoreAsync(ReindexRequest request, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(request);

        await using var _ = await AcquireExclusiveDatabaseAccessAsync(
            ct,
            "Maintenance requires exclusive access. Close active snapshot readers and retry.");

        if (!_transactions.IsEmpty)
        {
            throw new CSharpDbClientException(
                "Maintenance requires exclusive access. Commit or rollback active client-managed transactions and retry.");
        }

        return MapReindexResult(await DatabaseMaintenanceCoordinator.ReindexAsync(_databasePath, MapReindexRequest(request), ct));
    }

    private async Task<VacuumResult> VacuumCoreAsync(CancellationToken ct)
    {
        await using var _ = await AcquireExclusiveDatabaseAccessAsync(
            ct,
            "Maintenance requires exclusive access. Close active snapshot readers and retry.");

        if (!_transactions.IsEmpty)
        {
            throw new CSharpDbClientException(
                "Maintenance requires exclusive access. Commit or rollback active client-managed transactions and retry.");
        }

        return MapVacuumResult(await DatabaseMaintenanceCoordinator.VacuumAsync(_databasePath, ct));
    }

    private async Task<SqlExecutionResult> ExecuteSqlCoreAsync(string sql, CancellationToken ct)
    {
        await _lock.WaitAsync(ct);
        try
        {
            await EnsureCatalogsInitializedAsync(ct);
            var db = await GetDatabaseAsync(ct);
            var stopwatch = Stopwatch.StartNew();
            IReadOnlyList<string> statements;
            try
            {
                statements = SqlScriptSplitter.SplitExecutableStatements(sql);
            }
            catch (CSharpDB.Primitives.CSharpDbException ex)
            {
                stopwatch.Stop();
                return new SqlExecutionResult { Error = ex.Message, Elapsed = stopwatch.Elapsed };
            }

            if (statements.Count == 0)
            {
                stopwatch.Stop();
                return new SqlExecutionResult { IsQuery = false, RowsAffected = 0, Elapsed = stopwatch.Elapsed };
            }

            SqlExecutionResult? lastResult = null;
            int totalRowsAffected = 0;

            for (int i = 0; i < statements.Count; i++)
            {
                try
                {
                    var singleResult = await ExecuteQueryAsync(db, statements[i], ct);
                    lastResult = singleResult;
                    if (!singleResult.IsQuery)
                        totalRowsAffected += singleResult.RowsAffected;
                }
                catch (OperationCanceledException)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    stopwatch.Stop();
                    string error = statements.Count > 1 ? $"Statement {i + 1} failed: {ex.Message}" : ex.Message;
                    return new SqlExecutionResult { Error = error, Elapsed = stopwatch.Elapsed };
                }
            }

            stopwatch.Stop();
            if (lastResult is null)
                return new SqlExecutionResult { IsQuery = false, RowsAffected = 0, Elapsed = stopwatch.Elapsed };

            return lastResult.IsQuery
                ? new SqlExecutionResult
                {
                    IsQuery = true,
                    ColumnNames = lastResult.ColumnNames,
                    Rows = lastResult.Rows,
                    RowsAffected = lastResult.RowsAffected,
                    Elapsed = stopwatch.Elapsed,
                }
                : new SqlExecutionResult
                {
                    IsQuery = false,
                    RowsAffected = totalRowsAffected,
                    Elapsed = stopwatch.Elapsed,
                };
        }
        finally { _lock.Release(); }
    }

    private string ResolveDatabasePath(string? databasePath)
    {
        string path = string.IsNullOrWhiteSpace(databasePath) ? _databasePath : databasePath.Trim();
        return Path.GetFullPath(path);
    }

    private static BackupResult MapBackupResult(CSharpDB.Engine.DatabaseBackupResult result)
        => new()
        {
            SourcePath = result.SourcePath,
            DestinationPath = result.DestinationPath,
            ManifestPath = result.ManifestPath,
            DatabaseFileBytes = result.DatabaseFileBytes,
            PhysicalPageCount = result.PhysicalPageCount,
            DeclaredPageCount = result.DeclaredPageCount,
            ChangeCounter = result.ChangeCounter,
            WarningCount = result.WarningCount,
            ErrorCount = result.ErrorCount,
            Sha256 = result.Sha256,
        };

    private static RestoreResult MapRestoreResult(CSharpDB.Engine.DatabaseRestoreResult result)
        => new()
        {
            SourcePath = result.SourcePath,
            DestinationPath = result.DestinationPath,
            ValidateOnly = result.ValidateOnly,
            DatabaseFileBytes = result.DatabaseFileBytes,
            PhysicalPageCount = result.PhysicalPageCount,
            DeclaredPageCount = result.DeclaredPageCount,
            ChangeCounter = result.ChangeCounter,
            SourceWalExists = result.SourceWalExists,
            WarningCount = result.WarningCount,
            ErrorCount = result.ErrorCount,
        };
}
