using System.Diagnostics;
using CSharpDB.Client.Models;
using CSharpDB.Engine;
using CSharpDB.Sql;
using CSharpDB.Storage.Diagnostics;

namespace CSharpDB.Client.Internal;

internal sealed partial class EngineTransportClient
{
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

        await _lock.WaitAsync(ct);
        try
        {
            await PrepareForExclusiveMaintenanceAsync(ct);
            return MapReindexResult(await DatabaseMaintenanceCoordinator.ReindexAsync(_databasePath, MapReindexRequest(request), ct));
        }
        finally
        {
            _lock.Release();
        }
    }

    private async Task<VacuumResult> VacuumCoreAsync(CancellationToken ct)
    {
        await _lock.WaitAsync(ct);
        try
        {
            await PrepareForExclusiveMaintenanceAsync(ct);
            return MapVacuumResult(await DatabaseMaintenanceCoordinator.VacuumAsync(_databasePath, ct));
        }
        finally
        {
            _lock.Release();
        }
    }

    private async Task PrepareForExclusiveMaintenanceAsync(CancellationToken ct)
    {
        if (!_transactions.IsEmpty)
            throw new CSharpDbClientException("Maintenance requires exclusive access. Commit or rollback active client-managed transactions and retry.");

        await ReleaseCachedDatabaseCoreAsync(
            ct,
            "Maintenance requires exclusive access. Close active snapshot readers and retry.");
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
            catch (CSharpDB.Core.CSharpDbException ex)
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
}
