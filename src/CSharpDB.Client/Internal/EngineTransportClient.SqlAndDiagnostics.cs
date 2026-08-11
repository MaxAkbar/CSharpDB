using System.Diagnostics;
using CSharpDB.Client.Models;
using CSharpDB.Engine;
using CSharpDB.Observability;
using CSharpDB.Sql;
using CSharpDB.Storage.Diagnostics;

namespace CSharpDB.Client.Internal;

internal sealed partial class EngineTransportClient
{
    public async Task<BackupResult> BackupAsync(BackupRequest request, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentException.ThrowIfNullOrWhiteSpace(request.DestinationPath);

        IDisposable? diagnosticBoundary = EnterDirectOperationalTransportScope();
        ClientMaintenanceLifetimeLease? maintenanceLifetime = null;
        MaintenanceObservation? operation = null;
        IDisposable? operationScope = null;
        ClientLockLease clientLock = default;
        bool lockTaken = false;
        try
        {
            if (IsClientMaintenanceObservationEnabled())
            {
                maintenanceLifetime = RegisterClientMaintenanceLifetime();
                operation = StartClientMaintenanceObservation(
                    MaintenanceOperationKind.Backup,
                    MaintenanceOperationPhase.Queued,
                    CSharpDbOperationClass.Backup,
                    CSharpDbLogEvents.BackupCompleted);
                operationScope = operation?.EnterScope();
            }

            clientLock = await AcquireClientLockAsync(ct);
            lockTaken = true;
            ThrowIfDisposing();
            if (!_transactions.IsEmpty)
            {
                operation?.Reject(SafeErrorKind.DatabaseBusy);
                throw new CSharpDbClientException(
                    "Backup requires committed state. Commit or rollback active client-managed transactions and retry.");
            }

            operation?.SetPhase(MaintenanceOperationPhase.AcquiringAccess);
            var database = await GetDatabaseAsync(ct);
            return MapBackupResult(
                await DatabaseBackupCoordinator.BackupFromClientAsync(
                    database,
                    _databasePath,
                    request.DestinationPath,
                    request.WithManifest,
                    operation,
                    ct));
        }
        catch (Exception exception)
        {
            operation?.Fail(exception);
            throw;
        }
        finally
        {
            if (lockTaken)
                clientLock.Dispose();
            operationScope?.Dispose();
            maintenanceLifetime?.Dispose();
            diagnosticBoundary?.Dispose();
        }
    }

    public async Task<RestoreResult> RestoreAsync(RestoreRequest request, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentException.ThrowIfNullOrWhiteSpace(request.SourcePath);

        if (request.ValidateOnly)
        {
            IDisposable? diagnosticBoundary =
                EnterDirectOperationalTransportScope();
            ClientMaintenanceLifetimeLease? maintenanceLifetime = null;
            MaintenanceObservation? operation = null;
            IDisposable? operationScope = null;
            try
            {
                if (IsClientMaintenanceObservationEnabled())
                {
                    maintenanceLifetime = RegisterClientMaintenanceLifetime();
                    operation = StartClientMaintenanceObservation(
                        MaintenanceOperationKind.RestoreValidation,
                        MaintenanceOperationPhase.Validating,
                        CSharpDbOperationClass.Restore,
                        CSharpDbLogEvents.RestoreCompleted);
                    operationScope = operation?.EnterScope();
                }
                return MapRestoreResult(
                    await DatabaseBackupCoordinator
                        .ValidateRestoreSourceFromClientAsync(
                            request.SourcePath,
                            operation,
                            ct));
            }
            catch (Exception exception)
            {
                operation?.Fail(exception);
                throw;
            }
            finally
            {
                operationScope?.Dispose();
                maintenanceLifetime?.Dispose();
                diagnosticBoundary?.Dispose();
            }
        }

        IDisposable? fullRestoreBoundary = EnterDirectOperationalTransportScope();
        ClientMaintenanceLifetimeLease? fullRestoreLifetime = null;
        MaintenanceObservation? fullRestoreOperation = null;
        IDisposable? fullRestoreScope = null;
        ExclusiveDatabaseAccessLease? exclusiveLease = null;
        try
        {
            if (IsClientMaintenanceObservationEnabled())
            {
                fullRestoreLifetime = RegisterClientMaintenanceLifetime();
                fullRestoreOperation = StartClientMaintenanceObservation(
                    MaintenanceOperationKind.Restore,
                    MaintenanceOperationPhase.Queued,
                    CSharpDbOperationClass.Restore,
                    CSharpDbLogEvents.RestoreCompleted);
                fullRestoreScope = fullRestoreOperation?.EnterScope();
            }

            if (_databasePath.StartsWith(
                    ":memory:",
                    StringComparison.OrdinalIgnoreCase))
            {
                fullRestoreOperation?.Reject(
                    SafeErrorKind.ClientConfiguration);
                throw new CSharpDbClientException(
                    "Full restore requires a file-backed direct database.");
            }

            exclusiveLease = await AcquireExclusiveDatabaseAccessAsync(
                ct,
                "Restore requires exclusive access. Close active snapshot readers and retry.",
                "Restore requires exclusive access. Commit or rollback active client-managed transactions and retry.",
                fullRestoreOperation);

            return MapRestoreResult(
                await DatabaseBackupCoordinator.RestoreFromClientAsync(
                    request.SourcePath,
                    _databasePath,
                    exclusiveLease.ReopenAndCacheAsync,
                    exclusiveLease.ReopenUnchangedDestinationOnPrePonr,
                    fullRestoreOperation,
                    ct));
        }
        catch (ExclusiveDatabaseAccessRejectedException exception)
        {
            fullRestoreOperation?.Reject(SafeErrorKind.DatabaseBusy);
            throw new CSharpDbClientException(exception.Message);
        }
        catch (Exception exception)
        {
            fullRestoreOperation?.Fail(exception);
            throw;
        }
        finally
        {
            if (exclusiveLease is not null)
                await exclusiveLease.DisposeAsync();
            fullRestoreScope?.Dispose();
            fullRestoreLifetime?.Dispose();
            fullRestoreBoundary?.Dispose();
        }
    }

    public async Task<ForeignKeyMigrationResult> MigrateForeignKeysAsync(ForeignKeyMigrationRequest request, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        IDisposable? diagnosticBoundary = EnterDirectOperationalTransportScope();
        ClientMaintenanceLifetimeLease? maintenanceLifetime = null;
        MaintenanceObservation? operation = null;
        IDisposable? operationScope = null;
        ExclusiveDatabaseAccessLease? exclusiveLease = null;
        try
        {
            if (IsClientMaintenanceObservationEnabled())
            {
                maintenanceLifetime = RegisterClientMaintenanceLifetime();
                operation = StartClientMaintenanceObservation(
                    MaintenanceOperationKind.ForeignKeyMigration,
                    MaintenanceOperationPhase.Queued,
                    CSharpDbOperationClass.Maintenance,
                    CSharpDbLogEvents.MaintenanceCompleted);
                operationScope = operation?.EnterScope();
            }

            exclusiveLease = await AcquireExclusiveDatabaseAccessAsync(
                ct,
                "Foreign key migration requires exclusive access. Close active snapshot readers and retry.",
                "Foreign key migration requires exclusive access. Commit or rollback active client-managed transactions and retry.",
                operation);

            ForeignKeyMigrationResult result = MapForeignKeyMigrationResult(
                await DatabaseMaintenanceCoordinator
                    .MigrateForeignKeysFromClientAsync(
                        _databasePath,
                        MapForeignKeyMigrationRequest(request),
                        operation,
                        ct));
            operation?.Succeed(
                result.AffectedTables,
                result.AffectedTables,
                errorCount: result.ViolationCount);
            return result;
        }
        catch (ExclusiveDatabaseAccessRejectedException exception)
        {
            operation?.Reject(SafeErrorKind.DatabaseBusy);
            throw new CSharpDbClientException(exception.Message);
        }
        catch (Exception exception)
        {
            operation?.Fail(exception);
            throw;
        }
        finally
        {
            if (exclusiveLease is not null)
                await exclusiveLease.DisposeAsync();
            operationScope?.Dispose();
            maintenanceLifetime?.Dispose();
            diagnosticBoundary?.Dispose();
        }
    }

    public async Task<CSharpDB.Client.Models.DatabaseMaintenanceReport> GetMaintenanceReportAsync(CancellationToken ct = default)
        => MapMaintenanceReport(await DatabaseMaintenanceCoordinator.GetMaintenanceReportAsync(_databasePath, ct));

    public Task<ReindexResult> ReindexAsync(ReindexRequest request, CancellationToken ct = default)
        => ReindexCoreAsync(request, ct);

    public Task<VacuumResult> VacuumAsync(CancellationToken ct = default)
        => VacuumCoreAsync(ct);

    public Task<DatabaseInspectReport> InspectStorageAsync(string? databasePath = null, bool includePages = false, CancellationToken ct = default)
        => InspectStorageCoreAsync(databasePath, includePages, ct);

    public Task<WalInspectReport> CheckWalAsync(string? databasePath = null, CancellationToken ct = default)
        => WalInspector.InspectAsync(ResolveDatabasePath(databasePath), ct: ct).AsTask();

    public Task<PageInspectReport> InspectPageAsync(uint pageId, bool includeHex = false, string? databasePath = null, CancellationToken ct = default)
        => DatabaseInspector.InspectPageAsync(ResolveDatabasePath(databasePath), pageId, includeHex, ct).AsTask();

    public Task<IndexInspectReport> CheckIndexesAsync(string? databasePath = null, string? indexName = null, int? sampleSize = null, CancellationToken ct = default)
        => IndexInspector.CheckAsync(ResolveDatabasePath(databasePath), indexName, sampleSize, ct).AsTask();

    private async Task<DatabaseInspectReport> InspectStorageCoreAsync(
        string? databasePath,
        bool includePages,
        CancellationToken ct)
    {
        string dbPath = ResolveDatabasePath(databasePath);
        return await DatabaseInspector.InspectAsync(
            dbPath,
            new DatabaseInspectOptions { IncludePages = includePages },
            ct);
    }

    private async Task<ReindexResult> ReindexCoreAsync(ReindexRequest request, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(request);

        IDisposable? diagnosticBoundary = EnterDirectOperationalTransportScope();
        ClientMaintenanceLifetimeLease? maintenanceLifetime = null;
        MaintenanceObservation? operation = null;
        IDisposable? operationScope = null;
        ExclusiveDatabaseAccessLease? exclusiveLease = null;
        try
        {
            if (IsClientMaintenanceObservationEnabled())
            {
                maintenanceLifetime = RegisterClientMaintenanceLifetime();
                operation = StartClientMaintenanceObservation(
                    MaintenanceOperationKind.Reindex,
                    MaintenanceOperationPhase.Queued,
                    CSharpDbOperationClass.Reindex,
                    CSharpDbLogEvents.MaintenanceCompleted);
                operationScope = operation?.EnterScope();
            }

            exclusiveLease = await AcquireExclusiveDatabaseAccessAsync(
                ct,
                "Maintenance requires exclusive access. Close active snapshot readers and retry.",
                "Maintenance requires exclusive access. Commit or rollback active client-managed transactions and retry.",
                operation);

            ReindexResult result = MapReindexResult(
                await DatabaseMaintenanceCoordinator.ReindexFromClientAsync(
                    _databasePath,
                    MapReindexRequest(request),
                    operation,
                    ct));
            operation?.Succeed(
                result.RebuiltIndexCount,
                result.RebuiltIndexCount,
                warningCount: result.RecoveredCorruptIndexCount);
            return result;
        }
        catch (ExclusiveDatabaseAccessRejectedException exception)
        {
            operation?.Reject(SafeErrorKind.DatabaseBusy);
            throw new CSharpDbClientException(exception.Message);
        }
        catch (Exception exception)
        {
            operation?.Fail(exception);
            throw;
        }
        finally
        {
            if (exclusiveLease is not null)
                await exclusiveLease.DisposeAsync();
            operationScope?.Dispose();
            maintenanceLifetime?.Dispose();
            diagnosticBoundary?.Dispose();
        }
    }

    private async Task<VacuumResult> VacuumCoreAsync(CancellationToken ct)
    {
        IDisposable? diagnosticBoundary = EnterDirectOperationalTransportScope();
        ClientMaintenanceLifetimeLease? maintenanceLifetime = null;
        MaintenanceObservation? operation = null;
        IDisposable? operationScope = null;
        ExclusiveDatabaseAccessLease? exclusiveLease = null;
        try
        {
            if (IsClientMaintenanceObservationEnabled())
            {
                maintenanceLifetime = RegisterClientMaintenanceLifetime();
                operation = StartClientMaintenanceObservation(
                    MaintenanceOperationKind.Vacuum,
                    MaintenanceOperationPhase.Queued,
                    CSharpDbOperationClass.Vacuum,
                    CSharpDbLogEvents.MaintenanceCompleted);
                operationScope = operation?.EnterScope();
            }

            exclusiveLease = await AcquireExclusiveDatabaseAccessAsync(
                ct,
                "Maintenance requires exclusive access. Close active snapshot readers and retry.",
                "Maintenance requires exclusive access. Commit or rollback active client-managed transactions and retry.",
                operation);

            VacuumResult result = MapVacuumResult(
                await DatabaseMaintenanceCoordinator.VacuumFromClientAsync(
                    _databasePath,
                    operation,
                    ct));
            operation?.Succeed();
            return result;
        }
        catch (ExclusiveDatabaseAccessRejectedException exception)
        {
            operation?.Reject(SafeErrorKind.DatabaseBusy);
            throw new CSharpDbClientException(exception.Message);
        }
        catch (Exception exception)
        {
            operation?.Fail(exception);
            throw;
        }
        finally
        {
            if (exclusiveLease is not null)
                await exclusiveLease.DisposeAsync();
            operationScope?.Dispose();
            maintenanceLifetime?.Dispose();
            diagnosticBoundary?.Dispose();
        }
    }

    private async Task<SqlExecutionResult> ExecuteSqlCoreAsync(string sql, CancellationToken ct)
    {
        using IDisposable? transportScope = EnterDirectTransportScope();
        IReadOnlyList<string>? statements = null;
        CSharpDB.Primitives.CSharpDbException? splitError = null;
        try
        {
            statements = SqlScriptSplitter.SplitExecutableStatements(sql);
        }
        catch (CSharpDB.Primitives.CSharpDbException exception)
        {
            splitError = exception;
        }

        CSharpDbOperationClass operationClass = splitError is not null ||
                                                 statements is null ||
                                                 statements.Count != 1
            ? CSharpDbOperationClass.Script
            : CSharpDbOperationClass.Query;
        CompositeQueryOperation? operation = StartCompositeQueryOperation(
            operationClass,
            operationClass == CSharpDbOperationClass.Query ? sql : null);
        using IDisposable? operationScope = operation?.EnterScope();
        ClientLockLease clientLock = default;
        bool lockTaken = false;
        bool queryDispatched = false;
        try
        {
            clientLock = await AcquireClientLockAsync(ct);
            lockTaken = true;
            operation?.MarkDequeued();
            using IDisposable? queueDurationScope = operation?.EnterQueueDurationScope();

            if (splitError is not null)
            {
                operation?.Fail(splitError.Code);
                return new SqlExecutionResult
                {
                    Error = splitError.Message,
                    ErrorCode = splitError.Code,
                    Elapsed = operation?.Elapsed ?? TimeSpan.Zero,
                };
            }

            if (statements is null || statements.Count == 0)
            {
                operation?.Succeed(rowsProduced: 0, rowsAffected: 0);
                return new SqlExecutionResult
                {
                    IsQuery = false,
                    RowsAffected = 0,
                    Elapsed = operation?.Elapsed ?? TimeSpan.Zero,
                };
            }

            await EnsureCatalogsInitializedAsync(ct);
            var db = await GetDatabaseAsync(ct);
            var stopwatch = Stopwatch.StartNew();

            SqlExecutionResult? lastResult = null;
            long totalRowsProduced = 0;
            long observedRowsAffected = 0;
            int totalRowsAffected = 0;

            for (int i = 0; i < statements.Count; i++)
            {
                try
                {
                    if (operationClass == CSharpDbOperationClass.Query)
                        queryDispatched = true;
                    var singleResult = await ExecuteQueryAsync(db, statements[i], ct);
                    lastResult = singleResult;
                    if (singleResult.IsQuery)
                    {
                        totalRowsProduced = AddDiagnosticCount(
                            totalRowsProduced,
                            singleResult.Rows?.Count ?? 0);
                    }
                    else
                    {
                        totalRowsAffected = unchecked(
                            totalRowsAffected + singleResult.RowsAffected);
                        observedRowsAffected = AddDiagnosticCount(
                            observedRowsAffected,
                            singleResult.RowsAffected);
                    }
                }
                catch (OperationCanceledException ex)
                {
                    if (operationClass != CSharpDbOperationClass.Query || !queryDispatched)
                    {
                        operation?.Fail(
                            ex,
                            totalRowsProduced,
                            observedRowsAffected);
                    }
                    throw;
                }
                catch (CSharpDB.Primitives.CSharpDbException ex)
                {
                    stopwatch.Stop();
                    if (operationClass != CSharpDbOperationClass.Query || !queryDispatched)
                    {
                        operation?.Fail(
                            ex.Code,
                            totalRowsProduced,
                            observedRowsAffected);
                    }
                    string error = statements.Count > 1 ? $"Statement {i + 1} failed: {ex.Message}" : ex.Message;
                    return new SqlExecutionResult
                    {
                        Error = error,
                        ErrorCode = ex.Code,
                        Elapsed = stopwatch.Elapsed,
                    };
                }
                catch (Exception ex)
                {
                    stopwatch.Stop();
                    if (operationClass != CSharpDbOperationClass.Query || !queryDispatched)
                    {
                        operation?.Fail(
                            ex,
                            totalRowsProduced,
                            observedRowsAffected);
                    }
                    string error = statements.Count > 1 ? $"Statement {i + 1} failed: {ex.Message}" : ex.Message;
                    return new SqlExecutionResult { Error = error, Elapsed = stopwatch.Elapsed };
                }
            }

            stopwatch.Stop();
            if (lastResult is null)
            {
                operation?.Succeed(rowsProduced: 0, observedRowsAffected);
                return new SqlExecutionResult { IsQuery = false, RowsAffected = 0, Elapsed = stopwatch.Elapsed };
            }

            if (operationClass != CSharpDbOperationClass.Query)
                operation?.Succeed(totalRowsProduced, observedRowsAffected);

            return lastResult.IsQuery
                ? new SqlExecutionResult
                {
                    IsQuery = true,
                    ColumnNames = lastResult.ColumnNames,
                    Columns = lastResult.Columns,
                    ColumnTypes = lastResult.ColumnTypes,
                    ColumnNullability = lastResult.ColumnNullability,
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
        catch (Exception exception)
        {
            if (operationClass != CSharpDbOperationClass.Query || !queryDispatched)
            {
                if (!lockTaken)
                    operation?.MarkDequeued();
                operation?.Fail(exception);
            }
            throw;
        }
        finally
        {
            if (lockTaken)
                clientLock.Dispose();
        }
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
