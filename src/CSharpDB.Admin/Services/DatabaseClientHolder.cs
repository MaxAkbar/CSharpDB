using System.Text.Json;
using System.Data.Common;
using System.Globalization;
using CSharpDB.Admin.Configuration;
using CSharpDB.Client;
using CSharpDB.Client.Models;
using CSharpDB.Observability;
using CSharpDB.Storage.Diagnostics;
using DbFunctionRegistry = CSharpDB.Primitives.DbFunctionRegistry;

namespace CSharpDB.Admin.Services;

/// <summary>
/// Wraps <see cref="ICSharpDbClient"/> so the underlying client can be swapped
/// at runtime (e.g. when the user opens a different database file).
/// Registered as a singleton; all Blazor circuits share the same instance.
/// </summary>
public sealed class DatabaseClientHolder : ICSharpDbClient, ICSharpDbObservabilityClient, ICSharpDbTableArchiveProgressExporter, ICSharpDbTransactionalSnapshotReader, ICSharpDbShardAdminClient, ICSharpDbShardDirectoryClient
{
    private ICSharpDbClient _inner;
    private ICSharpDbShardAdminClient? _shardAdmin;
    private CSharpDbClientOptions? _baseClientOptions;
    private readonly AdminHostDatabaseOptions _hostDatabaseOptions;
    private readonly DbFunctionRegistry _functions;
    private readonly CSharpDbObservabilityOptions? _observabilityOptions;
    private readonly AdminHostReadinessService? _readiness;
    private readonly object _lock = new();
    private readonly Dictionary<ICSharpDbClient, int> _observabilityLeaseCounts =
        new(ReferenceEqualityComparer.Instance);
    private readonly Dictionary<ICSharpDbClient, TaskCompletionSource<bool>> _observabilityDrainSignals =
        new(ReferenceEqualityComparer.Instance);
    private Task? _disposeTask;

    public event Action? DatabaseChanged;

    public DatabaseClientHolder(
        ICSharpDbClient initial,
        ICSharpDbShardAdminClient? shardAdmin,
        CSharpDbClientOptions? baseClientOptions,
        AdminHostDatabaseOptions hostDatabaseOptions,
        DbFunctionRegistry functions)
        : this(
            initial,
            shardAdmin,
            baseClientOptions,
            hostDatabaseOptions,
            functions,
            readiness: null,
            observabilityOptions: baseClientOptions?.DirectDatabaseOptions?.ObservabilityOptions)
    {
    }

    internal DatabaseClientHolder(
        ICSharpDbClient initial,
        ICSharpDbShardAdminClient? shardAdmin,
        CSharpDbClientOptions? baseClientOptions,
        AdminHostDatabaseOptions hostDatabaseOptions,
        DbFunctionRegistry functions,
        AdminHostReadinessService? readiness)
        : this(
            initial,
            shardAdmin,
            baseClientOptions,
            hostDatabaseOptions,
            functions,
            readiness,
            baseClientOptions?.DirectDatabaseOptions?.ObservabilityOptions)
    {
    }

    internal DatabaseClientHolder(
        ICSharpDbClient initial,
        ICSharpDbShardAdminClient? shardAdmin,
        CSharpDbClientOptions? baseClientOptions,
        AdminHostDatabaseOptions hostDatabaseOptions,
        DbFunctionRegistry functions,
        AdminHostReadinessService? readiness,
        CSharpDbObservabilityOptions? observabilityOptions)
    {
        _inner = initial;
        _shardAdmin = shardAdmin;
        _baseClientOptions = baseClientOptions;
        _hostDatabaseOptions = hostDatabaseOptions;
        _functions = functions;
        _readiness = readiness;
        _observabilityOptions = observabilityOptions;
    }

    public async Task SwitchAsync(string databasePath)
    {
        using IDisposable? readinessLease = _readiness?.EnterDatabaseSwitch();
        CSharpDbClientOptions newOptions = BuildSwitchClientOptions(databasePath);
        ICSharpDbClient newClient;
        ICSharpDbShardAdminClient? newShardAdmin;
        CSharpDbClientOptions? newBaseClientOptions;
        if (CSharpDbShardedClient.TryCreateFromMasterCatalog(newOptions) is { } shardedClient)
        {
            newClient = shardedClient;
            newShardAdmin = shardedClient;
            newBaseClientOptions = null;
        }
        else
        {
            newClient = CSharpDbClient.Create(newOptions);
            newShardAdmin = null;
            newBaseClientOptions = newOptions;
        }

        // Verify the new database is accessible before swapping.
        await newClient.GetInfoAsync();

        await ReplaceClientAsync(newClient, newShardAdmin, newBaseClientOptions);
    }

    internal CSharpDbClientOptions BuildSwitchClientOptions(string databasePath)
        => AdminClientOptionsBuilder.BuildDirectDataSource(
            databasePath,
            _hostDatabaseOptions,
            _functions,
            _observabilityOptions);

    // ── Delegated members ──────────────────────────────────

    public string DataSource => _inner.DataSource;
    public bool SupportsShardAdmin => _shardAdmin is not null;
    public bool SupportsMasterCatalogBootstrap
    {
        get
        {
            lock (_lock)
                return TryResolveDirectDataSource(_baseClientOptions) is not null;
        }
    }

    public string? MasterCatalogDataSource
    {
        get
        {
            lock (_lock)
                return TryResolveDirectDataSource(_baseClientOptions);
        }
    }

    public bool SupportsRouteBoundClients
        => _inner is CSharpDbShardedClient || _baseClientOptions is not null;
    public bool SupportsTableArchiveExport
        => _inner is ICSharpDbTableArchiveExporter exporter && exporter.SupportsTableArchiveExport;
    public bool SupportsTransactionalSnapshotReads
        => _inner is ICSharpDbTransactionalSnapshotReader reader && reader.SupportsTransactionalSnapshotReads;

    public ICSharpDbClient CreateRouteBoundClient(CSharpDbRouteContext routeContext)
    {
        ArgumentNullException.ThrowIfNull(routeContext);

        ICSharpDbClient inner;
        CSharpDbClientOptions? baseClientOptions;
        lock (_lock)
        {
            inner = _inner;
            baseClientOptions = _baseClientOptions;
        }

        if (inner is CSharpDbShardedClient shardedClient)
            return shardedClient.ForRoute(routeContext);

        if (baseClientOptions is null)
        {
            throw new CSharpDbClientConfigurationException(
                "The current CSharpDB connection cannot create a route-bound Admin client.");
        }

        return CSharpDbClient.Create(CloneOptionsWithRoute(baseClientOptions, routeContext));
    }

    public async Task CreateShardCatalogAndReloadAsync(
        CSharpDbShardingOptions activeMap,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(activeMap);
        using IDisposable? readinessLease = _readiness?.EnterDatabaseSwitch();

        CSharpDbClientOptions baseClientOptions;
        lock (_lock)
        {
            if (TryResolveDirectDataSource(_baseClientOptions) is null)
            {
                throw new CSharpDbClientConfigurationException(
                    "The current CSharpDB connection cannot create a master shard catalog. Open a local direct master database first.");
            }

            baseClientOptions = CloneOptions(_baseClientOptions!);
        }

        await CSharpDbShardedClient.SeedMasterCatalogAsync(baseClientOptions, activeMap, ct);

        CSharpDbShardedClient shardedClient =
            await CSharpDbShardedClient.TryCreateFromMasterCatalogAsync(baseClientOptions, ct: ct)
            ?? throw new CSharpDbClientConfigurationException(
                "The master database was seeded, but no active shard map could be loaded.");

        await shardedClient.GetInfoAsync(ct);

        await ReplaceClientAsync(shardedClient, shardedClient, newBaseClientOptions: null);
    }

    internal async Task ReplaceClientAsync(
        ICSharpDbClient newClient,
        ICSharpDbShardAdminClient? newShardAdmin,
        CSharpDbClientOptions? newBaseClientOptions)
    {
        ArgumentNullException.ThrowIfNull(newClient);

        ICSharpDbClient old;
        ICSharpDbShardAdminClient? oldShardAdmin;
        Task<bool> observabilityDrain;
        lock (_lock)
        {
            ObjectDisposedException.ThrowIf(_disposeTask is not null, this);

            old = _inner;
            oldShardAdmin = _shardAdmin;
            _inner = newClient;
            _shardAdmin = newShardAdmin;
            _baseClientOptions = newBaseClientOptions;
            observabilityDrain = GetObservabilityDrainTaskNoLock(old);
        }

        // The replacement is authoritative as soon as the atomic swap above
        // completes. Notify observers before waiting for calls that leased the
        // previous client so UI state cannot display the old database while
        // disposal drains in the background.
        PublishDatabaseChanged();

        // Calls that captured the previous client may still be awaiting it.
        // The holder lock is deliberately not held while they drain.
        bool mayDisposeOldClient = await observabilityDrain.ConfigureAwait(false);

        if (!mayDisposeOldClient)
        {
            // A saturated lease count is deliberately fail-safe. Keep the old
            // client alive until process teardown rather than risk disposing
            // it while an uncounted diagnostics call may still be using it.
            return;
        }

        if (oldShardAdmin is not null && !ReferenceEquals(oldShardAdmin, old))
            await oldShardAdmin.DisposeAsync().ConfigureAwait(false);

        await old.DisposeAsync().ConfigureAwait(false);
    }

    private void PublishDatabaseChanged()
    {
        Action? handlers = DatabaseChanged;
        if (handlers is null)
            return;

        foreach (Delegate handler in handlers.GetInvocationList())
        {
            try { ((Action)handler)(); }
            catch { }
        }
    }

    public Task<DatabaseInfo> GetInfoAsync(CancellationToken ct = default) => _inner.GetInfoAsync(ct);
    public Task<IReadOnlyList<string>> GetTableNamesAsync(CancellationToken ct = default) => _inner.GetTableNamesAsync(ct);
    public Task<TableSchema?> GetTableSchemaAsync(string tableName, CancellationToken ct = default) => _inner.GetTableSchemaAsync(tableName, ct);
    public Task<int> GetRowCountAsync(string tableName, CancellationToken ct = default) => _inner.GetRowCountAsync(tableName, ct);
    public Task<TableBrowseResult> BrowseTableAsync(string tableName, int page = 1, int pageSize = 50, CancellationToken ct = default) => _inner.BrowseTableAsync(tableName, page, pageSize, ct);
    public Task<Dictionary<string, object?>?> GetRowByPkAsync(string tableName, string pkColumn, object pkValue, CancellationToken ct = default) => _inner.GetRowByPkAsync(tableName, pkColumn, pkValue, ct);
    public Task<int> InsertRowAsync(string tableName, Dictionary<string, object?> values, CancellationToken ct = default) => _inner.InsertRowAsync(tableName, values, ct);
    public Task<int> UpdateRowAsync(string tableName, string pkColumn, object pkValue, Dictionary<string, object?> values, CancellationToken ct = default) => _inner.UpdateRowAsync(tableName, pkColumn, pkValue, values, ct);
    public Task<int> DeleteRowAsync(string tableName, string pkColumn, object pkValue, CancellationToken ct = default) => _inner.DeleteRowAsync(tableName, pkColumn, pkValue, ct);
    public Task DropTableAsync(string tableName, CancellationToken ct = default) => _inner.DropTableAsync(tableName, ct);
    public Task RenameTableAsync(string tableName, string newTableName, CancellationToken ct = default) => _inner.RenameTableAsync(tableName, newTableName, ct);
    public Task AddColumnAsync(string tableName, string columnName, Client.Models.DbType type, bool notNull, CancellationToken ct = default) => _inner.AddColumnAsync(tableName, columnName, type, notNull, ct);
    public Task AddColumnAsync(string tableName, string columnName, Client.Models.DbType type, bool notNull, string? collation, CancellationToken ct = default) => _inner.AddColumnAsync(tableName, columnName, type, notNull, collation, ct);
    public Task DropColumnAsync(string tableName, string columnName, CancellationToken ct = default) => _inner.DropColumnAsync(tableName, columnName, ct);
    public Task RenameColumnAsync(string tableName, string oldColumnName, string newColumnName, CancellationToken ct = default) => _inner.RenameColumnAsync(tableName, oldColumnName, newColumnName, ct);
    public Task<IReadOnlyList<IndexSchema>> GetIndexesAsync(CancellationToken ct = default) => _inner.GetIndexesAsync(ct);
    public Task CreateIndexAsync(string indexName, string tableName, string columnName, bool isUnique, CancellationToken ct = default) => _inner.CreateIndexAsync(indexName, tableName, columnName, isUnique, ct);
    public Task CreateIndexAsync(string indexName, string tableName, string columnName, bool isUnique, string? collation, CancellationToken ct = default) => _inner.CreateIndexAsync(indexName, tableName, columnName, isUnique, collation, ct);
    public Task UpdateIndexAsync(string existingIndexName, string newIndexName, string tableName, string columnName, bool isUnique, CancellationToken ct = default) => _inner.UpdateIndexAsync(existingIndexName, newIndexName, tableName, columnName, isUnique, ct);
    public Task UpdateIndexAsync(string existingIndexName, string newIndexName, string tableName, string columnName, bool isUnique, string? collation, CancellationToken ct = default) => _inner.UpdateIndexAsync(existingIndexName, newIndexName, tableName, columnName, isUnique, collation, ct);
    public Task DropIndexAsync(string indexName, CancellationToken ct = default) => _inner.DropIndexAsync(indexName, ct);
    public Task<IReadOnlyList<string>> GetViewNamesAsync(CancellationToken ct = default) => _inner.GetViewNamesAsync(ct);
    public Task<IReadOnlyList<ViewDefinition>> GetViewsAsync(CancellationToken ct = default) => _inner.GetViewsAsync(ct);
    public Task<ViewDefinition?> GetViewAsync(string viewName, CancellationToken ct = default) => _inner.GetViewAsync(viewName, ct);
    public Task<string?> GetViewSqlAsync(string viewName, CancellationToken ct = default) => _inner.GetViewSqlAsync(viewName, ct);
    public Task<ViewBrowseResult> BrowseViewAsync(string viewName, int page = 1, int pageSize = 50, CancellationToken ct = default) => _inner.BrowseViewAsync(viewName, page, pageSize, ct);
    public Task CreateViewAsync(string viewName, string selectSql, CancellationToken ct = default) => _inner.CreateViewAsync(viewName, selectSql, ct);
    public Task UpdateViewAsync(string existingViewName, string newViewName, string selectSql, CancellationToken ct = default) => _inner.UpdateViewAsync(existingViewName, newViewName, selectSql, ct);
    public Task DropViewAsync(string viewName, CancellationToken ct = default) => _inner.DropViewAsync(viewName, ct);
    public Task<IReadOnlyList<TriggerSchema>> GetTriggersAsync(CancellationToken ct = default) => _inner.GetTriggersAsync(ct);
    public Task CreateTriggerAsync(string triggerName, string tableName, TriggerTiming timing, TriggerEvent triggerEvent, string bodySql, CancellationToken ct = default) => _inner.CreateTriggerAsync(triggerName, tableName, timing, triggerEvent, bodySql, ct);
    public Task UpdateTriggerAsync(string existingTriggerName, string newTriggerName, string tableName, TriggerTiming timing, TriggerEvent triggerEvent, string bodySql, CancellationToken ct = default) => _inner.UpdateTriggerAsync(existingTriggerName, newTriggerName, tableName, timing, triggerEvent, bodySql, ct);
    public Task DropTriggerAsync(string triggerName, CancellationToken ct = default) => _inner.DropTriggerAsync(triggerName, ct);
    public Task<IReadOnlyList<SavedQueryDefinition>> GetSavedQueriesAsync(CancellationToken ct = default) => _inner.GetSavedQueriesAsync(ct);
    public Task<SavedQueryDefinition?> GetSavedQueryAsync(string name, CancellationToken ct = default) => _inner.GetSavedQueryAsync(name, ct);
    public Task<SavedQueryDefinition> UpsertSavedQueryAsync(string name, string sqlText, CancellationToken ct = default) => _inner.UpsertSavedQueryAsync(name, sqlText, ct);
    public Task DeleteSavedQueryAsync(string name, CancellationToken ct = default) => _inner.DeleteSavedQueryAsync(name, ct);
    public Task<IReadOnlyList<ProcedureDefinition>> GetProceduresAsync(bool includeDisabled = true, CancellationToken ct = default) => _inner.GetProceduresAsync(includeDisabled, ct);
    public Task<ProcedureDefinition?> GetProcedureAsync(string name, CancellationToken ct = default) => _inner.GetProcedureAsync(name, ct);
    public Task CreateProcedureAsync(ProcedureDefinition definition, CancellationToken ct = default) => _inner.CreateProcedureAsync(definition, ct);
    public Task UpdateProcedureAsync(string existingName, ProcedureDefinition definition, CancellationToken ct = default) => _inner.UpdateProcedureAsync(existingName, definition, ct);
    public Task DeleteProcedureAsync(string name, CancellationToken ct = default) => _inner.DeleteProcedureAsync(name, ct);
    public Task<ProcedureExecutionResult> ExecuteProcedureAsync(string name, IReadOnlyDictionary<string, object?> args, CancellationToken ct = default) => _inner.ExecuteProcedureAsync(name, args, ct);
    public Task<SqlExecutionResult> ExecuteSqlAsync(string sql, CancellationToken ct = default) => _inner.ExecuteSqlAsync(sql, ct);
    public ValueTask<ForwardOnlyQueryCursor?> TryOpenForwardOnlyQueryCursorAsync(string sql, CancellationToken ct = default)
        => _inner is CSharpDbClient client
            ? client.TryOpenForwardOnlyQueryCursorAsync(sql, ct)
            : ValueTask.FromResult<ForwardOnlyQueryCursor?>(null);
    public ValueTask<TransactionTableSnapshot?> ReadTableSnapshotAsync(string transactionId, string tableName, CancellationToken ct = default)
        => _inner is ICSharpDbTransactionalSnapshotReader reader && reader.SupportsTransactionalSnapshotReads
            ? reader.ReadTableSnapshotAsync(transactionId, tableName, ct)
            : ValueTask.FromResult<TransactionTableSnapshot?>(null);
    public ValueTask<ForwardOnlyQueryCursor?> TryOpenForwardOnlyQueryCursorAsync(string transactionId, string sql, CancellationToken ct = default)
        => _inner is ICSharpDbTransactionalSnapshotReader reader && reader.SupportsTransactionalSnapshotReads
            ? reader.TryOpenForwardOnlyQueryCursorAsync(transactionId, sql, ct)
            : ValueTask.FromResult<ForwardOnlyQueryCursor?>(null);
    public Task<TransactionSessionInfo> BeginTransactionAsync(CancellationToken ct = default) => _inner.BeginTransactionAsync(ct);
    public Task<SqlExecutionResult> ExecuteInTransactionAsync(string transactionId, string sql, CancellationToken ct = default) => _inner.ExecuteInTransactionAsync(transactionId, sql, ct);
    public Task CommitTransactionAsync(string transactionId, CancellationToken ct = default) => _inner.CommitTransactionAsync(transactionId, ct);
    public Task RollbackTransactionAsync(string transactionId, CancellationToken ct = default) => _inner.RollbackTransactionAsync(transactionId, ct);
    public Task<IReadOnlyList<string>> GetCollectionNamesAsync(CancellationToken ct = default) => _inner.GetCollectionNamesAsync(ct);
    public Task<int> GetCollectionCountAsync(string collectionName, CancellationToken ct = default) => _inner.GetCollectionCountAsync(collectionName, ct);
    public Task<CollectionBrowseResult> BrowseCollectionAsync(string collectionName, int page = 1, int pageSize = 50, CancellationToken ct = default) => _inner.BrowseCollectionAsync(collectionName, page, pageSize, ct);
    public Task<JsonElement?> GetDocumentAsync(string collectionName, string key, CancellationToken ct = default) => _inner.GetDocumentAsync(collectionName, key, ct);
    public Task PutDocumentAsync(string collectionName, string key, JsonElement document, CancellationToken ct = default) => _inner.PutDocumentAsync(collectionName, key, document, ct);
    public Task<bool> DeleteDocumentAsync(string collectionName, string key, CancellationToken ct = default) => _inner.DeleteDocumentAsync(collectionName, key, ct);
    public Task DropCollectionAsync(string collectionName, CancellationToken ct = default) => _inner.DropCollectionAsync(collectionName, ct);
    public Task CheckpointAsync(CancellationToken ct = default) => _inner.CheckpointAsync(ct);
    public Task<BackupResult> BackupAsync(BackupRequest request, CancellationToken ct = default) => _inner.BackupAsync(request, ct);
    public Task<RestoreResult> RestoreAsync(RestoreRequest request, CancellationToken ct = default) => _inner.RestoreAsync(request, ct);
    public Task<ForeignKeyMigrationResult> MigrateForeignKeysAsync(ForeignKeyMigrationRequest request, CancellationToken ct = default) => _inner.MigrateForeignKeysAsync(request, ct);
    public Task<DatabaseMaintenanceReport> GetMaintenanceReportAsync(CancellationToken ct = default) => _inner.GetMaintenanceReportAsync(ct);
    public Task<ReindexResult> ReindexAsync(ReindexRequest request, CancellationToken ct = default) => _inner.ReindexAsync(request, ct);
    public Task<VacuumResult> VacuumAsync(CancellationToken ct = default) => _inner.VacuumAsync(ct);
    public Task<DatabaseInspectReport> InspectStorageAsync(string? databasePath = null, bool includePages = false, CancellationToken ct = default) => _inner.InspectStorageAsync(databasePath, includePages, ct);
    public Task<WalInspectReport> CheckWalAsync(string? databasePath = null, CancellationToken ct = default) => _inner.CheckWalAsync(databasePath, ct);
    public Task<PageInspectReport> InspectPageAsync(uint pageId, bool includeHex = false, string? databasePath = null, CancellationToken ct = default) => _inner.InspectPageAsync(pageId, includeHex, databasePath, ct);
    public Task<IndexInspectReport> CheckIndexesAsync(string? databasePath = null, string? indexName = null, int? sampleSize = null, CancellationToken ct = default) => _inner.CheckIndexesAsync(databasePath, indexName, sampleSize, ct);

    public Task<DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot>>
        GetRuntimeDiagnosticsAsync(CancellationToken ct = default)
        => DelegateObservabilityAsync(client => client.GetRuntimeDiagnosticsAsync(ct));

    public Task<DiagnosticsTopologySnapshot<DiagnosticsValueSnapshot<StorageRuntimeDiagnosticsSnapshot>>>
        GetStorageDiagnosticsAsync(CancellationToken ct = default)
        => DelegateObservabilityAsync(client => client.GetStorageDiagnosticsAsync(ct));

    public Task<DiagnosticsTopologySnapshot<DiagnosticsValueSnapshot<WalRuntimeDiagnosticsSnapshot>>>
        GetWalDiagnosticsAsync(CancellationToken ct = default)
        => DelegateObservabilityAsync(client => client.GetWalDiagnosticsAsync(ct));

    public Task<DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>>>
        GetActiveQueriesAsync(int maximumRecords, CancellationToken ct = default)
        => DelegateObservabilityAsync(client => client.GetActiveQueriesAsync(maximumRecords, ct));

    public Task<DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<RecentQuerySnapshot>>>
        GetRecentQueriesAsync(int maximumRecords, CancellationToken ct = default)
        => DelegateObservabilityAsync(client => client.GetRecentQueriesAsync(maximumRecords, ct));

    public Task<DiagnosticsTopologySnapshot<DiagnosticsValueSnapshot<QueryPlanDiagnosticsSnapshot>>>
        GetQueryPlanDiagnosticsAsync(OpaqueDiagnosticsId operationId, CancellationToken ct = default)
        => DelegateObservabilityAsync(client => client.GetQueryPlanDiagnosticsAsync(operationId, ct));

    public Task<DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<SessionDiagnosticsSnapshot>>>
        GetSessionsAsync(int maximumRecords, CancellationToken ct = default)
        => DelegateObservabilityAsync(client => client.GetSessionsAsync(maximumRecords, ct));

    public Task<DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<MaintenanceOperationSnapshot>>>
        GetActiveMaintenanceOperationsAsync(int maximumRecords, CancellationToken ct = default)
        => DelegateObservabilityAsync(client => client.GetActiveMaintenanceOperationsAsync(maximumRecords, ct));

    public Task<DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<MaintenanceOperationSnapshot>>>
        GetRecentMaintenanceOperationsAsync(int maximumRecords, CancellationToken ct = default)
        => DelegateObservabilityAsync(client => client.GetRecentMaintenanceOperationsAsync(maximumRecords, ct));

    public Task<DiagnosticsTopologySnapshot<DiagnosticsValueSnapshot<QueryDetailSnapshot>>>
        GetQueryDetailAsync(OpaqueDiagnosticsId operationId, CancellationToken ct = default)
        => DelegateObservabilityAsync(client => client.GetQueryDetailAsync(operationId, ct));

    public Task<TableArchiveExportResult> ExportTableArchiveAsync(string tableName, string path, CancellationToken ct = default)
        => _inner is ICSharpDbTableArchiveExporter exporter && exporter.SupportsTableArchiveExport
            ? exporter.ExportTableArchiveAsync(tableName, path, ct)
            : throw new CSharpDbClientException("Native table archive export is only available for direct CSharpDB transports.");

    public Task<TableArchiveExportResult> ExportTableArchiveAsync(
        string tableName,
        string path,
        IProgress<TableArchiveExportProgress>? progress,
        CancellationToken ct = default)
        => _inner is ICSharpDbTableArchiveProgressExporter progressExporter && progressExporter.SupportsTableArchiveExport
            ? progressExporter.ExportTableArchiveAsync(tableName, path, progress, ct)
            : ExportTableArchiveAsync(tableName, path, ct);

    public Task<CSharpDbShardMapSnapshot> GetShardMapAsync(CancellationToken ct = default)
        => RequireShardAdmin().GetShardMapAsync(ct);

    public Task<CSharpDbShardResolution> ResolveRouteAsync(CSharpDbRouteContext routeContext, CancellationToken ct = default)
        => RequireShardAdmin().ResolveRouteAsync(routeContext, ct);

    public Task<IReadOnlyList<CSharpDbShardStatus>> GetShardStatusAsync(CancellationToken ct = default)
        => RequireShardAdmin().GetShardStatusAsync(ct);

    public Task<IReadOnlyList<CSharpDbShardSqlExecutionResult>> ExecuteSqlOnAllShardsAsync(string sql, CancellationToken ct = default)
        => RequireShardAdmin().ExecuteSqlOnAllShardsAsync(sql, ct);

    public Task<IReadOnlyList<CSharpDbShardSqlExecutionResult>> ExecuteReadOnlySqlOnAllShardsAsync(string sql, CancellationToken ct = default)
        => RequireShardAdmin().ExecuteReadOnlySqlOnAllShardsAsync(sql, ct);

    public Task<CSharpDbShardCatalogState> GetShardCatalogAsync(CancellationToken ct = default)
        => RequireShardAdmin().GetShardCatalogAsync(ct);

    public Task<CSharpDbShardCatalogValidationResult> ValidateShardCatalogUpdateAsync(CSharpDbShardCatalogUpdateRequest request, CancellationToken ct = default)
        => RequireShardAdmin().ValidateShardCatalogUpdateAsync(request, ct);

    public Task<CSharpDbShardCatalogApplyResult> ApplyShardCatalogUpdateAsync(CSharpDbShardCatalogUpdateRequest request, CancellationToken ct = default)
        => RequireShardAdmin().ApplyShardCatalogUpdateAsync(request, ct);

    public Task<CSharpDbShardMigrationResult> MigrateExactRouteKeyAsync(CSharpDbShardExactKeyMigrationRequest request, CancellationToken ct = default)
        => RequireShardAdmin().MigrateExactRouteKeyAsync(request, ct);

    public Task<CSharpDbShardMigrationResult> MigrateBucketRangeAsync(CSharpDbShardBucketRangeMigrationRequest request, CancellationToken ct = default)
        => RequireShardAdmin().MigrateBucketRangeAsync(request, ct);

    public Task<IReadOnlyList<CSharpDbShardMigrationHistoryEntry>> GetShardMigrationHistoryAsync(CancellationToken ct = default)
        => RequireShardAdmin().GetShardMigrationHistoryAsync(ct);

    public Task<IReadOnlyList<CSharpDbShardMigrationProgress>> GetShardMigrationProgressAsync(CancellationToken ct = default)
        => RequireShardAdmin().GetShardMigrationProgressAsync(ct);

    public Task<CSharpDbShardMigrationProgress?> GetShardMigrationProgressAsync(string migrationId, CancellationToken ct = default)
        => RequireShardAdmin().GetShardMigrationProgressAsync(migrationId, ct);

    public Task<CSharpDbShardMigrationResult> ResumeShardMigrationAsync(string migrationId, CancellationToken ct = default)
        => RequireShardAdmin().ResumeShardMigrationAsync(migrationId, ct);

    public Task<CSharpDbShardMigrationResult> RetryShardMigrationAsync(string migrationId, CancellationToken ct = default)
        => RequireShardAdmin().RetryShardMigrationAsync(migrationId, ct);

    public Task<CSharpDbShardDirectoryResolution> ResolveDirectoryEntryAsync(CSharpDbShardDirectoryResolveRequest request, CancellationToken ct = default)
        => RequireShardDirectory().ResolveDirectoryEntryAsync(request, ct);

    public Task<CSharpDbShardDirectoryMutationResult> ReserveDirectoryEntryAsync(CSharpDbShardDirectoryReserveRequest request, CancellationToken ct = default)
        => RequireShardDirectory().ReserveDirectoryEntryAsync(request, ct);

    public Task<CSharpDbShardDirectoryMutationResult> ActivateDirectoryEntryAsync(CSharpDbShardDirectoryActivateRequest request, CancellationToken ct = default)
        => RequireShardDirectory().ActivateDirectoryEntryAsync(request, ct);

    public Task<CSharpDbShardDirectoryMutationResult> UpsertDirectoryEntryAsync(CSharpDbShardDirectoryUpsertRequest request, CancellationToken ct = default)
        => RequireShardDirectory().UpsertDirectoryEntryAsync(request, ct);

    public Task<CSharpDbShardDirectoryMutationResult> DisableDirectoryEntryAsync(CSharpDbShardDirectoryDisableRequest request, CancellationToken ct = default)
        => RequireShardDirectory().DisableDirectoryEntryAsync(request, ct);

    public Task<CSharpDbShardDirectoryMutationResult> DeleteDirectoryEntryAsync(CSharpDbShardDirectoryDeleteRequest request, CancellationToken ct = default)
        => RequireShardDirectory().DeleteDirectoryEntryAsync(request, ct);

    public Task<CSharpDbShardDirectoryMutationResult> MarkDirectoryEntryStaleAsync(CSharpDbShardDirectoryMarkStaleRequest request, CancellationToken ct = default)
        => RequireShardDirectory().MarkDirectoryEntryStaleAsync(request, ct);

    private ICSharpDbShardAdminClient RequireShardAdmin()
        => _shardAdmin
            ?? throw new CSharpDbClientConfigurationException("The current CSharpDB connection does not expose shard-admin APIs.");

    private ICSharpDbShardDirectoryClient RequireShardDirectory()
        => _shardAdmin as ICSharpDbShardDirectoryClient
           ?? throw new CSharpDbClientConfigurationException("The current CSharpDB connection does not expose shard-directory APIs.");

    private Task<TResult> DelegateObservabilityAsync<TResult>(
        Func<ICSharpDbObservabilityClient, Task<TResult>> operation)
    {
        ObservabilityClientLease lease = CaptureObservabilityClient();
        Task<TResult> task;
        try
        {
            // Invoke outside the holder lock so synchronous validation and
            // cancellation behavior come directly from the captured client.
            task = operation(lease.Client)
                ?? throw new InvalidOperationException("The observability client returned no operation task.");
        }
        catch
        {
            lease.Dispose();
            throw;
        }

        if (task.IsCompleted)
        {
            lease.Dispose();
            return task;
        }

        return AwaitObservabilityAsync(task, lease);
    }

    private static async Task<TResult> AwaitObservabilityAsync<TResult>(
        Task<TResult> task,
        ObservabilityClientLease lease)
    {
        try
        {
            return await task.ConfigureAwait(false);
        }
        finally
        {
            lease.Dispose();
        }
    }

    private ObservabilityClientLease CaptureObservabilityClient()
    {
        lock (_lock)
        {
            ObjectDisposedException.ThrowIf(_disposeTask is not null, this);

            ICSharpDbClient inner = _inner;
            if (inner is not ICSharpDbObservabilityClient observabilityClient)
                throw new CSharpDbObservabilityNotSupportedException();

            _observabilityLeaseCounts.TryGetValue(inner, out int leaseCount);
            _observabilityLeaseCounts[inner] = SaturatingIncrementLeaseCount(leaseCount);
            return new ObservabilityClientLease(this, inner, observabilityClient);
        }
    }

    internal static int SaturatingIncrementLeaseCount(int leaseCount)
    {
        if (leaseCount < 0)
            throw new ArgumentOutOfRangeException(nameof(leaseCount));

        return leaseCount == int.MaxValue ? int.MaxValue : leaseCount + 1;
    }

    private Task<bool> GetObservabilityDrainTaskNoLock(ICSharpDbClient client)
    {
        if (!_observabilityLeaseCounts.TryGetValue(client, out int leaseCount))
            return Task.FromResult(true);
        if (leaseCount == int.MaxValue)
            return Task.FromResult(false);

        if (!_observabilityDrainSignals.TryGetValue(client, out TaskCompletionSource<bool>? signal))
        {
            signal = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            _observabilityDrainSignals.Add(client, signal);
        }

        return signal.Task;
    }

    private void ReleaseObservabilityClient(ICSharpDbClient client)
    {
        TaskCompletionSource<bool>? signal = null;
        lock (_lock)
        {
            if (!_observabilityLeaseCounts.TryGetValue(client, out int leaseCount))
                return;

            if (leaseCount == int.MaxValue)
            {
                // Saturation retains the client until process teardown.
                return;
            }

            if (leaseCount > 1)
            {
                _observabilityLeaseCounts[client] = leaseCount - 1;
                return;
            }

            _observabilityLeaseCounts.Remove(client);
            if (_observabilityDrainSignals.Remove(client, out TaskCompletionSource<bool>? removed))
                signal = removed;
        }

        signal?.TrySetResult(true);
    }

    private sealed class ObservabilityClientLease(
        DatabaseClientHolder owner,
        ICSharpDbClient inner,
        ICSharpDbObservabilityClient client) : IDisposable
    {
        private DatabaseClientHolder? _owner = owner;

        public ICSharpDbObservabilityClient Client { get; } = client;

        public void Dispose()
        {
            DatabaseClientHolder? capturedOwner = Interlocked.Exchange(ref _owner, null);
            capturedOwner?.ReleaseObservabilityClient(inner);
        }
    }

    private static CSharpDbClientOptions CloneOptionsWithRoute(
        CSharpDbClientOptions options,
        CSharpDbRouteContext routeContext)
        => CloneOptions(options, routeContext);

    private static CSharpDbClientOptions CloneOptions(
        CSharpDbClientOptions options)
        => CloneOptions(options, routeContext: null);

    private static CSharpDbClientOptions CloneOptions(
        CSharpDbClientOptions options,
        CSharpDbRouteContext? routeContext)
    {
        return new CSharpDbClientOptions
        {
            Transport = options.Transport,
            Endpoint = options.Endpoint,
            ConnectionString = options.ConnectionString,
            DataSource = options.DataSource,
            HttpClient = options.HttpClient,
            ApiKey = options.ApiKey,
            ApiKeyHeaderName = options.ApiKeyHeaderName,
            RouteContext = routeContext,
            DirectDatabaseOptions = options.DirectDatabaseOptions,
            HybridDatabaseOptions = options.HybridDatabaseOptions,
        };
    }

    private static string? TryResolveDirectDataSource(CSharpDbClientOptions? options)
    {
        if (options is null)
            return null;
        if (options.Transport is not null && options.Transport != CSharpDB.Client.CSharpDbTransport.Direct)
            return null;

        string? endpoint = NormalizeOptional(options.Endpoint);
        if (endpoint is not null)
        {
            if (Uri.TryCreate(endpoint, UriKind.Absolute, out Uri? uri))
                return string.Equals(uri.Scheme, Uri.UriSchemeFile, StringComparison.OrdinalIgnoreCase)
                    ? uri.LocalPath
                    : null;

            if (!endpoint.Contains("://", StringComparison.Ordinal))
                return endpoint;
        }

        if (!string.IsNullOrWhiteSpace(options.DataSource))
            return options.DataSource.Trim();

        if (string.IsNullOrWhiteSpace(options.ConnectionString))
            return null;

        var builder = new DbConnectionStringBuilder
        {
            ConnectionString = options.ConnectionString.Trim(),
        };

        foreach (string key in builder.Keys.Cast<string>())
        {
            if (!key.Equals("Data Source", StringComparison.OrdinalIgnoreCase) &&
                !key.Equals("DataSource", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            string? value = Convert.ToString(builder[key], CultureInfo.InvariantCulture);
            return string.IsNullOrWhiteSpace(value) ? null : value.Trim();
        }

        return null;
    }

    private static string? NormalizeOptional(string? value)
        => string.IsNullOrWhiteSpace(value) ? null : value.Trim();

    public ValueTask DisposeAsync()
    {
        lock (_lock)
        {
            if (_disposeTask is not null)
                return new ValueTask(_disposeTask);

            ICSharpDbClient inner = _inner;
            ICSharpDbShardAdminClient? shardAdmin = _shardAdmin;
            Task<bool> observabilityDrain = GetObservabilityDrainTaskNoLock(inner);
            _disposeTask = DisposeAsyncCore(inner, shardAdmin, observabilityDrain);
            return new ValueTask(_disposeTask);
        }
    }

    private static async Task DisposeAsyncCore(
        ICSharpDbClient inner,
        ICSharpDbShardAdminClient? shardAdmin,
        Task<bool> observabilityDrain)
    {
        // Ensure no external disposal work can run while the caller still owns
        // the holder lock used to install this task.
        await Task.Yield();
        if (!await observabilityDrain.ConfigureAwait(false))
            return;

        if (shardAdmin is not null && !ReferenceEquals(shardAdmin, inner))
            await shardAdmin.DisposeAsync().ConfigureAwait(false);

        await inner.DisposeAsync().ConfigureAwait(false);
    }
}
