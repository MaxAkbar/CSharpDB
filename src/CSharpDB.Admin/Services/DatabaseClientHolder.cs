using System.Text.Json;
using CSharpDB.Admin.Configuration;
using CSharpDB.Client;
using CSharpDB.Client.Models;
using CSharpDB.Storage.Diagnostics;
using DbFunctionRegistry = CSharpDB.Primitives.DbFunctionRegistry;

namespace CSharpDB.Admin.Services;

/// <summary>
/// Wraps <see cref="ICSharpDbClient"/> so the underlying client can be swapped
/// at runtime (e.g. when the user opens a different database file).
/// Registered as a singleton; all Blazor circuits share the same instance.
/// </summary>
public sealed class DatabaseClientHolder : ICSharpDbClient, ICSharpDbTableArchiveProgressExporter, ICSharpDbShardAdminClient, ICSharpDbShardDirectoryClient
{
    private ICSharpDbClient _inner;
    private ICSharpDbShardAdminClient? _shardAdmin;
    private CSharpDbClientOptions? _baseClientOptions;
    private readonly AdminHostDatabaseOptions _hostDatabaseOptions;
    private readonly DbFunctionRegistry _functions;
    private readonly object _lock = new();

    public event Action? DatabaseChanged;

    public DatabaseClientHolder(
        ICSharpDbClient initial,
        ICSharpDbShardAdminClient? shardAdmin,
        CSharpDbClientOptions? baseClientOptions,
        AdminHostDatabaseOptions hostDatabaseOptions,
        DbFunctionRegistry functions)
    {
        _inner = initial;
        _shardAdmin = shardAdmin;
        _baseClientOptions = baseClientOptions;
        _hostDatabaseOptions = hostDatabaseOptions;
        _functions = functions;
    }

    public async Task SwitchAsync(string databasePath)
    {
        CSharpDbClientOptions newOptions = AdminClientOptionsBuilder.BuildDirectDataSource(databasePath, _hostDatabaseOptions, _functions);
        var newClient = CSharpDbClient.Create(newOptions);

        // Verify the new database is accessible before swapping.
        await newClient.GetInfoAsync();

        ICSharpDbClient old;
        ICSharpDbShardAdminClient? oldShardAdmin;
        lock (_lock)
        {
            old = _inner;
            oldShardAdmin = _shardAdmin;
            _inner = newClient;
            _shardAdmin = null;
            _baseClientOptions = newOptions;
        }

        if (oldShardAdmin is not null && !ReferenceEquals(oldShardAdmin, old))
            await oldShardAdmin.DisposeAsync();

        await old.DisposeAsync();
        DatabaseChanged?.Invoke();
    }

    // ── Delegated members ──────────────────────────────────

    public string DataSource => _inner.DataSource;
    public bool SupportsShardAdmin => _shardAdmin is not null;
    public bool SupportsRouteBoundClients
        => _inner is CSharpDbShardedClient || _baseClientOptions is not null;
    public bool SupportsTableArchiveExport
        => _inner is ICSharpDbTableArchiveExporter exporter && exporter.SupportsTableArchiveExport;

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

    private static CSharpDbClientOptions CloneOptionsWithRoute(
        CSharpDbClientOptions options,
        CSharpDbRouteContext routeContext)
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

    public async ValueTask DisposeAsync()
    {
        ICSharpDbClient inner = _inner;
        ICSharpDbShardAdminClient? shardAdmin = _shardAdmin;
        if (shardAdmin is not null && !ReferenceEquals(shardAdmin, inner))
            await shardAdmin.DisposeAsync();

        await inner.DisposeAsync();
    }
}
