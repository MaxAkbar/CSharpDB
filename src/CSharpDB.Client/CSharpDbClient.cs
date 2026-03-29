using CSharpDB.Client.Internal;
using CSharpDB.Client.Models;
using CSharpDB.Engine;
using CSharpDB.Storage.Diagnostics;

namespace CSharpDB.Client;

public sealed class CSharpDbClient : ICSharpDbClient, IEngineBackedClient
{
    private readonly ICSharpDbClient _inner;

    private CSharpDbClient(ICSharpDbClient inner)
    {
        _inner = inner;
    }

    public static ICSharpDbClient Create(CSharpDbClientOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        return new CSharpDbClient(ClientTransportResolver.Create(options));
    }

    public string DataSource => _inner.DataSource;
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
    public Task AddColumnAsync(string tableName, string columnName, DbType type, bool notNull, CancellationToken ct = default) => _inner.AddColumnAsync(tableName, columnName, type, notNull, ct);
    public Task AddColumnAsync(string tableName, string columnName, DbType type, bool notNull, string? collation, CancellationToken ct = default) => _inner.AddColumnAsync(tableName, columnName, type, notNull, collation, ct);
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
    public Task<TransactionSessionInfo> BeginTransactionAsync(CancellationToken ct = default) => _inner.BeginTransactionAsync(ct);
    public Task<SqlExecutionResult> ExecuteInTransactionAsync(string transactionId, string sql, CancellationToken ct = default) => _inner.ExecuteInTransactionAsync(transactionId, sql, ct);
    public Task CommitTransactionAsync(string transactionId, CancellationToken ct = default) => _inner.CommitTransactionAsync(transactionId, ct);
    public Task RollbackTransactionAsync(string transactionId, CancellationToken ct = default) => _inner.RollbackTransactionAsync(transactionId, ct);
    public Task<IReadOnlyList<string>> GetCollectionNamesAsync(CancellationToken ct = default) => _inner.GetCollectionNamesAsync(ct);
    public Task<int> GetCollectionCountAsync(string collectionName, CancellationToken ct = default) => _inner.GetCollectionCountAsync(collectionName, ct);
    public Task<CollectionBrowseResult> BrowseCollectionAsync(string collectionName, int page = 1, int pageSize = 50, CancellationToken ct = default) => _inner.BrowseCollectionAsync(collectionName, page, pageSize, ct);
    public Task<System.Text.Json.JsonElement?> GetDocumentAsync(string collectionName, string key, CancellationToken ct = default) => _inner.GetDocumentAsync(collectionName, key, ct);
    public Task PutDocumentAsync(string collectionName, string key, System.Text.Json.JsonElement document, CancellationToken ct = default) => _inner.PutDocumentAsync(collectionName, key, document, ct);
    public Task<bool> DeleteDocumentAsync(string collectionName, string key, CancellationToken ct = default) => _inner.DeleteDocumentAsync(collectionName, key, ct);
    public Task CheckpointAsync(CancellationToken ct = default) => _inner.CheckpointAsync(ct);
    public Task<BackupResult> BackupAsync(BackupRequest request, CancellationToken ct = default) => _inner.BackupAsync(request, ct);
    public Task<RestoreResult> RestoreAsync(RestoreRequest request, CancellationToken ct = default) => _inner.RestoreAsync(request, ct);
    public Task<CSharpDB.Client.Models.DatabaseMaintenanceReport> GetMaintenanceReportAsync(CancellationToken ct = default) => _inner.GetMaintenanceReportAsync(ct);
    public Task<ReindexResult> ReindexAsync(ReindexRequest request, CancellationToken ct = default) => _inner.ReindexAsync(request, ct);
    public Task<VacuumResult> VacuumAsync(CancellationToken ct = default) => _inner.VacuumAsync(ct);
    public Task<DatabaseInspectReport> InspectStorageAsync(string? databasePath = null, bool includePages = false, CancellationToken ct = default) => _inner.InspectStorageAsync(databasePath, includePages, ct);
    public Task<WalInspectReport> CheckWalAsync(string? databasePath = null, CancellationToken ct = default) => _inner.CheckWalAsync(databasePath, ct);
    public Task<PageInspectReport> InspectPageAsync(uint pageId, bool includeHex = false, string? databasePath = null, CancellationToken ct = default) => _inner.InspectPageAsync(pageId, includeHex, databasePath, ct);
    public Task<IndexInspectReport> CheckIndexesAsync(string? databasePath = null, string? indexName = null, int? sampleSize = null, CancellationToken ct = default) => _inner.CheckIndexesAsync(databasePath, indexName, sampleSize, ct);
    public ValueTask DisposeAsync() => _inner.DisposeAsync();
    public ValueTask<Database?> TryGetDatabaseAsync(CancellationToken ct = default)
        => _inner is IEngineBackedClient engineBacked
            ? engineBacked.TryGetDatabaseAsync(ct)
            : ValueTask.FromResult<Database?>(null);
    public ValueTask ReleaseCachedDatabaseAsync(CancellationToken ct = default)
        => _inner is IEngineBackedClient engineBacked
            ? engineBacked.ReleaseCachedDatabaseAsync(ct)
            : ValueTask.CompletedTask;
}
