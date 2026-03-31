using System.Text.Json;
using CSharpDB.Client.Models;
using CSharpDB.Storage.Diagnostics;

namespace CSharpDB.Client;

public interface ICSharpDbClient : IAsyncDisposable
{
    string DataSource { get; }

    Task<DatabaseInfo> GetInfoAsync(CancellationToken ct = default);

    Task<IReadOnlyList<string>> GetTableNamesAsync(CancellationToken ct = default);
    Task<TableSchema?> GetTableSchemaAsync(string tableName, CancellationToken ct = default);
    Task<int> GetRowCountAsync(string tableName, CancellationToken ct = default);
    Task<TableBrowseResult> BrowseTableAsync(string tableName, int page = 1, int pageSize = 50, CancellationToken ct = default);
    Task<Dictionary<string, object?>?> GetRowByPkAsync(string tableName, string pkColumn, object pkValue, CancellationToken ct = default);
    Task<int> InsertRowAsync(string tableName, Dictionary<string, object?> values, CancellationToken ct = default);
    Task<int> UpdateRowAsync(string tableName, string pkColumn, object pkValue, Dictionary<string, object?> values, CancellationToken ct = default);
    Task<int> DeleteRowAsync(string tableName, string pkColumn, object pkValue, CancellationToken ct = default);

    Task DropTableAsync(string tableName, CancellationToken ct = default);
    Task RenameTableAsync(string tableName, string newTableName, CancellationToken ct = default);
    Task AddColumnAsync(string tableName, string columnName, DbType type, bool notNull, CancellationToken ct = default);
    Task AddColumnAsync(string tableName, string columnName, DbType type, bool notNull, string? collation, CancellationToken ct = default);
    Task DropColumnAsync(string tableName, string columnName, CancellationToken ct = default);
    Task RenameColumnAsync(string tableName, string oldColumnName, string newColumnName, CancellationToken ct = default);

    Task<IReadOnlyList<IndexSchema>> GetIndexesAsync(CancellationToken ct = default);
    Task CreateIndexAsync(string indexName, string tableName, string columnName, bool isUnique, CancellationToken ct = default);
    Task CreateIndexAsync(string indexName, string tableName, string columnName, bool isUnique, string? collation, CancellationToken ct = default);
    Task UpdateIndexAsync(string existingIndexName, string newIndexName, string tableName, string columnName, bool isUnique, CancellationToken ct = default);
    Task UpdateIndexAsync(string existingIndexName, string newIndexName, string tableName, string columnName, bool isUnique, string? collation, CancellationToken ct = default);
    Task DropIndexAsync(string indexName, CancellationToken ct = default);

    Task<IReadOnlyList<string>> GetViewNamesAsync(CancellationToken ct = default);
    Task<IReadOnlyList<ViewDefinition>> GetViewsAsync(CancellationToken ct = default);
    Task<ViewDefinition?> GetViewAsync(string viewName, CancellationToken ct = default);
    Task<string?> GetViewSqlAsync(string viewName, CancellationToken ct = default);
    Task<ViewBrowseResult> BrowseViewAsync(string viewName, int page = 1, int pageSize = 50, CancellationToken ct = default);
    Task CreateViewAsync(string viewName, string selectSql, CancellationToken ct = default);
    Task UpdateViewAsync(string existingViewName, string newViewName, string selectSql, CancellationToken ct = default);
    Task DropViewAsync(string viewName, CancellationToken ct = default);

    Task<IReadOnlyList<TriggerSchema>> GetTriggersAsync(CancellationToken ct = default);
    Task CreateTriggerAsync(string triggerName, string tableName, TriggerTiming timing, TriggerEvent triggerEvent, string bodySql, CancellationToken ct = default);
    Task UpdateTriggerAsync(string existingTriggerName, string newTriggerName, string tableName, TriggerTiming timing, TriggerEvent triggerEvent, string bodySql, CancellationToken ct = default);
    Task DropTriggerAsync(string triggerName, CancellationToken ct = default);

    Task<IReadOnlyList<SavedQueryDefinition>> GetSavedQueriesAsync(CancellationToken ct = default);
    Task<SavedQueryDefinition?> GetSavedQueryAsync(string name, CancellationToken ct = default);
    Task<SavedQueryDefinition> UpsertSavedQueryAsync(string name, string sqlText, CancellationToken ct = default);
    Task DeleteSavedQueryAsync(string name, CancellationToken ct = default);

    Task<IReadOnlyList<ProcedureDefinition>> GetProceduresAsync(bool includeDisabled = true, CancellationToken ct = default);
    Task<ProcedureDefinition?> GetProcedureAsync(string name, CancellationToken ct = default);
    Task CreateProcedureAsync(ProcedureDefinition definition, CancellationToken ct = default);
    Task UpdateProcedureAsync(string existingName, ProcedureDefinition definition, CancellationToken ct = default);
    Task DeleteProcedureAsync(string name, CancellationToken ct = default);
    Task<ProcedureExecutionResult> ExecuteProcedureAsync(string name, IReadOnlyDictionary<string, object?> args, CancellationToken ct = default);

    Task<SqlExecutionResult> ExecuteSqlAsync(string sql, CancellationToken ct = default);

    Task<TransactionSessionInfo> BeginTransactionAsync(CancellationToken ct = default);
    Task<SqlExecutionResult> ExecuteInTransactionAsync(string transactionId, string sql, CancellationToken ct = default);
    Task CommitTransactionAsync(string transactionId, CancellationToken ct = default);
    Task RollbackTransactionAsync(string transactionId, CancellationToken ct = default);

    Task<IReadOnlyList<string>> GetCollectionNamesAsync(CancellationToken ct = default);
    Task<int> GetCollectionCountAsync(string collectionName, CancellationToken ct = default);
    Task<CollectionBrowseResult> BrowseCollectionAsync(string collectionName, int page = 1, int pageSize = 50, CancellationToken ct = default);
    Task<JsonElement?> GetDocumentAsync(string collectionName, string key, CancellationToken ct = default);
    Task PutDocumentAsync(string collectionName, string key, JsonElement document, CancellationToken ct = default);
    Task<bool> DeleteDocumentAsync(string collectionName, string key, CancellationToken ct = default);

    Task CheckpointAsync(CancellationToken ct = default);
    Task<BackupResult> BackupAsync(BackupRequest request, CancellationToken ct = default);
    Task<RestoreResult> RestoreAsync(RestoreRequest request, CancellationToken ct = default);
    Task<ForeignKeyMigrationResult> MigrateForeignKeysAsync(ForeignKeyMigrationRequest request, CancellationToken ct = default);
    Task<DatabaseMaintenanceReport> GetMaintenanceReportAsync(CancellationToken ct = default);
    Task<ReindexResult> ReindexAsync(ReindexRequest request, CancellationToken ct = default);
    Task<VacuumResult> VacuumAsync(CancellationToken ct = default);
    Task<DatabaseInspectReport> InspectStorageAsync(string? databasePath = null, bool includePages = false, CancellationToken ct = default);
    Task<WalInspectReport> CheckWalAsync(string? databasePath = null, CancellationToken ct = default);
    Task<PageInspectReport> InspectPageAsync(uint pageId, bool includeHex = false, string? databasePath = null, CancellationToken ct = default);
    Task<IndexInspectReport> CheckIndexesAsync(string? databasePath = null, string? indexName = null, int? sampleSize = null, CancellationToken ct = default);
}
