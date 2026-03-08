using System.Globalization;
using CSharpDB.Client.Models;
using CSharpDB.Engine;

namespace CSharpDB.Client.Internal;

internal sealed partial class EngineTransportClient
{
    private const string ProcedureEnabledIndexName = "idx___procedures_is_enabled";
    private const string SavedQueryNameIndexName = "idx___saved_queries_name";

    public async Task<IReadOnlyList<string>> GetViewNamesAsync(CancellationToken ct = default)
    {
        await _lock.WaitAsync(ct);
        try
        {
            await EnsureCatalogsInitializedAsync(ct);
            var db = await GetDatabaseAsync(ct);
            return db.GetViewNames()
                .OrderBy(name => name, StringComparer.OrdinalIgnoreCase)
                .ToArray();
        }
        finally { _lock.Release(); }
    }

    public async Task<string?> GetViewSqlAsync(string viewName, CancellationToken ct = default)
    {
        await _lock.WaitAsync(ct);
        try
        {
            await EnsureCatalogsInitializedAsync(ct);
            string normalizedViewName = RequireIdentifier(viewName, nameof(viewName));
            return (await GetDatabaseAsync(ct)).GetViewSql(normalizedViewName);
        }
        finally { _lock.Release(); }
    }

    private async Task<DatabaseInfo> GetInfoCoreAsync(CancellationToken ct)
    {
        await _lock.WaitAsync(ct);
        try
        {
            await EnsureCatalogsInitializedAsync(ct);
            var db = await GetDatabaseAsync(ct);

            return new DatabaseInfo
            {
                DataSource = _databasePath,
                TableCount = db.GetTableNames().Count(name => !IsInternalTable(name)),
                IndexCount = db.GetIndexes().Count,
                ViewCount = db.GetViewNames().Count,
                TriggerCount = db.GetTriggers().Count,
                ProcedureCount = await CountRowsViaScalarAsync(db, ProcedureTableName, ct),
                CollectionCount = db.GetCollectionNames().Count,
                SavedQueryCount = await CountRowsViaScalarAsync(db, SavedQueryTableName, ct),
            };
        }
        finally { _lock.Release(); }
    }

    private async Task EnsureCatalogsInitializedAsync(CancellationToken ct)
    {
        if (_catalogsInitialized)
            return;

        var db = await GetDatabaseAsync(ct);
        await EnsureProcedureCatalogAsync(db, ct);
        await EnsureSavedQueryCatalogAsync(db, ct);
        _catalogsInitialized = true;
    }

    private async Task EnsureProcedureCatalogAsync(Database db, CancellationToken ct)
    {
        await ExecuteStatementAsync(db, $"""
            CREATE TABLE IF NOT EXISTS {ProcedureTableName} (
                name TEXT PRIMARY KEY,
                body_sql TEXT NOT NULL,
                params_json TEXT NOT NULL,
                description TEXT,
                is_enabled INTEGER NOT NULL,
                created_utc TEXT NOT NULL,
                updated_utc TEXT NOT NULL
            );
            """, ct);

        await ExecuteStatementAsync(db, $"""
            CREATE INDEX IF NOT EXISTS {ProcedureEnabledIndexName}
            ON {ProcedureTableName} (is_enabled);
            """, ct);
    }

    private async Task EnsureSavedQueryCatalogAsync(Database db, CancellationToken ct)
    {
        await ExecuteStatementAsync(db, $"""
            CREATE TABLE IF NOT EXISTS {SavedQueryTableName} (
                id INTEGER PRIMARY KEY IDENTITY,
                name TEXT NOT NULL,
                sql_text TEXT NOT NULL,
                created_utc TEXT NOT NULL,
                updated_utc TEXT NOT NULL
            );
            """, ct);

        await ExecuteStatementAsync(db, $"""
            CREATE UNIQUE INDEX IF NOT EXISTS {SavedQueryNameIndexName}
            ON {SavedQueryTableName} (name);
            """, ct);
    }

    private static async Task<int> CountRowsViaScalarAsync(Database db, string tableName, CancellationToken ct)
    {
        var result = await ExecuteQueryAsync(db, $"SELECT COUNT(*) FROM {tableName};", ct);
        if (result.Rows is null || result.Rows.Count == 0 || result.Rows[0].Length == 0 || result.Rows[0][0] is null)
            return 0;

        return Convert.ToInt32(result.Rows[0][0], CultureInfo.InvariantCulture);
    }
}
