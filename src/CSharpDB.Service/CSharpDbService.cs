using CSharpDB.Client;
using ClientModels = CSharpDB.Client.Models;
using CSharpDB.Core;
using CSharpDB.Service.Models;
using CSharpDB.Sql;
using CSharpDB.Storage.Diagnostics;
using Microsoft.Extensions.Configuration;

namespace CSharpDB.Service;

[Obsolete("CSharpDB.Service is deprecated and will be removed in v2.0.0. Use CSharpDB.Client instead.")]
public sealed class CSharpDbService : IAsyncDisposable
{
    private const string ProcedureTableName = "__procedures";

    private readonly ICSharpDbClient _client;
    private readonly SemaphoreSlim _lock = new(1, 1);

    public CSharpDbService(IConfiguration configuration)
    {
        string connectionString = configuration.GetConnectionString("CSharpDB")
            ?? "Data Source=csharpdb.db";

        _client = CSharpDbClient.Create(new CSharpDbClientOptions
        {
            ConnectionString = connectionString,
        });
    }

    public async Task InitializeAsync()
        => _ = await _client.GetInfoAsync();

    public string DataSource => _client.DataSource;

    public event Action? TablesChanged;
    public event Action? SchemaChanged;
    public event Action? ProceduresChanged;

    public async Task<IReadOnlyCollection<string>> GetTableNamesAsync()
        => (await WithLockAsync(() => _client.GetTableNamesAsync())).ToArray();

    public async Task<TableSchema?> GetTableSchemaAsync(string tableName)
    {
        var schema = await WithLockAsync(() => _client.GetTableSchemaAsync(tableName));
        return schema is null ? null : MapTableSchema(schema);
    }

    public async Task<IReadOnlyCollection<IndexSchema>> GetIndexesAsync()
        => (await WithLockAsync(() => _client.GetIndexesAsync())).Select(MapIndexSchema).ToArray();

    public async Task<IReadOnlyCollection<ViewDefinition>> GetViewsAsync()
        => (await WithLockAsync(() => _client.GetViewsAsync())).Select(MapViewDefinition).ToArray();

    public async Task<IReadOnlyCollection<string>> GetViewNamesAsync()
        => (await WithLockAsync(() => _client.GetViewNamesAsync())).ToArray();

    public Task<string?> GetViewSqlAsync(string viewName)
        => WithLockAsync(() => _client.GetViewSqlAsync(viewName));

    public async Task<ViewBrowseResult> BrowseViewAsync(string viewName, int page, int pageSize)
        => MapViewBrowseResult(await WithLockAsync(() => _client.BrowseViewAsync(viewName, page, pageSize)));

    public async Task<IReadOnlyCollection<TriggerSchema>> GetTriggersAsync()
        => (await WithLockAsync(() => _client.GetTriggersAsync())).Select(MapTriggerSchema).ToArray();

    public async Task<IReadOnlyList<SavedQueryDefinition>> GetSavedQueriesAsync()
        => (await WithLockAsync(() => _client.GetSavedQueriesAsync())).Select(MapSavedQueryDefinition).ToList();

    public async Task<SavedQueryDefinition?> GetSavedQueryAsync(string name)
    {
        var savedQuery = await WithLockAsync(() => _client.GetSavedQueryAsync(name));
        return savedQuery is null ? null : MapSavedQueryDefinition(savedQuery);
    }

    public async Task<SavedQueryDefinition> UpsertSavedQueryAsync(string name, string sqlText)
        => MapSavedQueryDefinition(await WithLockAsync(() => _client.UpsertSavedQueryAsync(name, sqlText)));

    public Task DeleteSavedQueryAsync(string name)
        => WithLockAsync(() => _client.DeleteSavedQueryAsync(name));

    public async Task<IReadOnlyList<ProcedureDefinition>> GetProceduresAsync(bool includeDisabled = true)
        => (await WithLockAsync(() => _client.GetProceduresAsync(includeDisabled))).Select(MapProcedureDefinition).ToList();

    public async Task<ProcedureDefinition?> GetProcedureAsync(string name)
    {
        var procedure = await WithLockAsync(() => _client.GetProcedureAsync(name));
        return procedure is null ? null : MapProcedureDefinition(procedure);
    }

    public async Task CreateProcedureAsync(ProcedureDefinition definition)
    {
        await WithLockAsync(() => _client.CreateProcedureAsync(MapProcedureDefinition(definition)));
        ProceduresChanged?.Invoke();
    }

    public async Task UpdateProcedureAsync(string existingName, ProcedureDefinition definition)
    {
        await WithLockAsync(() => _client.UpdateProcedureAsync(existingName, MapProcedureDefinition(definition)));
        ProceduresChanged?.Invoke();
    }

    public async Task DeleteProcedureAsync(string name)
    {
        await WithLockAsync(() => _client.DeleteProcedureAsync(name));
        ProceduresChanged?.Invoke();
    }

    public async Task<ProcedureExecutionResult> ExecuteProcedureAsync(string name, IReadOnlyDictionary<string, object?> args)
    {
        var result = MapProcedureExecutionResult(await WithLockAsync(() => _client.ExecuteProcedureAsync(name, args)));
        AnalyzeProcedureEffects(result, out bool schemaMutated, out bool tableMutated, out bool proceduresMutated);
        if (schemaMutated)
            NotifySchemaChanged(tableMutated);
        if (proceduresMutated)
            ProceduresChanged?.Invoke();
        return result;
    }

    public async Task<TableBrowseResult> BrowseTableAsync(string tableName, int page, int pageSize)
        => MapTableBrowseResult(await WithLockAsync(() => _client.BrowseTableAsync(tableName, page, pageSize)));

    public Task<Dictionary<string, object?>?> GetRowByPkAsync(string tableName, string pkColumn, object pkValue)
        => WithLockAsync(() => _client.GetRowByPkAsync(tableName, pkColumn, pkValue));

    public Task<int> InsertRowAsync(string tableName, Dictionary<string, object?> values)
        => WithLockAsync(() => _client.InsertRowAsync(tableName, values));

    public Task<int> UpdateRowAsync(string tableName, string pkColumn, object pkValue, Dictionary<string, object?> values)
        => WithLockAsync(() => _client.UpdateRowAsync(tableName, pkColumn, pkValue, values));

    public Task<int> DeleteRowAsync(string tableName, string pkColumn, object pkValue)
        => WithLockAsync(() => _client.DeleteRowAsync(tableName, pkColumn, pkValue));

    public async Task DropTableAsync(string tableName)
    {
        await WithLockAsync(() => _client.DropTableAsync(tableName));
        NotifySchemaChanged(tablesMayHaveChanged: true);
    }

    public async Task RenameTableAsync(string tableName, string newTableName)
    {
        await WithLockAsync(() => _client.RenameTableAsync(tableName, newTableName));
        NotifySchemaChanged(tablesMayHaveChanged: true);
    }

    public async Task AddColumnAsync(string tableName, string columnName, DbType type, bool notNull)
    {
        await WithLockAsync(() => _client.AddColumnAsync(tableName, columnName, MapDbType(type), notNull));
        NotifySchemaChanged(tablesMayHaveChanged: true);
    }

    public async Task DropColumnAsync(string tableName, string columnName)
    {
        await WithLockAsync(() => _client.DropColumnAsync(tableName, columnName));
        NotifySchemaChanged(tablesMayHaveChanged: true);
    }

    public async Task RenameColumnAsync(string tableName, string oldColumnName, string newColumnName)
    {
        await WithLockAsync(() => _client.RenameColumnAsync(tableName, oldColumnName, newColumnName));
        NotifySchemaChanged(tablesMayHaveChanged: true);
    }

    public async Task CreateIndexAsync(string indexName, string tableName, string columnName, bool isUnique)
    {
        await WithLockAsync(() => _client.CreateIndexAsync(indexName, tableName, columnName, isUnique));
        NotifySchemaChanged(tablesMayHaveChanged: false);
    }

    public async Task UpdateIndexAsync(string existingIndexName, string newIndexName, string tableName, string columnName, bool isUnique)
    {
        await WithLockAsync(() => _client.UpdateIndexAsync(existingIndexName, newIndexName, tableName, columnName, isUnique));
        NotifySchemaChanged(tablesMayHaveChanged: false);
    }

    public async Task DropIndexAsync(string indexName)
    {
        await WithLockAsync(() => _client.DropIndexAsync(indexName));
        NotifySchemaChanged(tablesMayHaveChanged: false);
    }

    public async Task CreateViewAsync(string viewName, string selectSql)
    {
        await WithLockAsync(() => _client.CreateViewAsync(viewName, selectSql));
        NotifySchemaChanged(tablesMayHaveChanged: false);
    }

    public async Task UpdateViewAsync(string existingViewName, string newViewName, string selectSql)
    {
        await WithLockAsync(() => _client.UpdateViewAsync(existingViewName, newViewName, selectSql));
        NotifySchemaChanged(tablesMayHaveChanged: false);
    }

    public async Task DropViewAsync(string viewName)
    {
        await WithLockAsync(() => _client.DropViewAsync(viewName));
        NotifySchemaChanged(tablesMayHaveChanged: false);
    }

    public async Task CreateTriggerAsync(string triggerName, string tableName, TriggerTiming timing, TriggerEvent triggerEvent, string bodySql)
    {
        await WithLockAsync(() => _client.CreateTriggerAsync(triggerName, tableName, MapTriggerTiming(timing), MapTriggerEvent(triggerEvent), bodySql));
        NotifySchemaChanged(tablesMayHaveChanged: false);
    }

    public async Task UpdateTriggerAsync(string existingTriggerName, string newTriggerName, string tableName, TriggerTiming timing, TriggerEvent triggerEvent, string bodySql)
    {
        await WithLockAsync(() => _client.UpdateTriggerAsync(existingTriggerName, newTriggerName, tableName, MapTriggerTiming(timing), MapTriggerEvent(triggerEvent), bodySql));
        NotifySchemaChanged(tablesMayHaveChanged: false);
    }

    public async Task DropTriggerAsync(string triggerName)
    {
        await WithLockAsync(() => _client.DropTriggerAsync(triggerName));
        NotifySchemaChanged(tablesMayHaveChanged: false);
    }

    public async Task<SqlExecutionResult> ExecuteSqlAsync(string sql)
    {
        var result = MapSqlExecutionResult(await WithLockAsync(() => _client.ExecuteSqlAsync(sql)));
        AnalyzeSqlEffects(sql, out bool schemaMutated, out bool tableMutated, out bool proceduresMutated);
        if (schemaMutated)
            NotifySchemaChanged(tableMutated);
        if (proceduresMutated)
            ProceduresChanged?.Invoke();
        return result;
    }

    public Task<DatabaseInspectReport> InspectStorageAsync(string? databasePath = null, bool includePages = false)
        => WithLockAsync(() => _client.InspectStorageAsync(databasePath, includePages));

    public Task<ClientModels.DatabaseMaintenanceReport> GetMaintenanceReportAsync()
        => WithLockAsync(() => _client.GetMaintenanceReportAsync());

    public Task<ClientModels.ReindexResult> ReindexAsync(ClientModels.ReindexRequest request)
        => WithLockAsync(() => _client.ReindexAsync(request));

    public Task<ClientModels.VacuumResult> VacuumAsync()
        => WithLockAsync(() => _client.VacuumAsync());

    public Task<WalInspectReport> CheckWalAsync(string? databasePath = null)
        => WithLockAsync(() => _client.CheckWalAsync(databasePath));

    public Task<PageInspectReport> InspectPageAsync(uint pageId, bool includeHex = false, string? databasePath = null)
        => WithLockAsync(() => _client.InspectPageAsync(pageId, includeHex, databasePath));

    public Task<IndexInspectReport> CheckIndexesAsync(string? databasePath = null, string? indexName = null, int? sampleSize = null)
        => WithLockAsync(() => _client.CheckIndexesAsync(databasePath, indexName, sampleSize));

    public Task<int> GetRowCountAsync(string tableName)
        => WithLockAsync(() => _client.GetRowCountAsync(tableName));

    private void NotifySchemaChanged(bool tablesMayHaveChanged)
    {
        SchemaChanged?.Invoke();
        if (tablesMayHaveChanged)
            TablesChanged?.Invoke();
    }

    private async Task<T> WithLockAsync<T>(Func<Task<T>> action)
    {
        await _lock.WaitAsync();
        try { return await action(); }
        finally { _lock.Release(); }
    }

    private async Task WithLockAsync(Func<Task> action)
    {
        await _lock.WaitAsync();
        try { await action(); }
        finally { _lock.Release(); }
    }

    private static ClientModels.DbType MapDbType(DbType type) => type switch
    {
        DbType.Integer => ClientModels.DbType.Integer,
        DbType.Real => ClientModels.DbType.Real,
        DbType.Text => ClientModels.DbType.Text,
        DbType.Blob => ClientModels.DbType.Blob,
        _ => throw new ArgumentOutOfRangeException(nameof(type)),
    };

    private static DbType MapDbType(ClientModels.DbType type) => type switch
    {
        ClientModels.DbType.Integer => DbType.Integer,
        ClientModels.DbType.Real => DbType.Real,
        ClientModels.DbType.Text => DbType.Text,
        ClientModels.DbType.Blob => DbType.Blob,
        _ => throw new ArgumentOutOfRangeException(nameof(type)),
    };

    private static TableSchema MapTableSchema(ClientModels.TableSchema schema) => new()
    {
        TableName = schema.TableName,
        Columns = schema.Columns.Select(column => new ColumnDefinition
        {
            Name = column.Name,
            Type = MapDbType(column.Type),
            Nullable = column.Nullable,
            IsPrimaryKey = column.IsPrimaryKey,
            IsIdentity = column.IsIdentity,
        }).ToArray(),
    };

    private static IndexSchema MapIndexSchema(ClientModels.IndexSchema index) => new()
    {
        IndexName = index.IndexName,
        TableName = index.TableName,
        Columns = index.Columns.ToArray(),
        IsUnique = index.IsUnique,
    };

    private static ViewDefinition MapViewDefinition(ClientModels.ViewDefinition view) => new()
    {
        Name = view.Name,
        Sql = view.Sql,
    };

    private static TriggerSchema MapTriggerSchema(ClientModels.TriggerSchema trigger) => new()
    {
        TriggerName = trigger.TriggerName,
        TableName = trigger.TableName,
        Timing = MapTriggerTiming(trigger.Timing),
        Event = MapTriggerEvent(trigger.Event),
        BodySql = trigger.BodySql,
    };

    private static ProcedureDefinition MapProcedureDefinition(ClientModels.ProcedureDefinition procedure) => new()
    {
        Name = procedure.Name,
        BodySql = procedure.BodySql,
        Parameters = procedure.Parameters.Select(MapProcedureParameterDefinition).ToList(),
        Description = procedure.Description,
        IsEnabled = procedure.IsEnabled,
        CreatedUtc = procedure.CreatedUtc,
        UpdatedUtc = procedure.UpdatedUtc,
    };

    private static ClientModels.ProcedureDefinition MapProcedureDefinition(ProcedureDefinition procedure) => new()
    {
        Name = procedure.Name,
        BodySql = procedure.BodySql,
        Parameters = procedure.Parameters.Select(MapProcedureParameterDefinition).ToList(),
        Description = procedure.Description,
        IsEnabled = procedure.IsEnabled,
        CreatedUtc = procedure.CreatedUtc,
        UpdatedUtc = procedure.UpdatedUtc,
    };

    private static ProcedureParameterDefinition MapProcedureParameterDefinition(ClientModels.ProcedureParameterDefinition parameter) => new()
    {
        Name = parameter.Name,
        Type = MapDbType(parameter.Type),
        Required = parameter.Required,
        Default = parameter.Default,
        Description = parameter.Description,
    };

    private static ClientModels.ProcedureParameterDefinition MapProcedureParameterDefinition(ProcedureParameterDefinition parameter) => new()
    {
        Name = parameter.Name,
        Type = MapDbType(parameter.Type),
        Required = parameter.Required,
        Default = parameter.Default,
        Description = parameter.Description,
    };

    private static ProcedureExecutionResult MapProcedureExecutionResult(ClientModels.ProcedureExecutionResult result) => new()
    {
        ProcedureName = result.ProcedureName,
        Succeeded = result.Succeeded,
        Statements = result.Statements.Select(statement => new ProcedureStatementExecutionResult
        {
            StatementIndex = statement.StatementIndex,
            StatementText = statement.StatementText,
            IsQuery = statement.IsQuery,
            ColumnNames = statement.ColumnNames,
            Rows = statement.Rows is null ? null : statement.Rows.Select(row => row.ToArray()).ToList(),
            RowsAffected = statement.RowsAffected,
            Elapsed = statement.Elapsed,
        }).ToList(),
        Error = result.Error,
        FailedStatementIndex = result.FailedStatementIndex,
        Elapsed = result.Elapsed,
    };

    private static SavedQueryDefinition MapSavedQueryDefinition(ClientModels.SavedQueryDefinition savedQuery) => new()
    {
        Id = savedQuery.Id,
        Name = savedQuery.Name,
        SqlText = savedQuery.SqlText,
        CreatedUtc = savedQuery.CreatedUtc,
        UpdatedUtc = savedQuery.UpdatedUtc,
    };

    private static TableBrowseResult MapTableBrowseResult(ClientModels.TableBrowseResult result) => new()
    {
        TableName = result.TableName,
        Schema = MapTableSchema(result.Schema),
        Rows = result.Rows.Select(row => row.ToArray()).ToList(),
        TotalRows = result.TotalRows,
        Page = result.Page,
        PageSize = result.PageSize,
    };

    private static ViewBrowseResult MapViewBrowseResult(ClientModels.ViewBrowseResult result) => new()
    {
        ViewName = result.ViewName,
        ColumnNames = result.ColumnNames.ToArray(),
        Rows = result.Rows.Select(row => row.ToArray()).ToList(),
        TotalRows = result.TotalRows,
        Page = result.Page,
        PageSize = result.PageSize,
    };

    private static SqlExecutionResult MapSqlExecutionResult(ClientModels.SqlExecutionResult result) => new()
    {
        IsQuery = result.IsQuery,
        ColumnNames = result.ColumnNames,
        Rows = result.Rows is null ? null : result.Rows.Select(row => row.ToArray()).ToList(),
        RowsAffected = result.RowsAffected,
        Error = result.Error,
        Elapsed = result.Elapsed,
    };

    private static TriggerTiming MapTriggerTiming(ClientModels.TriggerTiming timing) => timing switch
    {
        ClientModels.TriggerTiming.Before => TriggerTiming.Before,
        ClientModels.TriggerTiming.After => TriggerTiming.After,
        _ => throw new ArgumentOutOfRangeException(nameof(timing)),
    };

    private static ClientModels.TriggerTiming MapTriggerTiming(TriggerTiming timing) => timing switch
    {
        TriggerTiming.Before => ClientModels.TriggerTiming.Before,
        TriggerTiming.After => ClientModels.TriggerTiming.After,
        _ => throw new ArgumentOutOfRangeException(nameof(timing)),
    };

    private static TriggerEvent MapTriggerEvent(ClientModels.TriggerEvent triggerEvent) => triggerEvent switch
    {
        ClientModels.TriggerEvent.Insert => TriggerEvent.Insert,
        ClientModels.TriggerEvent.Update => TriggerEvent.Update,
        ClientModels.TriggerEvent.Delete => TriggerEvent.Delete,
        _ => throw new ArgumentOutOfRangeException(nameof(triggerEvent)),
    };

    private static ClientModels.TriggerEvent MapTriggerEvent(TriggerEvent triggerEvent) => triggerEvent switch
    {
        TriggerEvent.Insert => ClientModels.TriggerEvent.Insert,
        TriggerEvent.Update => ClientModels.TriggerEvent.Update,
        TriggerEvent.Delete => ClientModels.TriggerEvent.Delete,
        _ => throw new ArgumentOutOfRangeException(nameof(triggerEvent)),
    };

    private static void AnalyzeSqlEffects(string sql, out bool schemaMutated, out bool tableMutated, out bool proceduresMutated)
    {
        schemaMutated = false;
        tableMutated = false;
        proceduresMutated = false;

        foreach (string statement in SqlScriptSplitter.SplitExecutableStatements(sql))
        {
            if (LooksLikeSchemaMutation(statement))
            {
                schemaMutated = true;
                tableMutated |= LooksLikeTableMutation(statement);
            }

            proceduresMutated |= LooksLikeProcedureMutation(statement);
        }
    }

    private static void AnalyzeProcedureEffects(
        ProcedureExecutionResult result,
        out bool schemaMutated,
        out bool tableMutated,
        out bool proceduresMutated)
    {
        schemaMutated = false;
        tableMutated = false;
        proceduresMutated = false;

        foreach (var statement in result.Statements)
        {
            AnalyzeSqlEffects(statement.StatementText, out bool statementSchemaMutated, out bool statementTableMutated, out bool statementProceduresMutated);
            schemaMutated |= statementSchemaMutated;
            tableMutated |= statementTableMutated;
            proceduresMutated |= statementProceduresMutated;
        }
    }

    private static bool LooksLikeSchemaMutation(string sql)
    {
        string upper = sql.TrimStart().ToUpperInvariant();
        return upper.StartsWith("CREATE ", StringComparison.Ordinal)
            || upper.StartsWith("DROP ", StringComparison.Ordinal)
            || upper.StartsWith("ALTER ", StringComparison.Ordinal);
    }

    private static bool LooksLikeTableMutation(string sql)
    {
        string upper = sql.TrimStart().ToUpperInvariant();
        return upper.StartsWith("CREATE TABLE", StringComparison.Ordinal)
            || upper.StartsWith("DROP TABLE", StringComparison.Ordinal)
            || upper.StartsWith("ALTER TABLE", StringComparison.Ordinal);
    }

    private static bool LooksLikeProcedureMutation(string sql)
    {
        string upper = sql.TrimStart().ToUpperInvariant();
        return upper.StartsWith($"INSERT INTO {ProcedureTableName.ToUpperInvariant()}", StringComparison.Ordinal)
            || upper.StartsWith($"UPDATE {ProcedureTableName.ToUpperInvariant()}", StringComparison.Ordinal)
            || upper.StartsWith($"DELETE FROM {ProcedureTableName.ToUpperInvariant()}", StringComparison.Ordinal)
            || upper.StartsWith($"CREATE TABLE {ProcedureTableName.ToUpperInvariant()}", StringComparison.Ordinal)
            || upper.StartsWith($"CREATE TABLE IF NOT EXISTS {ProcedureTableName.ToUpperInvariant()}", StringComparison.Ordinal)
            || upper.StartsWith($"DROP TABLE {ProcedureTableName.ToUpperInvariant()}", StringComparison.Ordinal)
            || upper.StartsWith($"ALTER TABLE {ProcedureTableName.ToUpperInvariant()}", StringComparison.Ordinal);
    }

    public async ValueTask DisposeAsync()
    {
        await _client.DisposeAsync();
        _lock.Dispose();
    }
}
