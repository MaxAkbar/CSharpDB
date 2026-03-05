using System.Data.Common;
using System.Diagnostics;
using System.Globalization;
using System.Text.Json;
using CSharpDB.Service.Models;
using CSharpDB.Core;
using CSharpDB.Data;
using CSharpDB.Sql;
using CSharpDB.Storage.Diagnostics;
using Microsoft.Extensions.Configuration;

namespace CSharpDB.Service;

public sealed class CSharpDbService : IAsyncDisposable
{
    private const string ProcedureTableName = "__procedures";
    private const string ProcedureEnabledIndexName = "idx___procedures_is_enabled";

    private readonly CSharpDbConnection _connection;
    private readonly SemaphoreSlim _lock = new(1, 1);
    private static readonly JsonSerializerOptions s_jsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    public CSharpDbService(IConfiguration configuration)
    {
        string connString = configuration.GetConnectionString("CSharpDB")
            ?? "Data Source=csharpdb.db";
        _connection = new CSharpDbConnection(connString);
    }

    public async Task InitializeAsync()
    {
        await _connection.OpenAsync();

        await _lock.WaitAsync();
        try
        {
            await EnsureProcedureCatalogAsync();
        }
        finally { _lock.Release(); }
    }

    public string DataSource => _connection.DataSource;

    /// <summary>Raised after tables are created or dropped so the sidebar can refresh.</summary>
    public event Action? TablesChanged;
    /// <summary>Raised after schema objects (tables, indexes, views, triggers) change.</summary>
    public event Action? SchemaChanged;
    /// <summary>Raised after procedure definitions change.</summary>
    public event Action? ProceduresChanged;

    private void NotifySchemaChanged(bool tablesMayHaveChanged)
    {
        SchemaChanged?.Invoke();
        if (tablesMayHaveChanged)
            TablesChanged?.Invoke();
    }

    private void NotifyProceduresChanged() => ProceduresChanged?.Invoke();

    // ─── Schema ────────────────────────────────────────────────────

    public async Task<IReadOnlyCollection<string>> GetTableNamesAsync()
    {
        await _lock.WaitAsync();
        try
        {
            return _connection.GetTableNames()
                .Where(name => !IsInternalTable(name))
                .ToArray();
        }
        finally { _lock.Release(); }
    }

    public async Task<TableSchema?> GetTableSchemaAsync(string tableName)
    {
        await _lock.WaitAsync();
        try
        {
            if (IsInternalTable(tableName))
                return null;
            return _connection.GetTableSchema(tableName);
        }
        finally { _lock.Release(); }
    }

    public async Task<IReadOnlyCollection<IndexSchema>> GetIndexesAsync()
    {
        await _lock.WaitAsync();
        try
        {
            return _connection.GetIndexes()
                .OrderBy(i => i.IndexName, StringComparer.OrdinalIgnoreCase)
                .ToArray();
        }
        finally { _lock.Release(); }
    }

    public async Task<IReadOnlyCollection<ViewDefinition>> GetViewsAsync()
    {
        await _lock.WaitAsync();
        try
        {
            var views = new List<ViewDefinition>();
            foreach (var viewName in _connection.GetViewNames().OrderBy(v => v, StringComparer.OrdinalIgnoreCase))
            {
                views.Add(new ViewDefinition
                {
                    Name = viewName,
                    Sql = _connection.GetViewSql(viewName) ?? string.Empty,
                });
            }

            return views;
        }
        finally { _lock.Release(); }
    }

    public async Task<IReadOnlyCollection<string>> GetViewNamesAsync()
    {
        await _lock.WaitAsync();
        try
        {
            return _connection.GetViewNames()
                .OrderBy(v => v, StringComparer.OrdinalIgnoreCase)
                .ToArray();
        }
        finally { _lock.Release(); }
    }

    public async Task<string?> GetViewSqlAsync(string viewName)
    {
        await _lock.WaitAsync();
        try
        {
            ValidateViewName(viewName);
            return _connection.GetViewSql(viewName);
        }
        finally { _lock.Release(); }
    }

    public async Task<ViewBrowseResult> BrowseViewAsync(string viewName, int page, int pageSize)
    {
        await _lock.WaitAsync();
        try
        {
            ValidateViewName(viewName);

            int totalRows = await CountRowsInternal(viewName);
            int offset = (page - 1) * pageSize;
            var rows = new List<object?[]>();

            using var cmd = (CSharpDbCommand)_connection.CreateCommand();
            cmd.CommandText = $"SELECT * FROM {viewName} LIMIT @limit OFFSET @offset;";
            cmd.Parameters.AddWithValue("@limit", pageSize);
            cmd.Parameters.AddWithValue("@offset", offset);

            await using var reader = await cmd.ExecuteReaderAsync();
            var columnNames = new string[reader.FieldCount];
            for (int i = 0; i < reader.FieldCount; i++)
                columnNames[i] = reader.GetName(i);

            while (await reader.ReadAsync())
            {
                var row = new object?[reader.FieldCount];
                for (int i = 0; i < reader.FieldCount; i++)
                    row[i] = reader.IsDBNull(i) ? null : reader.GetValue(i);
                rows.Add(row);
            }

            return new ViewBrowseResult
            {
                ViewName = viewName,
                ColumnNames = columnNames,
                Rows = rows,
                TotalRows = totalRows,
                Page = page,
                PageSize = pageSize,
            };
        }
        finally { _lock.Release(); }
    }

    public async Task<IReadOnlyCollection<TriggerSchema>> GetTriggersAsync()
    {
        await _lock.WaitAsync();
        try
        {
            return _connection.GetTriggers()
                .OrderBy(t => t.TriggerName, StringComparer.OrdinalIgnoreCase)
                .ToArray();
        }
        finally { _lock.Release(); }
    }

    // ─── Procedures ────────────────────────────────────────────────

    public async Task<IReadOnlyList<ProcedureDefinition>> GetProceduresAsync(bool includeDisabled = true)
    {
        await _lock.WaitAsync();
        try
        {
            var procedures = new List<ProcedureDefinition>();
            using var cmd = (CSharpDbCommand)_connection.CreateCommand();
            cmd.CommandText = includeDisabled
                ? $"SELECT name, body_sql, params_json, description, is_enabled, created_utc, updated_utc FROM {ProcedureTableName} ORDER BY name;"
                : $"SELECT name, body_sql, params_json, description, is_enabled, created_utc, updated_utc FROM {ProcedureTableName} WHERE is_enabled = 1 ORDER BY name;";

            await using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
                procedures.Add(ReadProcedureDefinition(reader));

            return procedures;
        }
        finally { _lock.Release(); }
    }

    public async Task<ProcedureDefinition?> GetProcedureAsync(string name)
    {
        await _lock.WaitAsync();
        try
        {
            ValidateIdentifier(name, "procedure name");
            return await GetProcedureInternalAsync(name);
        }
        finally { _lock.Release(); }
    }

    public async Task CreateProcedureAsync(ProcedureDefinition definition)
    {
        ArgumentNullException.ThrowIfNull(definition);

        await _lock.WaitAsync();
        try
        {
            var normalized = NormalizeProcedureDefinition(definition, defaultCreatedUtc: DateTime.UtcNow);
            if (await GetProcedureInternalAsync(normalized.Name) is not null)
                throw new ArgumentException($"Procedure '{normalized.Name}' already exists.");

            string paramsJson = SerializeProcedureParameters(normalized.Parameters);
            string createdUtc = normalized.CreatedUtc.ToString("O", CultureInfo.InvariantCulture);
            string updatedUtc = normalized.UpdatedUtc.ToString("O", CultureInfo.InvariantCulture);

            using var cmd = (CSharpDbCommand)_connection.CreateCommand();
            cmd.CommandText = $"""
                INSERT INTO {ProcedureTableName}
                    (name, body_sql, params_json, description, is_enabled, created_utc, updated_utc)
                VALUES
                    (@name, @body, @params, @description, @enabled, @created, @updated);
                """;
            cmd.Parameters.AddWithValue("@name", normalized.Name);
            cmd.Parameters.AddWithValue("@body", normalized.BodySql);
            cmd.Parameters.AddWithValue("@params", paramsJson);
            cmd.Parameters.AddWithValue("@description", normalized.Description is null ? DBNull.Value : normalized.Description);
            cmd.Parameters.AddWithValue("@enabled", normalized.IsEnabled ? 1L : 0L);
            cmd.Parameters.AddWithValue("@created", createdUtc);
            cmd.Parameters.AddWithValue("@updated", updatedUtc);
            await cmd.ExecuteNonQueryAsync();

            NotifyProceduresChanged();
        }
        finally { _lock.Release(); }
    }

    public async Task UpdateProcedureAsync(string existingName, ProcedureDefinition definition)
    {
        ArgumentNullException.ThrowIfNull(definition);

        await _lock.WaitAsync();
        try
        {
            ValidateIdentifier(existingName, "existing procedure name");
            var existing = await GetProcedureInternalAsync(existingName);
            if (existing is null)
                throw new ArgumentException($"Procedure '{existingName}' not found.");

            var normalized = NormalizeProcedureDefinition(definition, existing.CreatedUtc);
            string paramsJson = SerializeProcedureParameters(normalized.Parameters);
            string createdUtc = normalized.CreatedUtc.ToString("O", CultureInfo.InvariantCulture);
            string updatedUtc = normalized.UpdatedUtc.ToString("O", CultureInfo.InvariantCulture);

            using var cmd = (CSharpDbCommand)_connection.CreateCommand();
            cmd.CommandText = $"""
                UPDATE {ProcedureTableName}
                SET name = @newName,
                    body_sql = @body,
                    params_json = @params,
                    description = @description,
                    is_enabled = @enabled,
                    created_utc = @created,
                    updated_utc = @updated
                WHERE name = @existing;
                """;
            cmd.Parameters.AddWithValue("@newName", normalized.Name);
            cmd.Parameters.AddWithValue("@body", normalized.BodySql);
            cmd.Parameters.AddWithValue("@params", paramsJson);
            cmd.Parameters.AddWithValue("@description", normalized.Description is null ? DBNull.Value : normalized.Description);
            cmd.Parameters.AddWithValue("@enabled", normalized.IsEnabled ? 1L : 0L);
            cmd.Parameters.AddWithValue("@created", createdUtc);
            cmd.Parameters.AddWithValue("@updated", updatedUtc);
            cmd.Parameters.AddWithValue("@existing", existingName);
            int affected = await cmd.ExecuteNonQueryAsync();
            if (affected == 0)
                throw new ArgumentException($"Procedure '{existingName}' not found.");

            NotifyProceduresChanged();
        }
        finally { _lock.Release(); }
    }

    public async Task DeleteProcedureAsync(string name)
    {
        await _lock.WaitAsync();
        try
        {
            ValidateIdentifier(name, "procedure name");
            using var cmd = (CSharpDbCommand)_connection.CreateCommand();
            cmd.CommandText = $"DELETE FROM {ProcedureTableName} WHERE name = @name;";
            cmd.Parameters.AddWithValue("@name", name);
            int affected = await cmd.ExecuteNonQueryAsync();
            if (affected == 0)
                throw new ArgumentException($"Procedure '{name}' not found.");

            NotifyProceduresChanged();
        }
        finally { _lock.Release(); }
    }

    public async Task<ProcedureExecutionResult> ExecuteProcedureAsync(
        string name,
        IReadOnlyDictionary<string, object?> args)
    {
        ArgumentNullException.ThrowIfNull(args);

        await _lock.WaitAsync();
        try
        {
            ValidateIdentifier(name, "procedure name");

            var procedure = await GetProcedureInternalAsync(name);
            if (procedure is null)
                throw new ArgumentException($"Procedure '{name}' not found.");

            if (!procedure.IsEnabled)
            {
                return new ProcedureExecutionResult
                {
                    ProcedureName = name,
                    Succeeded = false,
                    Error = $"Procedure '{name}' is disabled.",
                    Elapsed = TimeSpan.Zero,
                };
            }

            var totalSw = Stopwatch.StartNew();
            Dictionary<string, object?> boundArgs;
            try
            {
                boundArgs = BindProcedureArguments(procedure, args);
            }
            catch (ArgumentException ex)
            {
                totalSw.Stop();
                return new ProcedureExecutionResult
                {
                    ProcedureName = name,
                    Succeeded = false,
                    Error = ex.Message,
                    Elapsed = totalSw.Elapsed,
                };
            }

            IReadOnlyList<string> statements;
            try
            {
                statements = SplitSqlStatements(procedure.BodySql);
            }
            catch (CSharpDbException ex)
            {
                totalSw.Stop();
                return new ProcedureExecutionResult
                {
                    ProcedureName = name,
                    Succeeded = false,
                    Error = ex.Message,
                    Elapsed = totalSw.Elapsed,
                };
            }

            var results = new List<ProcedureStatementExecutionResult>(statements.Count);
            bool schemaMutated = false;
            bool tableMutated = false;
            bool proceduresMutated = false;

            await using var tx = await _connection.BeginTransactionAsync();
            try
            {
                for (int i = 0; i < statements.Count; i++)
                {
                    string statement = statements[i];
                    var single = await ExecuteSingleStatementWithArgumentsAsync(i, statement, boundArgs);
                    results.Add(single);

                    if (LooksLikeSchemaMutation(statement))
                    {
                        schemaMutated = true;
                        tableMutated |= LooksLikeTableMutation(statement);
                    }

                    proceduresMutated |= LooksLikeProcedureMutation(statement);
                }

                await tx.CommitAsync();
            }
            catch (DbException ex)
            {
                await tx.RollbackAsync();
                totalSw.Stop();
                return new ProcedureExecutionResult
                {
                    ProcedureName = name,
                    Succeeded = false,
                    Statements = results,
                    Error = ex.Message,
                    FailedStatementIndex = results.Count,
                    Elapsed = totalSw.Elapsed,
                };
            }
            catch
            {
                await tx.RollbackAsync();
                throw;
            }

            totalSw.Stop();

            if (schemaMutated)
                NotifySchemaChanged(tablesMayHaveChanged: tableMutated);
            if (proceduresMutated)
                NotifyProceduresChanged();

            return new ProcedureExecutionResult
            {
                ProcedureName = name,
                Succeeded = true,
                Statements = results,
                Elapsed = totalSw.Elapsed,
            };
        }
        finally { _lock.Release(); }
    }

    // ─── Browse (paginated) ────────────────────────────────────────

    public async Task<TableBrowseResult> BrowseTableAsync(string tableName, int page, int pageSize)
    {
        await _lock.WaitAsync();
        try
        {
            ValidateTableName(tableName);
            var schema = _connection.GetTableSchema(tableName)!;

            int totalRows = await CountRowsInternal(tableName);

            int offset = (page - 1) * pageSize;
            var rows = new List<object?[]>();

            using var cmd = (CSharpDbCommand)_connection.CreateCommand();
            cmd.CommandText = $"SELECT * FROM {tableName} LIMIT @limit OFFSET @offset;";
            cmd.Parameters.AddWithValue("@limit", pageSize);
            cmd.Parameters.AddWithValue("@offset", offset);

            await using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                var row = new object?[reader.FieldCount];
                for (int i = 0; i < reader.FieldCount; i++)
                    row[i] = reader.IsDBNull(i) ? null : reader.GetValue(i);
                rows.Add(row);
            }

            return new TableBrowseResult
            {
                TableName = tableName,
                Schema = schema,
                Rows = rows,
                TotalRows = totalRows,
                Page = page,
                PageSize = pageSize,
            };
        }
        finally { _lock.Release(); }
    }

    // ─── Single row by PK ──────────────────────────────────────────

    public async Task<Dictionary<string, object?>?> GetRowByPkAsync(
        string tableName, string pkColumn, object pkValue)
    {
        await _lock.WaitAsync();
        try
        {
            ValidateTableName(tableName);
            ValidateColumnName(tableName, pkColumn);

            using var cmd = (CSharpDbCommand)_connection.CreateCommand();
            cmd.CommandText = $"SELECT * FROM {tableName} WHERE {pkColumn} = @pk;";
            cmd.Parameters.AddWithValue("@pk", pkValue);

            await using var reader = await cmd.ExecuteReaderAsync();
            if (!await reader.ReadAsync())
                return null;

            var result = new Dictionary<string, object?>();
            for (int i = 0; i < reader.FieldCount; i++)
            {
                string name = reader.GetName(i);
                result[name] = reader.IsDBNull(i) ? null : reader.GetValue(i);
            }
            return result;
        }
        finally { _lock.Release(); }
    }

    // ─── Insert ────────────────────────────────────────────────────

    public async Task<int> InsertRowAsync(string tableName, Dictionary<string, object?> values)
    {
        await _lock.WaitAsync();
        try
        {
            ValidateTableName(tableName);
            var schema = _connection.GetTableSchema(tableName)!;

            var cols = new List<string>();
            var paramNames = new List<string>();

            using var cmd = (CSharpDbCommand)_connection.CreateCommand();
            int idx = 0;
            foreach (var (col, val) in values)
            {
                ValidateColumnName(tableName, col);
                cols.Add(col);
                string pName = $"@p{idx}";
                paramNames.Add(pName);
                cmd.Parameters.AddWithValue(pName, val ?? DBNull.Value);
                idx++;
            }

            cmd.CommandText = $"INSERT INTO {tableName} ({string.Join(", ", cols)}) VALUES ({string.Join(", ", paramNames)});";
            return await cmd.ExecuteNonQueryAsync();
        }
        finally { _lock.Release(); }
    }

    // ─── Update ────────────────────────────────────────────────────

    public async Task<int> UpdateRowAsync(
        string tableName, string pkColumn, object pkValue, Dictionary<string, object?> values)
    {
        await _lock.WaitAsync();
        try
        {
            ValidateTableName(tableName);
            ValidateColumnName(tableName, pkColumn);

            using var cmd = (CSharpDbCommand)_connection.CreateCommand();
            var setClauses = new List<string>();
            int idx = 0;
            foreach (var (col, val) in values)
            {
                if (col == pkColumn) continue;
                ValidateColumnName(tableName, col);
                string pName = $"@p{idx}";
                setClauses.Add($"{col} = {pName}");
                cmd.Parameters.AddWithValue(pName, val ?? DBNull.Value);
                idx++;
            }

            cmd.Parameters.AddWithValue("@pk", pkValue);
            cmd.CommandText = $"UPDATE {tableName} SET {string.Join(", ", setClauses)} WHERE {pkColumn} = @pk;";
            return await cmd.ExecuteNonQueryAsync();
        }
        finally { _lock.Release(); }
    }

    // ─── Delete ────────────────────────────────────────────────────

    public async Task<int> DeleteRowAsync(string tableName, string pkColumn, object pkValue)
    {
        await _lock.WaitAsync();
        try
        {
            ValidateTableName(tableName);
            ValidateColumnName(tableName, pkColumn);

            using var cmd = (CSharpDbCommand)_connection.CreateCommand();
            cmd.CommandText = $"DELETE FROM {tableName} WHERE {pkColumn} = @pk;";
            cmd.Parameters.AddWithValue("@pk", pkValue);
            return await cmd.ExecuteNonQueryAsync();
        }
        finally { _lock.Release(); }
    }

    // ─── Drop Table ────────────────────────────────────────────────

    public async Task DropTableAsync(string tableName)
    {
        await _lock.WaitAsync();
        try
        {
            ValidateTableName(tableName);
            using var cmd = (CSharpDbCommand)_connection.CreateCommand();
            cmd.CommandText = $"DROP TABLE {tableName};";
            await cmd.ExecuteNonQueryAsync();
            NotifySchemaChanged(tablesMayHaveChanged: true);
        }
        finally { _lock.Release(); }
    }

    public async Task RenameTableAsync(string tableName, string newTableName)
    {
        await _lock.WaitAsync();
        try
        {
            ValidateTableName(tableName);
            ValidateIdentifier(newTableName, "new table name");
            await ExecuteNonQueryAsync($"ALTER TABLE {tableName} RENAME TO {newTableName};");
            NotifySchemaChanged(tablesMayHaveChanged: true);
        }
        finally { _lock.Release(); }
    }

    public async Task AddColumnAsync(string tableName, string columnName, DbType type, bool notNull)
    {
        await _lock.WaitAsync();
        try
        {
            ValidateTableName(tableName);
            ValidateIdentifier(columnName, "column name");
            string nullClause = notNull ? " NOT NULL" : string.Empty;
            await ExecuteNonQueryAsync($"ALTER TABLE {tableName} ADD COLUMN {columnName} {type.ToString().ToUpperInvariant()}{nullClause};");
            NotifySchemaChanged(tablesMayHaveChanged: true);
        }
        finally { _lock.Release(); }
    }

    public async Task DropColumnAsync(string tableName, string columnName)
    {
        await _lock.WaitAsync();
        try
        {
            ValidateTableName(tableName);
            ValidateColumnName(tableName, columnName);
            await ExecuteNonQueryAsync($"ALTER TABLE {tableName} DROP COLUMN {columnName};");
            NotifySchemaChanged(tablesMayHaveChanged: true);
        }
        finally { _lock.Release(); }
    }

    public async Task RenameColumnAsync(string tableName, string oldColumnName, string newColumnName)
    {
        await _lock.WaitAsync();
        try
        {
            ValidateTableName(tableName);
            ValidateColumnName(tableName, oldColumnName);
            ValidateIdentifier(newColumnName, "new column name");
            await ExecuteNonQueryAsync($"ALTER TABLE {tableName} RENAME COLUMN {oldColumnName} TO {newColumnName};");
            NotifySchemaChanged(tablesMayHaveChanged: true);
        }
        finally { _lock.Release(); }
    }

    public async Task CreateIndexAsync(string indexName, string tableName, string columnName, bool isUnique)
    {
        await _lock.WaitAsync();
        try
        {
            ValidateIdentifier(indexName, "index name");
            ValidateTableName(tableName);
            ValidateColumnName(tableName, columnName);

            string uniqueClause = isUnique ? "UNIQUE " : string.Empty;
            await ExecuteNonQueryAsync($"CREATE {uniqueClause}INDEX {indexName} ON {tableName} ({columnName});");
            NotifySchemaChanged(tablesMayHaveChanged: false);
        }
        finally { _lock.Release(); }
    }

    public async Task UpdateIndexAsync(
        string existingIndexName,
        string newIndexName,
        string tableName,
        string columnName,
        bool isUnique)
    {
        await _lock.WaitAsync();
        try
        {
            ValidateIdentifier(existingIndexName, "existing index name");
            ValidateIdentifier(newIndexName, "new index name");
            ValidateTableName(tableName);
            ValidateColumnName(tableName, columnName);

            await using var tx = await _connection.BeginTransactionAsync();
            try
            {
                await ExecuteNonQueryAsync($"DROP INDEX {existingIndexName};");

                string uniqueClause = isUnique ? "UNIQUE " : string.Empty;
                await ExecuteNonQueryAsync($"CREATE {uniqueClause}INDEX {newIndexName} ON {tableName} ({columnName});");
                await tx.CommitAsync();
            }
            catch
            {
                await tx.RollbackAsync();
                throw;
            }

            NotifySchemaChanged(tablesMayHaveChanged: false);
        }
        finally { _lock.Release(); }
    }

    public async Task DropIndexAsync(string indexName)
    {
        await _lock.WaitAsync();
        try
        {
            ValidateIdentifier(indexName, "index name");
            using var cmd = (CSharpDbCommand)_connection.CreateCommand();
            cmd.CommandText = $"DROP INDEX {indexName};";
            await cmd.ExecuteNonQueryAsync();
            NotifySchemaChanged(tablesMayHaveChanged: false);
        }
        finally { _lock.Release(); }
    }

    public async Task CreateViewAsync(string viewName, string selectSql)
    {
        await _lock.WaitAsync();
        try
        {
            ValidateIdentifier(viewName, "view name");
            string normalizedSelect = NormalizeSqlFragment(selectSql, "view query");
            await ExecuteNonQueryAsync($"CREATE VIEW {viewName} AS {normalizedSelect};");
            NotifySchemaChanged(tablesMayHaveChanged: false);
        }
        finally { _lock.Release(); }
    }

    public async Task UpdateViewAsync(string existingViewName, string newViewName, string selectSql)
    {
        await _lock.WaitAsync();
        try
        {
            ValidateIdentifier(existingViewName, "existing view name");
            ValidateIdentifier(newViewName, "new view name");
            string normalizedSelect = NormalizeSqlFragment(selectSql, "view query");

            await using var tx = await _connection.BeginTransactionAsync();
            try
            {
                await ExecuteNonQueryAsync($"DROP VIEW {existingViewName};");
                await ExecuteNonQueryAsync($"CREATE VIEW {newViewName} AS {normalizedSelect};");
                await tx.CommitAsync();
            }
            catch
            {
                await tx.RollbackAsync();
                throw;
            }

            NotifySchemaChanged(tablesMayHaveChanged: false);
        }
        finally { _lock.Release(); }
    }

    public async Task DropViewAsync(string viewName)
    {
        await _lock.WaitAsync();
        try
        {
            ValidateIdentifier(viewName, "view name");
            using var cmd = (CSharpDbCommand)_connection.CreateCommand();
            cmd.CommandText = $"DROP VIEW {viewName};";
            await cmd.ExecuteNonQueryAsync();
            NotifySchemaChanged(tablesMayHaveChanged: false);
        }
        finally { _lock.Release(); }
    }

    public async Task CreateTriggerAsync(
        string triggerName,
        string tableName,
        TriggerTiming timing,
        TriggerEvent triggerEvent,
        string bodySql)
    {
        await _lock.WaitAsync();
        try
        {
            ValidateIdentifier(triggerName, "trigger name");
            ValidateTableName(tableName);
            string normalizedBody = NormalizeSqlFragment(bodySql, "trigger body");

            await ExecuteNonQueryAsync(
                $"CREATE TRIGGER {triggerName} {timing.ToString().ToUpperInvariant()} {triggerEvent.ToString().ToUpperInvariant()} ON {tableName} BEGIN {normalizedBody}; END;");

            NotifySchemaChanged(tablesMayHaveChanged: false);
        }
        finally { _lock.Release(); }
    }

    public async Task UpdateTriggerAsync(
        string existingTriggerName,
        string newTriggerName,
        string tableName,
        TriggerTiming timing,
        TriggerEvent triggerEvent,
        string bodySql)
    {
        await _lock.WaitAsync();
        try
        {
            ValidateIdentifier(existingTriggerName, "existing trigger name");
            ValidateIdentifier(newTriggerName, "new trigger name");
            ValidateTableName(tableName);
            string normalizedBody = NormalizeSqlFragment(bodySql, "trigger body");

            await using var tx = await _connection.BeginTransactionAsync();
            try
            {
                await ExecuteNonQueryAsync($"DROP TRIGGER {existingTriggerName};");
                await ExecuteNonQueryAsync(
                    $"CREATE TRIGGER {newTriggerName} {timing.ToString().ToUpperInvariant()} {triggerEvent.ToString().ToUpperInvariant()} ON {tableName} BEGIN {normalizedBody}; END;");
                await tx.CommitAsync();
            }
            catch
            {
                await tx.RollbackAsync();
                throw;
            }

            NotifySchemaChanged(tablesMayHaveChanged: false);
        }
        finally { _lock.Release(); }
    }

    public async Task DropTriggerAsync(string triggerName)
    {
        await _lock.WaitAsync();
        try
        {
            ValidateIdentifier(triggerName, "trigger name");
            using var cmd = (CSharpDbCommand)_connection.CreateCommand();
            cmd.CommandText = $"DROP TRIGGER {triggerName};";
            await cmd.ExecuteNonQueryAsync();
            NotifySchemaChanged(tablesMayHaveChanged: false);
        }
        finally { _lock.Release(); }
    }

    // ─── SQL Console ───────────────────────────────────────────────

    public async Task<SqlExecutionResult> ExecuteSqlAsync(string sql)
    {
        await _lock.WaitAsync();
        try
        {
            var sw = Stopwatch.StartNew();
            IReadOnlyList<string> statements;
            try
            {
                statements = SplitSqlStatements(sql);
            }
            catch (CSharpDbException ex)
            {
                sw.Stop();
                return new SqlExecutionResult
                {
                    Error = ex.Message,
                    Elapsed = sw.Elapsed,
                };
            }

            if (statements.Count == 0)
            {
                sw.Stop();
                return new SqlExecutionResult
                {
                    IsQuery = false,
                    RowsAffected = 0,
                    Elapsed = sw.Elapsed,
                };
            }

            SqlExecutionResult? lastResult = null;
            int totalRowsAffected = 0;
            bool schemaMutated = false;
            bool tableMutated = false;
            bool proceduresMutated = false;

            for (int i = 0; i < statements.Count; i++)
            {
                string statement = statements[i];

                try
                {
                    var singleResult = await ExecuteSingleStatementAsync(statement);
                    lastResult = singleResult;

                    if (!singleResult.IsQuery)
                        totalRowsAffected += singleResult.RowsAffected;

                    if (LooksLikeSchemaMutation(statement))
                    {
                        schemaMutated = true;
                        tableMutated |= LooksLikeTableMutation(statement);
                    }

                    proceduresMutated |= LooksLikeProcedureMutation(statement);
                }
                catch (DbException ex)
                {
                    sw.Stop();

                    if (schemaMutated)
                        NotifySchemaChanged(tablesMayHaveChanged: tableMutated);
                    if (proceduresMutated)
                        NotifyProceduresChanged();

                    string error = statements.Count > 1
                        ? $"Statement {i + 1} failed: {ex.Message}"
                        : ex.Message;

                    return new SqlExecutionResult
                    {
                        Error = error,
                        Elapsed = sw.Elapsed,
                    };
                }
            }

            sw.Stop();

            if (schemaMutated)
                NotifySchemaChanged(tablesMayHaveChanged: tableMutated);
            if (proceduresMutated)
                NotifyProceduresChanged();

            if (lastResult is null)
            {
                return new SqlExecutionResult
                {
                    IsQuery = false,
                    RowsAffected = 0,
                    Elapsed = sw.Elapsed,
                };
            }

            if (lastResult.IsQuery)
            {
                return new SqlExecutionResult
                {
                    IsQuery = true,
                    ColumnNames = lastResult.ColumnNames,
                    Rows = lastResult.Rows,
                    RowsAffected = lastResult.RowsAffected,
                    Elapsed = sw.Elapsed,
                };
            }

            return new SqlExecutionResult
            {
                IsQuery = false,
                RowsAffected = totalRowsAffected,
                Elapsed = sw.Elapsed,
            };
        }
        finally { _lock.Release(); }
    }

    // ─── Storage Diagnostics (read-only) ───────────────────────────

    public async Task<DatabaseInspectReport> InspectStorageAsync(
        string? databasePath = null,
        bool includePages = false)
    {
        await _lock.WaitAsync();
        try
        {
            string dbPath = ResolveDatabasePath(databasePath);
            return await DatabaseInspector.InspectAsync(
                dbPath,
                new DatabaseInspectOptions { IncludePages = includePages });
        }
        finally { _lock.Release(); }
    }

    public async Task<WalInspectReport> CheckWalAsync(string? databasePath = null)
    {
        await _lock.WaitAsync();
        try
        {
            string dbPath = ResolveDatabasePath(databasePath);
            return await WalInspector.InspectAsync(dbPath);
        }
        finally { _lock.Release(); }
    }

    public async Task<PageInspectReport> InspectPageAsync(
        uint pageId,
        bool includeHex = false,
        string? databasePath = null)
    {
        await _lock.WaitAsync();
        try
        {
            string dbPath = ResolveDatabasePath(databasePath);
            return await DatabaseInspector.InspectPageAsync(dbPath, pageId, includeHex);
        }
        finally { _lock.Release(); }
    }

    public async Task<IndexInspectReport> CheckIndexesAsync(
        string? databasePath = null,
        string? indexName = null,
        int? sampleSize = null)
    {
        await _lock.WaitAsync();
        try
        {
            string dbPath = ResolveDatabasePath(databasePath);
            return await IndexInspector.CheckAsync(dbPath, indexName, sampleSize);
        }
        finally { _lock.Release(); }
    }

    // ─── Row count ─────────────────────────────────────────────────

    public async Task<int> GetRowCountAsync(string tableName)
    {
        await _lock.WaitAsync();
        try
        {
            ValidateTableName(tableName);
            return await CountRowsInternal(tableName);
        }
        finally { _lock.Release(); }
    }

    // ─── Helpers ───────────────────────────────────────────────────

    private async Task<int> CountRowsInternal(string tableName)
    {
        using var cmd = (CSharpDbCommand)_connection.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM {tableName};";
        object? scalar = await cmd.ExecuteScalarAsync();
        if (scalar is null || scalar is DBNull)
            return 0;
        return Convert.ToInt32(scalar, CultureInfo.InvariantCulture);
    }

    private static bool IsInternalTable(string tableName)
        => string.Equals(tableName, ProcedureTableName, StringComparison.OrdinalIgnoreCase);

    private async Task EnsureProcedureCatalogAsync()
    {
        await ExecuteNonQueryAsync($"""
            CREATE TABLE IF NOT EXISTS {ProcedureTableName} (
                name TEXT PRIMARY KEY,
                body_sql TEXT NOT NULL,
                params_json TEXT NOT NULL,
                description TEXT,
                is_enabled INTEGER NOT NULL,
                created_utc TEXT NOT NULL,
                updated_utc TEXT NOT NULL
            );
            """);

        await ExecuteNonQueryAsync($"""
            CREATE INDEX IF NOT EXISTS {ProcedureEnabledIndexName}
            ON {ProcedureTableName} (is_enabled);
            """);
    }

    private async Task<ProcedureDefinition?> GetProcedureInternalAsync(string name)
    {
        using var cmd = (CSharpDbCommand)_connection.CreateCommand();
        cmd.CommandText = $"""
            SELECT name, body_sql, params_json, description, is_enabled, created_utc, updated_utc
            FROM {ProcedureTableName}
            WHERE name = @name;
            """;
        cmd.Parameters.AddWithValue("@name", name);

        await using var reader = await cmd.ExecuteReaderAsync();
        if (!await reader.ReadAsync())
            return null;

        return ReadProcedureDefinition(reader);
    }

    private static ProcedureDefinition ReadProcedureDefinition(DbDataReader reader)
    {
        string name = reader.GetString(0);
        string bodySql = reader.GetString(1);
        string paramsJson = reader.GetString(2);
        string? description = reader.IsDBNull(3) ? null : reader.GetString(3);
        bool isEnabled = !reader.IsDBNull(4) && Convert.ToInt64(reader.GetValue(4), CultureInfo.InvariantCulture) != 0;
        string createdRaw = reader.GetString(5);
        string updatedRaw = reader.GetString(6);

        if (!DateTime.TryParse(
                createdRaw,
                CultureInfo.InvariantCulture,
                DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal,
                out var createdUtc))
        {
            createdUtc = DateTime.UtcNow;
        }

        if (!DateTime.TryParse(
                updatedRaw,
                CultureInfo.InvariantCulture,
                DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal,
                out var updatedUtc))
        {
            updatedUtc = createdUtc;
        }

        return new ProcedureDefinition
        {
            Name = name,
            BodySql = bodySql,
            Parameters = DeserializeProcedureParameters(paramsJson),
            Description = description,
            IsEnabled = isEnabled,
            CreatedUtc = createdUtc,
            UpdatedUtc = updatedUtc,
        };
    }

    private ProcedureDefinition NormalizeProcedureDefinition(
        ProcedureDefinition definition,
        DateTime defaultCreatedUtc)
    {
        ValidateIdentifier(definition.Name, "procedure name");

        string normalizedBody = NormalizeSqlFragment(definition.BodySql, "procedure body");
        var normalizedParameters = NormalizeProcedureParameters(definition.Parameters);
        ValidateProcedureBodyReferences(normalizedBody, normalizedParameters);

        return new ProcedureDefinition
        {
            Name = definition.Name.Trim(),
            BodySql = normalizedBody,
            Parameters = normalizedParameters,
            Description = string.IsNullOrWhiteSpace(definition.Description) ? null : definition.Description.Trim(),
            IsEnabled = definition.IsEnabled,
            CreatedUtc = defaultCreatedUtc,
            UpdatedUtc = DateTime.UtcNow,
        };
    }

    private static IReadOnlyList<ProcedureParameterDefinition> NormalizeProcedureParameters(
        IReadOnlyList<ProcedureParameterDefinition> parameters)
    {
        ArgumentNullException.ThrowIfNull(parameters);

        var normalized = new List<ProcedureParameterDefinition>(parameters.Count);
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var parameter in parameters)
        {
            if (parameter is null)
                throw new ArgumentException("Procedure parameter entry cannot be null.");

            string name = NormalizeProcedureParameterName(parameter.Name);
            if (!seen.Add(name))
                throw new ArgumentException($"Duplicate parameter '{name}' in procedure definition.");

            object? normalizedDefault = parameter.Default is JsonElement je
                ? ConvertJsonElement(je)
                : parameter.Default;

            normalized.Add(new ProcedureParameterDefinition
            {
                Name = name,
                Type = parameter.Type,
                Required = parameter.Required,
                Default = normalizedDefault,
                Description = string.IsNullOrWhiteSpace(parameter.Description) ? null : parameter.Description.Trim(),
            });
        }

        return normalized;
    }

    private static void ValidateProcedureBodyReferences(
        string bodySql,
        IReadOnlyList<ProcedureParameterDefinition> parameters)
    {
        var defined = new HashSet<string>(parameters.Select(p => p.Name), StringComparer.OrdinalIgnoreCase);
        foreach (string bodyParameter in ExtractParameterNamesFromSql(bodySql))
        {
            if (!defined.Contains(bodyParameter))
            {
                throw new ArgumentException(
                    $"Procedure SQL references parameter '@{bodyParameter}' but it is missing from params metadata.");
            }
        }
    }

    private static Dictionary<string, object?> BindProcedureArguments(
        ProcedureDefinition procedure,
        IReadOnlyDictionary<string, object?> args)
    {
        var incoming = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        foreach (var (rawName, rawValue) in args)
        {
            string normalized = NormalizeProcedureParameterName(rawName);
            incoming[normalized] = rawValue is JsonElement je ? ConvertJsonElement(je) : rawValue;
        }

        var known = new HashSet<string>(procedure.Parameters.Select(p => p.Name), StringComparer.OrdinalIgnoreCase);
        foreach (string provided in incoming.Keys)
        {
            if (!known.Contains(provided))
                throw new ArgumentException($"Unknown argument '{provided}'.");
        }

        var bound = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        foreach (var parameter in procedure.Parameters)
        {
            if (incoming.TryGetValue(parameter.Name, out var providedValue))
            {
                if (providedValue is null)
                {
                    if (parameter.Default is not null)
                    {
                        bound[parameter.Name] = CoerceProcedureParameterValue(parameter.Name, parameter.Type, parameter.Default);
                        continue;
                    }

                    if (parameter.Required)
                        throw new ArgumentException($"Required parameter '{parameter.Name}' cannot be null.");

                    bound[parameter.Name] = null;
                    continue;
                }

                bound[parameter.Name] = CoerceProcedureParameterValue(parameter.Name, parameter.Type, providedValue);
                continue;
            }

            if (parameter.Default is not null)
            {
                bound[parameter.Name] = CoerceProcedureParameterValue(parameter.Name, parameter.Type, parameter.Default);
                continue;
            }

            if (parameter.Required)
                throw new ArgumentException($"Missing required parameter '{parameter.Name}'.");

            bound[parameter.Name] = null;
        }

        return bound;
    }

    private async Task<ProcedureStatementExecutionResult> ExecuteSingleStatementWithArgumentsAsync(
        int statementIndex,
        string sql,
        IReadOnlyDictionary<string, object?> args)
    {
        var sw = Stopwatch.StartNew();
        using var cmd = (CSharpDbCommand)_connection.CreateCommand();
        cmd.CommandText = sql;

        foreach (string parameterName in ExtractParameterNamesFromSql(sql))
        {
            if (!args.TryGetValue(parameterName, out object? value))
                throw new ArgumentException($"Missing argument for SQL parameter '@{parameterName}'.");

            cmd.Parameters.AddWithValue($"@{parameterName}", value ?? DBNull.Value);
        }

        await using var reader = await cmd.ExecuteReaderAsync();

        if (reader.FieldCount > 0)
        {
            var colNames = new string[reader.FieldCount];
            for (int i = 0; i < reader.FieldCount; i++)
                colNames[i] = reader.GetName(i);

            var rows = new List<object?[]>();
            while (await reader.ReadAsync())
            {
                var row = new object?[reader.FieldCount];
                for (int i = 0; i < reader.FieldCount; i++)
                    row[i] = reader.IsDBNull(i) ? null : reader.GetValue(i);
                rows.Add(row);
            }

            sw.Stop();
            return new ProcedureStatementExecutionResult
            {
                StatementIndex = statementIndex,
                StatementText = sql,
                IsQuery = true,
                ColumnNames = colNames,
                Rows = rows,
                RowsAffected = rows.Count,
                Elapsed = sw.Elapsed,
            };
        }

        sw.Stop();
        return new ProcedureStatementExecutionResult
        {
            StatementIndex = statementIndex,
            StatementText = sql,
            IsQuery = false,
            RowsAffected = reader.RecordsAffected,
            Elapsed = sw.Elapsed,
        };
    }

    private static string SerializeProcedureParameters(IReadOnlyList<ProcedureParameterDefinition> parameters)
    {
        var storage = parameters.Select(p => new ProcedureParameterStorage
        {
            Name = p.Name,
            Type = p.Type.ToString().ToUpperInvariant(),
            Required = p.Required,
            Default = p.Type == DbType.Blob && p.Default is byte[] bytes
                ? Convert.ToBase64String(bytes)
                : p.Default,
            Description = p.Description,
        });

        return JsonSerializer.Serialize(storage, s_jsonOptions);
    }

    private static IReadOnlyList<ProcedureParameterDefinition> DeserializeProcedureParameters(string json)
    {
        if (string.IsNullOrWhiteSpace(json))
            return [];

        List<ProcedureParameterStorage>? storage;
        try
        {
            storage = JsonSerializer.Deserialize<List<ProcedureParameterStorage>>(json, s_jsonOptions);
        }
        catch (JsonException ex)
        {
            throw new ArgumentException($"Invalid procedure params_json payload: {ex.Message}");
        }

        if (storage is null || storage.Count == 0)
            return [];

        var result = new List<ProcedureParameterDefinition>(storage.Count);
        foreach (var item in storage)
        {
            if (item is null)
                continue;

            string name = NormalizeProcedureParameterName(item.Name ?? string.Empty);
            if (!Enum.TryParse<DbType>(item.Type, ignoreCase: true, out var type))
                throw new ArgumentException($"Unsupported parameter type '{item.Type}' in params_json.");

            object? defaultValue = item.Default is JsonElement je
                ? ConvertJsonElement(je)
                : item.Default;

            result.Add(new ProcedureParameterDefinition
            {
                Name = name,
                Type = type,
                Required = item.Required,
                Default = defaultValue,
                Description = string.IsNullOrWhiteSpace(item.Description) ? null : item.Description.Trim(),
            });
        }

        return NormalizeProcedureParameters(result);
    }

    private static string NormalizeProcedureParameterName(string rawName)
    {
        if (string.IsNullOrWhiteSpace(rawName))
            throw new ArgumentException("Procedure parameter name is required.");

        string trimmed = rawName.Trim();
        if (trimmed.StartsWith('@'))
            trimmed = trimmed[1..];

        ValidateIdentifier(trimmed, "procedure parameter name");
        return trimmed;
    }

    private static HashSet<string> ExtractParameterNamesFromSql(string sql)
    {
        var names = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var token in new Tokenizer(sql).Tokenize())
        {
            if (token.Type == TokenType.Parameter)
                names.Add(token.Value);
        }
        return names;
    }

    private static object? ConvertJsonElement(JsonElement element) => element.ValueKind switch
    {
        JsonValueKind.Null => null,
        JsonValueKind.String => element.GetString(),
        JsonValueKind.Number => element.TryGetInt64(out long l) ? l : element.GetDouble(),
        JsonValueKind.True => true,
        JsonValueKind.False => false,
        _ => element.ToString(),
    };

    private static object? CoerceProcedureParameterValue(string name, DbType type, object? value)
    {
        if (value is null)
            return null;

        if (value is JsonElement element)
            value = ConvertJsonElement(element);
        if (value is null)
            return null;

        switch (type)
        {
            case DbType.Integer:
                if (TryCoerceInteger(value, out long integerValue))
                    return integerValue;
                throw new ArgumentException($"Parameter '{name}' expects INTEGER.");

            case DbType.Real:
                if (TryCoerceReal(value, out double realValue))
                    return realValue;
                throw new ArgumentException($"Parameter '{name}' expects REAL.");

            case DbType.Text:
                if (value is string textValue)
                    return textValue;
                throw new ArgumentException($"Parameter '{name}' expects TEXT.");

            case DbType.Blob:
                if (value is byte[] blob)
                    return blob;
                if (value is string b64)
                {
                    try
                    {
                        return Convert.FromBase64String(b64);
                    }
                    catch (FormatException)
                    {
                        throw new ArgumentException($"Parameter '{name}' expects BLOB as base64 string.");
                    }
                }
                throw new ArgumentException($"Parameter '{name}' expects BLOB.");

            default:
                throw new ArgumentException($"Unsupported parameter type '{type}' for '{name}'.");
        }
    }

    private static bool TryCoerceInteger(object value, out long result)
    {
        switch (value)
        {
            case long l:
                result = l;
                return true;
            case int i:
                result = i;
                return true;
            case short s:
                result = s;
                return true;
            case byte b:
                result = b;
                return true;
            case sbyte sb:
                result = sb;
                return true;
            case uint ui:
                result = ui;
                return true;
            case ulong ul when ul <= long.MaxValue:
                result = (long)ul;
                return true;
            case double d when IsWholeNumberInRange(d):
                result = (long)d;
                return true;
            case float f when IsWholeNumberInRange(f):
                result = (long)f;
                return true;
            case decimal m when m >= long.MinValue && m <= long.MaxValue && decimal.Truncate(m) == m:
                result = (long)m;
                return true;
            case string text when long.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out long parsed):
                result = parsed;
                return true;
            default:
                result = 0;
                return false;
        }
    }

    private static bool IsWholeNumberInRange(double value)
        => !double.IsNaN(value)
           && !double.IsInfinity(value)
           && value >= long.MinValue
           && value <= long.MaxValue
           && Math.Truncate(value) == value;

    private static bool IsWholeNumberInRange(float value)
        => !float.IsNaN(value)
           && !float.IsInfinity(value)
           && value >= long.MinValue
           && value <= long.MaxValue
           && MathF.Truncate(value) == value;

    private static bool TryCoerceReal(object value, out double result)
    {
        switch (value)
        {
            case double d:
                result = d;
                return true;
            case float f:
                result = f;
                return true;
            case decimal m:
                result = (double)m;
                return true;
            case long l:
                result = l;
                return true;
            case int i:
                result = i;
                return true;
            case short s:
                result = s;
                return true;
            case byte b:
                result = b;
                return true;
            case sbyte sb:
                result = sb;
                return true;
            case uint ui:
                result = ui;
                return true;
            case ulong ul:
                result = ul;
                return true;
            case string text when double.TryParse(text, NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture, out double parsed):
                result = parsed;
                return true;
            default:
                result = 0;
                return false;
        }
    }

    private void ValidateTableName(string tableName)
    {
        if (IsInternalTable(tableName))
            throw new ArgumentException($"Table '{tableName}' is internal and not available through table endpoints.");

        var known = _connection.GetTableNames();
        if (!known.Any(t => string.Equals(t, tableName, StringComparison.OrdinalIgnoreCase)))
            throw new ArgumentException($"Table '{tableName}' does not exist.");
    }

    private void ValidateColumnName(string tableName, string columnName)
    {
        var schema = _connection.GetTableSchema(tableName);
        if (schema == null || schema.GetColumnIndex(columnName) < 0)
            throw new ArgumentException($"Column '{columnName}' not found in table '{tableName}'.");
    }

    private void ValidateViewName(string viewName)
    {
        var known = _connection.GetViewNames();
        if (!known.Any(v => string.Equals(v, viewName, StringComparison.OrdinalIgnoreCase)))
            throw new ArgumentException($"View '{viewName}' does not exist.");
    }

    private static void ValidateIdentifier(string value, string label)
    {
        if (string.IsNullOrWhiteSpace(value))
            throw new ArgumentException($"{label} is required.");

        string trimmed = value.Trim();
        if (!IsIdentifierStart(trimmed[0]))
            throw new ArgumentException($"{label} must start with a letter or underscore.");

        for (int i = 1; i < trimmed.Length; i++)
        {
            if (!IsIdentifierPart(trimmed[i]))
                throw new ArgumentException($"{label} can only contain letters, digits, and underscore.");
        }
    }

    private static string NormalizeSqlFragment(string sql, string label)
    {
        if (string.IsNullOrWhiteSpace(sql))
            throw new ArgumentException($"{label} is required.");

        string trimmed = sql.Trim();
        if (trimmed.EndsWith(';'))
            trimmed = trimmed.TrimEnd(';').TrimEnd();
        if (trimmed.Length == 0)
            throw new ArgumentException($"{label} is required.");
        return trimmed;
    }

    private static bool IsIdentifierStart(char c) => char.IsLetter(c) || c == '_';
    private static bool IsIdentifierPart(char c) => char.IsLetterOrDigit(c) || c == '_';

    private async Task ExecuteNonQueryAsync(string sql)
    {
        using var cmd = (CSharpDbCommand)_connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task<SqlExecutionResult> ExecuteSingleStatementAsync(string sql)
    {
        using var cmd = (CSharpDbCommand)_connection.CreateCommand();
        cmd.CommandText = sql;

        await using var reader = await cmd.ExecuteReaderAsync();
        if (reader.FieldCount > 0)
        {
            var colNames = new string[reader.FieldCount];
            for (int i = 0; i < reader.FieldCount; i++)
                colNames[i] = reader.GetName(i);

            var rows = new List<object?[]>();
            while (await reader.ReadAsync())
            {
                var row = new object?[reader.FieldCount];
                for (int i = 0; i < reader.FieldCount; i++)
                    row[i] = reader.IsDBNull(i) ? null : reader.GetValue(i);
                rows.Add(row);
            }

            return new SqlExecutionResult
            {
                IsQuery = true,
                ColumnNames = colNames,
                Rows = rows,
                RowsAffected = rows.Count,
            };
        }

        return new SqlExecutionResult
        {
            IsQuery = false,
            RowsAffected = reader.RecordsAffected,
        };
    }

    private static IReadOnlyList<string> SplitSqlStatements(string sql)
    {
        if (string.IsNullOrWhiteSpace(sql))
            return Array.Empty<string>();

        var statements = new List<string>();
        var tokens = new Tokenizer(sql).Tokenize();

        int statementStart = 0;
        bool atStatementStart = true;
        bool createSeen = false;
        bool createTrigger = false;
        int triggerBeginDepth = 0;

        foreach (var token in tokens)
        {
            if (token.Type == TokenType.Eof)
                break;

            if (atStatementStart)
            {
                if (token.Type == TokenType.Semicolon)
                {
                    statementStart = token.Position + 1;
                    continue;
                }

                atStatementStart = false;
                createSeen = token.Type == TokenType.Create;
                createTrigger = false;
                triggerBeginDepth = 0;
            }
            else if (createSeen && !createTrigger && token.Type == TokenType.Trigger)
            {
                createTrigger = true;
            }

            if (createTrigger)
            {
                if (token.Type == TokenType.Begin)
                    triggerBeginDepth++;
                else if (token.Type == TokenType.End && triggerBeginDepth > 0)
                    triggerBeginDepth--;
            }

            if (token.Type == TokenType.Semicolon)
            {
                bool isStatementTerminator = !createTrigger || triggerBeginDepth == 0;
                if (!isStatementTerminator)
                    continue;

                int statementEnd = token.Position + 1;
                if (statementEnd > statementStart)
                {
                    string statement = sql[statementStart..statementEnd].Trim();
                    if (statement.Length > 0)
                        statements.Add(statement);
                }

                statementStart = statementEnd;
                atStatementStart = true;
                createSeen = false;
                createTrigger = false;
                triggerBeginDepth = 0;
            }
        }

        if (statementStart < sql.Length)
        {
            string remainder = sql[statementStart..];
            var remainderTokens = new Tokenizer(remainder).Tokenize();
            bool hasSql = remainderTokens.Any(t => t.Type is not (TokenType.Eof or TokenType.Semicolon));
            if (hasSql)
            {
                string trimmed = remainder.Trim();
                if (trimmed.Length > 0)
                    statements.Add(trimmed);
            }
        }

        return statements;
    }

    private static bool LooksLikeSchemaMutation(string sql)
    {
        var upper = sql.TrimStart().ToUpperInvariant();
        return upper.StartsWith("CREATE ", StringComparison.Ordinal)
            || upper.StartsWith("DROP ", StringComparison.Ordinal)
            || upper.StartsWith("ALTER ", StringComparison.Ordinal);
    }

    private static bool LooksLikeTableMutation(string sql)
    {
        var upper = sql.TrimStart().ToUpperInvariant();
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

    private sealed class ProcedureParameterStorage
    {
        public string? Name { get; init; }
        public string? Type { get; init; }
        public bool Required { get; init; }
        public object? Default { get; init; }
        public string? Description { get; init; }
    }

    private string ResolveDatabasePath(string? databasePath)
    {
        string path = string.IsNullOrWhiteSpace(databasePath)
            ? DataSource
            : databasePath.Trim();
        return Path.GetFullPath(path);
    }

    public async ValueTask DisposeAsync()
    {
        await _connection.DisposeAsync();
        _lock.Dispose();
    }
}
