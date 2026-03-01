using System.Data.Common;
using System.Diagnostics;
using System.Globalization;
using CSharpDB.Service.Models;
using CSharpDB.Core;
using CSharpDB.Data;
using CSharpDB.Sql;
using CSharpDB.Storage.Diagnostics;
using Microsoft.Extensions.Configuration;

namespace CSharpDB.Service;

public sealed class CSharpDbService : IAsyncDisposable
{
    private readonly CSharpDbConnection _connection;
    private readonly SemaphoreSlim _lock = new(1, 1);

    public CSharpDbService(IConfiguration configuration)
    {
        string connString = configuration.GetConnectionString("CSharpDB")
            ?? "Data Source=csharpdb.db";
        _connection = new CSharpDbConnection(connString);
    }

    public async Task InitializeAsync()
    {
        await _connection.OpenAsync();
    }

    public string DataSource => _connection.DataSource;

    /// <summary>Raised after tables are created or dropped so the sidebar can refresh.</summary>
    public event Action? TablesChanged;
    /// <summary>Raised after schema objects (tables, indexes, views, triggers) change.</summary>
    public event Action? SchemaChanged;

    private void NotifySchemaChanged(bool tablesMayHaveChanged)
    {
        SchemaChanged?.Invoke();
        if (tablesMayHaveChanged)
            TablesChanged?.Invoke();
    }

    // ─── Schema ────────────────────────────────────────────────────

    public async Task<IReadOnlyCollection<string>> GetTableNamesAsync()
    {
        await _lock.WaitAsync();
        try { return _connection.GetTableNames(); }
        finally { _lock.Release(); }
    }

    public async Task<TableSchema?> GetTableSchemaAsync(string tableName)
    {
        await _lock.WaitAsync();
        try { return _connection.GetTableSchema(tableName); }
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
                }
                catch (DbException ex)
                {
                    sw.Stop();

                    if (schemaMutated)
                        NotifySchemaChanged(tablesMayHaveChanged: tableMutated);

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

    private void ValidateTableName(string tableName)
    {
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
