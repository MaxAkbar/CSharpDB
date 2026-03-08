using System.Collections.Concurrent;
using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using CSharpDB.Client.Models;
using CSharpDB.Engine;
using CoreColumnDefinition = CSharpDB.Core.ColumnDefinition;
using CoreDbType = CSharpDB.Core.DbType;
using CoreIndexSchema = CSharpDB.Core.IndexSchema;
using CoreTableSchema = CSharpDB.Core.TableSchema;
using CoreTriggerEvent = CSharpDB.Core.TriggerEvent;
using CoreTriggerSchema = CSharpDB.Core.TriggerSchema;
using CoreTriggerTiming = CSharpDB.Core.TriggerTiming;

namespace CSharpDB.Client.Internal;

internal sealed partial class EngineTransportClient : ICSharpDbClient, IEngineBackedClient
{
    private const string CollectionPrefix = "_col_";
    private const string ProcedureTableName = "__procedures";
    private const string SavedQueryTableName = "__saved_queries";
    private static readonly Regex s_identifierPattern = new("^[A-Za-z_][A-Za-z0-9_]*$", RegexOptions.Compiled);

    private readonly string _databasePath;
    private readonly SemaphoreSlim _lock = new(1, 1);
    private readonly object _databaseGate = new();
    private readonly ConcurrentDictionary<string, Database> _transactions = new(StringComparer.Ordinal);
    private Task<Database>? _databaseTask;
    private bool _catalogsInitialized;

    public EngineTransportClient(string databasePath)
    {
        _databasePath = Path.GetFullPath(databasePath);
    }

    public string DataSource => _databasePath;

    public Task<DatabaseInfo> GetInfoAsync(CancellationToken ct = default)
        => GetInfoCoreAsync(ct);

    public async Task<IReadOnlyList<string>> GetTableNamesAsync(CancellationToken ct = default)
    {
        var db = await GetDatabaseAsync(ct);
        return db.GetTableNames()
            .Where(name => !IsInternalTable(name))
            .OrderBy(name => name, StringComparer.OrdinalIgnoreCase)
            .ToArray();
    }

    public async Task<TableSchema?> GetTableSchemaAsync(string tableName, CancellationToken ct = default)
    {
        var db = await GetDatabaseAsync(ct);
        if (IsInternalTable(tableName))
            return null;

        var schema = db.GetTableSchema(RequireIdentifier(tableName, nameof(tableName)));
        return schema is null ? null : MapTableSchema(schema);
    }

    public async Task<int> GetRowCountAsync(string tableName, CancellationToken ct = default)
    {
        var browse = await BrowseTableInternalAsync(await GetDatabaseAsync(ct), tableName, ct);
        return browse.Rows.Count;
    }

    public async Task<TableBrowseResult> BrowseTableAsync(string tableName, int page = 1, int pageSize = 50, CancellationToken ct = default)
    {
        var full = await BrowseTableInternalAsync(await GetDatabaseAsync(ct), tableName, ct);
        return PageTableResult(full, page, pageSize);
    }

    public async Task<Dictionary<string, object?>?> GetRowByPkAsync(string tableName, string pkColumn, object pkValue, CancellationToken ct = default)
    {
        var db = await GetDatabaseAsync(ct);
        string normalizedTableName = RequireIdentifier(tableName, nameof(tableName));
        string normalizedPkColumn = RequireIdentifier(pkColumn, nameof(pkColumn));

        await using var result = await db.ExecuteAsync($"SELECT * FROM {normalizedTableName}", ct);
        int pkIndex = Array.FindIndex(result.Schema, column => string.Equals(column.Name, normalizedPkColumn, StringComparison.OrdinalIgnoreCase));
        if (pkIndex < 0)
            throw new CSharpDbClientException($"Column '{normalizedPkColumn}' was not found in table '{normalizedTableName}'.");

        while (await result.MoveNextAsync(ct))
        {
            var row = result.Current;
            if (ValuesEqual(row[pkIndex], pkValue))
                return ToRowDictionary(result.Schema, row);
        }

        return null;
    }

    public async Task<int> InsertRowAsync(string tableName, Dictionary<string, object?> values, CancellationToken ct = default)
    {
        if (values.Count == 0)
            throw new CSharpDbClientException("Insert requires at least one value.");

        string normalizedTableName = RequireIdentifier(tableName, nameof(tableName));
        var assignments = values.Select(kvp => new KeyValuePair<string, object?>(RequireIdentifier(kvp.Key, nameof(values)), kvp.Value)).ToArray();
        string columns = string.Join(", ", assignments.Select(item => item.Key));
        string literals = string.Join(", ", assignments.Select(item => FormatSqlLiteral(item.Value)));
        return await ExecuteNonQueryAsync(await GetDatabaseAsync(ct), $"INSERT INTO {normalizedTableName} ({columns}) VALUES ({literals})", ct);
    }

    public async Task<int> UpdateRowAsync(string tableName, string pkColumn, object pkValue, Dictionary<string, object?> values, CancellationToken ct = default)
    {
        if (values.Count == 0)
            return 0;

        string normalizedTableName = RequireIdentifier(tableName, nameof(tableName));
        string normalizedPkColumn = RequireIdentifier(pkColumn, nameof(pkColumn));
        string setClause = string.Join(", ", values.Select(kvp => $"{RequireIdentifier(kvp.Key, nameof(values))} = {FormatSqlLiteral(kvp.Value)}"));
        string sql = $"UPDATE {normalizedTableName} SET {setClause} WHERE {normalizedPkColumn} = {FormatSqlLiteral(pkValue)}";
        return await ExecuteNonQueryAsync(await GetDatabaseAsync(ct), sql, ct);
    }

    public async Task<int> DeleteRowAsync(string tableName, string pkColumn, object pkValue, CancellationToken ct = default)
    {
        string normalizedTableName = RequireIdentifier(tableName, nameof(tableName));
        string normalizedPkColumn = RequireIdentifier(pkColumn, nameof(pkColumn));
        string sql = $"DELETE FROM {normalizedTableName} WHERE {normalizedPkColumn} = {FormatSqlLiteral(pkValue)}";
        return await ExecuteNonQueryAsync(await GetDatabaseAsync(ct), sql, ct);
    }

    public async Task DropTableAsync(string tableName, CancellationToken ct = default)
        => await ExecuteStatementAsync(await GetDatabaseAsync(ct), $"DROP TABLE {RequireIdentifier(tableName, nameof(tableName))}", ct);

    public async Task RenameTableAsync(string tableName, string newTableName, CancellationToken ct = default)
        => await ExecuteStatementAsync(await GetDatabaseAsync(ct), $"ALTER TABLE {RequireIdentifier(tableName, nameof(tableName))} RENAME TO {RequireIdentifier(newTableName, nameof(newTableName))}", ct);

    public async Task AddColumnAsync(string tableName, string columnName, Models.DbType type, bool notNull, CancellationToken ct = default)
    {
        string sql = $"ALTER TABLE {RequireIdentifier(tableName, nameof(tableName))} ADD COLUMN {RequireIdentifier(columnName, nameof(columnName))} {MapDbType(type)}";
        if (notNull)
            sql += " NOT NULL";

        await ExecuteStatementAsync(await GetDatabaseAsync(ct), sql, ct);
    }

    public async Task DropColumnAsync(string tableName, string columnName, CancellationToken ct = default)
        => await ExecuteStatementAsync(await GetDatabaseAsync(ct), $"ALTER TABLE {RequireIdentifier(tableName, nameof(tableName))} DROP COLUMN {RequireIdentifier(columnName, nameof(columnName))}", ct);

    public async Task RenameColumnAsync(string tableName, string oldColumnName, string newColumnName, CancellationToken ct = default)
        => await ExecuteStatementAsync(await GetDatabaseAsync(ct), $"ALTER TABLE {RequireIdentifier(tableName, nameof(tableName))} RENAME COLUMN {RequireIdentifier(oldColumnName, nameof(oldColumnName))} TO {RequireIdentifier(newColumnName, nameof(newColumnName))}", ct);

    public async Task<IReadOnlyList<IndexSchema>> GetIndexesAsync(CancellationToken ct = default)
    {
        var db = await GetDatabaseAsync(ct);
        return db.GetIndexes()
            .Select(MapIndexSchema)
            .OrderBy(index => index.IndexName, StringComparer.OrdinalIgnoreCase)
            .ToArray();
    }

    public async Task CreateIndexAsync(string indexName, string tableName, string columnName, bool isUnique, CancellationToken ct = default)
    {
        string unique = isUnique ? "UNIQUE " : string.Empty;
        string sql = $"CREATE {unique}INDEX {RequireIdentifier(indexName, nameof(indexName))} ON {RequireIdentifier(tableName, nameof(tableName))} ({RequireIdentifier(columnName, nameof(columnName))})";
        await ExecuteStatementAsync(await GetDatabaseAsync(ct), sql, ct);
    }

    public async Task UpdateIndexAsync(string existingIndexName, string newIndexName, string tableName, string columnName, bool isUnique, CancellationToken ct = default)
    {
        var db = await GetDatabaseAsync(ct);
        await ExecuteInSingleTransactionAsync(db, ct,
            $"DROP INDEX {RequireIdentifier(existingIndexName, nameof(existingIndexName))}",
            BuildCreateIndexSql(newIndexName, tableName, columnName, isUnique));
    }

    public async Task DropIndexAsync(string indexName, CancellationToken ct = default)
        => await ExecuteStatementAsync(await GetDatabaseAsync(ct), $"DROP INDEX {RequireIdentifier(indexName, nameof(indexName))}", ct);

    public async Task<IReadOnlyList<ViewDefinition>> GetViewsAsync(CancellationToken ct = default)
    {
        var db = await GetDatabaseAsync(ct);
        return db.GetViewNames()
            .OrderBy(name => name, StringComparer.OrdinalIgnoreCase)
            .Select(name => new ViewDefinition
            {
                Name = name,
                Sql = db.GetViewSql(name) ?? string.Empty,
            })
            .ToArray();
    }

    public async Task<ViewDefinition?> GetViewAsync(string viewName, CancellationToken ct = default)
    {
        var db = await GetDatabaseAsync(ct);
        string normalizedViewName = RequireIdentifier(viewName, nameof(viewName));
        string? sql = db.GetViewSql(normalizedViewName);
        return sql is null ? null : new ViewDefinition { Name = normalizedViewName, Sql = sql };
    }

    public async Task<ViewBrowseResult> BrowseViewAsync(string viewName, int page = 1, int pageSize = 50, CancellationToken ct = default)
    {
        var db = await GetDatabaseAsync(ct);
        string normalizedViewName = RequireIdentifier(viewName, nameof(viewName));
        var result = await ExecuteQueryAsync(db, $"SELECT * FROM {normalizedViewName}", ct);
        return PageViewResult(
            new ViewBrowseResult
            {
                ViewName = normalizedViewName,
                ColumnNames = result.ColumnNames ?? [],
                Rows = result.Rows ?? [],
                TotalRows = result.Rows?.Count ?? 0,
                Page = 1,
                PageSize = Math.Max(result.Rows?.Count ?? 0, 1),
            },
            page,
            pageSize);
    }

    public async Task CreateViewAsync(string viewName, string selectSql, CancellationToken ct = default)
        => await ExecuteStatementAsync(await GetDatabaseAsync(ct), $"CREATE VIEW {RequireIdentifier(viewName, nameof(viewName))} AS {NormalizeEmbeddedSql(selectSql)}", ct);

    public async Task UpdateViewAsync(string existingViewName, string newViewName, string selectSql, CancellationToken ct = default)
    {
        var db = await GetDatabaseAsync(ct);
        await ExecuteInSingleTransactionAsync(db, ct,
            $"DROP VIEW {RequireIdentifier(existingViewName, nameof(existingViewName))}",
            $"CREATE VIEW {RequireIdentifier(newViewName, nameof(newViewName))} AS {NormalizeEmbeddedSql(selectSql)}");
    }

    public async Task DropViewAsync(string viewName, CancellationToken ct = default)
        => await ExecuteStatementAsync(await GetDatabaseAsync(ct), $"DROP VIEW {RequireIdentifier(viewName, nameof(viewName))}", ct);

    public async Task<IReadOnlyList<TriggerSchema>> GetTriggersAsync(CancellationToken ct = default)
    {
        var db = await GetDatabaseAsync(ct);
        return db.GetTriggers()
            .Select(MapTriggerSchema)
            .OrderBy(trigger => trigger.TriggerName, StringComparer.OrdinalIgnoreCase)
            .ToArray();
    }

    public async Task CreateTriggerAsync(string triggerName, string tableName, TriggerTiming timing, TriggerEvent triggerEvent, string bodySql, CancellationToken ct = default)
        => await ExecuteStatementAsync(await GetDatabaseAsync(ct), BuildCreateTriggerSql(triggerName, tableName, timing, triggerEvent, bodySql), ct);

    public async Task UpdateTriggerAsync(string existingTriggerName, string newTriggerName, string tableName, TriggerTiming timing, TriggerEvent triggerEvent, string bodySql, CancellationToken ct = default)
    {
        var db = await GetDatabaseAsync(ct);
        await ExecuteInSingleTransactionAsync(db, ct,
            $"DROP TRIGGER {RequireIdentifier(existingTriggerName, nameof(existingTriggerName))}",
            BuildCreateTriggerSql(newTriggerName, tableName, timing, triggerEvent, bodySql));
    }

    public async Task DropTriggerAsync(string triggerName, CancellationToken ct = default)
        => await ExecuteStatementAsync(await GetDatabaseAsync(ct), $"DROP TRIGGER {RequireIdentifier(triggerName, nameof(triggerName))}", ct);

    public Task<SqlExecutionResult> ExecuteSqlAsync(string sql, CancellationToken ct = default)
        => ExecuteSqlCoreAsync(sql, ct);

    public async Task<TransactionSessionInfo> BeginTransactionAsync(CancellationToken ct = default)
    {
        var database = await Database.OpenAsync(_databasePath, ct);
        try
        {
            await database.BeginTransactionAsync(ct);
        }
        catch
        {
            await database.DisposeAsync();
            throw;
        }

        string transactionId = Guid.NewGuid().ToString("N", CultureInfo.InvariantCulture);
        if (!_transactions.TryAdd(transactionId, database))
        {
            await database.RollbackAsync(ct);
            await database.DisposeAsync();
            throw new CSharpDbClientException("Failed to register the transaction session.");
        }

        return new TransactionSessionInfo
        {
            TransactionId = transactionId,
            ExpiresAtUtc = DateTime.MaxValue,
        };
    }

    public async Task<SqlExecutionResult> ExecuteInTransactionAsync(string transactionId, string sql, CancellationToken ct = default)
        => await ExecuteQueryAsync(GetTransactionDatabase(transactionId), sql, ct);

    public async Task CommitTransactionAsync(string transactionId, CancellationToken ct = default)
    {
        var db = TakeTransactionDatabase(transactionId);
        try
        {
            await db.CommitAsync(ct);
        }
        finally
        {
            await db.DisposeAsync();
        }
    }

    public async Task RollbackTransactionAsync(string transactionId, CancellationToken ct = default)
    {
        var db = TakeTransactionDatabase(transactionId);
        try
        {
            await db.RollbackAsync(ct);
        }
        finally
        {
            await db.DisposeAsync();
        }
    }

    public async Task<IReadOnlyList<string>> GetCollectionNamesAsync(CancellationToken ct = default)
    {
        var db = await GetDatabaseAsync(ct);
        return db.GetCollectionNames()
            .OrderBy(name => name, StringComparer.OrdinalIgnoreCase)
            .ToArray();
    }

    public async Task<int> GetCollectionCountAsync(string collectionName, CancellationToken ct = default)
    {
        var collection = await (await GetDatabaseAsync(ct)).GetCollectionAsync<JsonElement>(RequireIdentifier(collectionName, nameof(collectionName)), ct);
        return checked((int)await collection.CountAsync(ct));
    }

    public async Task<CollectionBrowseResult> BrowseCollectionAsync(string collectionName, int page = 1, int pageSize = 50, CancellationToken ct = default)
    {
        string normalizedName = RequireIdentifier(collectionName, nameof(collectionName));
        var collection = await (await GetDatabaseAsync(ct)).GetCollectionAsync<JsonElement>(normalizedName, ct);
        var documents = new List<CollectionDocument>();
        await foreach (var item in collection.ScanAsync(ct))
        {
            documents.Add(new CollectionDocument
            {
                Key = item.Key,
                Document = item.Value,
            });
        }

        documents.Sort((left, right) => string.Compare(left.Key, right.Key, StringComparison.OrdinalIgnoreCase));
        int normalizedPage = NormalizePage(page);
        int normalizedPageSize = NormalizePageSize(pageSize);
        int skip = (normalizedPage - 1) * normalizedPageSize;

        return new CollectionBrowseResult
        {
            CollectionName = normalizedName,
            Documents = documents.Skip(skip).Take(normalizedPageSize).ToArray(),
            TotalCount = documents.Count,
            Page = normalizedPage,
            PageSize = normalizedPageSize,
        };
    }

    public async Task<JsonElement?> GetDocumentAsync(string collectionName, string key, CancellationToken ct = default)
    {
        var collection = await (await GetDatabaseAsync(ct)).GetCollectionAsync<JsonElement>(RequireIdentifier(collectionName, nameof(collectionName)), ct);
        var document = await collection.GetAsync(key, ct);
        return document.ValueKind == JsonValueKind.Undefined ? null : document;
    }

    public async Task PutDocumentAsync(string collectionName, string key, JsonElement document, CancellationToken ct = default)
    {
        var collection = await (await GetDatabaseAsync(ct)).GetCollectionAsync<JsonElement>(RequireIdentifier(collectionName, nameof(collectionName)), ct);
        await collection.PutAsync(key, document, ct);
    }

    public async Task<bool> DeleteDocumentAsync(string collectionName, string key, CancellationToken ct = default)
    {
        var collection = await (await GetDatabaseAsync(ct)).GetCollectionAsync<JsonElement>(RequireIdentifier(collectionName, nameof(collectionName)), ct);
        return await collection.DeleteAsync(key, ct);
    }

    public async Task CheckpointAsync(CancellationToken ct = default)
        => await (await GetDatabaseAsync(ct)).CheckpointAsync(ct);

    public async ValueTask DisposeAsync()
    {
        foreach (var pair in _transactions.ToArray())
        {
            if (_transactions.TryRemove(pair.Key, out var transactionDb))
                await transactionDb.DisposeAsync();
        }

        if (_databaseTask is null)
            return;

        try
        {
            var db = await _databaseTask;
            await db.DisposeAsync();
        }
        catch
        {
            // ignore lazy-init failures during dispose
        }
        _lock.Dispose();
    }

    private Task<Database> GetDatabaseAsync(CancellationToken ct)
    {
        Task<Database> openTask;
        lock (_databaseGate)
        {
            if (_databaseTask is null)
            {
                Task<Database>? createdTask = null;
                createdTask = OpenDatabaseCoreAsync();
                _databaseTask = createdTask;

                async Task<Database> OpenDatabaseCoreAsync()
                {
                    try
                    {
                        return await Database.OpenAsync(_databasePath, CancellationToken.None);
                    }
                    catch
                    {
                        lock (_databaseGate)
                        {
                            if (ReferenceEquals(_databaseTask, createdTask))
                                _databaseTask = null;
                        }

                        throw;
                    }
                }
            }

            openTask = _databaseTask;
        }

        return openTask.WaitAsync(ct);
    }

    public async ValueTask<Database?> TryGetDatabaseAsync(CancellationToken ct = default)
        => await GetDatabaseAsync(ct);

    private static TableSchema MapTableSchema(CoreTableSchema schema)
        => new()
        {
            TableName = schema.TableName,
            Columns = schema.Columns.Select(MapColumnDefinition).ToArray(),
        };

    private static ColumnDefinition MapColumnDefinition(CoreColumnDefinition column)
        => new()
        {
            Name = column.Name,
            Type = column.Type switch
            {
                CoreDbType.Integer => Models.DbType.Integer,
                CoreDbType.Real => Models.DbType.Real,
                CoreDbType.Text => Models.DbType.Text,
                CoreDbType.Blob => Models.DbType.Blob,
                _ => throw new CSharpDbClientException($"Unsupported column type '{column.Type}'."),
            },
            Nullable = column.Nullable,
            IsPrimaryKey = column.IsPrimaryKey,
            IsIdentity = column.IsIdentity,
        };

    private static IndexSchema MapIndexSchema(CoreIndexSchema index)
        => new()
        {
            IndexName = index.IndexName,
            TableName = index.TableName,
            Columns = index.Columns.ToArray(),
            IsUnique = index.IsUnique,
        };

    private static TriggerSchema MapTriggerSchema(CoreTriggerSchema trigger)
        => new()
        {
            TriggerName = trigger.TriggerName,
            TableName = trigger.TableName,
            Timing = trigger.Timing == CoreTriggerTiming.Before ? TriggerTiming.Before : TriggerTiming.After,
            Event = trigger.Event switch
            {
                CoreTriggerEvent.Insert => TriggerEvent.Insert,
                CoreTriggerEvent.Update => TriggerEvent.Update,
                CoreTriggerEvent.Delete => TriggerEvent.Delete,
                _ => throw new CSharpDbClientException($"Unsupported trigger event '{trigger.Event}'."),
            },
            BodySql = trigger.BodySql,
        };

    private static async Task<SqlExecutionResult> ExecuteQueryAsync(Database db, string sql, CancellationToken ct)
    {
        await using var result = await db.ExecuteAsync(sql, ct);
        var rows = await result.ToListAsync(ct);
        return new SqlExecutionResult
        {
            IsQuery = result.IsQuery,
            ColumnNames = result.IsQuery ? result.Schema.Select(column => column.Name).ToArray() : null,
            Rows = result.IsQuery ? rows.Select(ToObjects).ToList() : null,
            RowsAffected = result.IsQuery ? rows.Count : result.RowsAffected,
        };
    }

    private static async Task ExecuteStatementAsync(Database db, string sql, CancellationToken ct)
    {
        await using var result = await db.ExecuteAsync(sql, ct);
        if (result.IsQuery)
            await result.ToListAsync(ct);
    }

    private static async Task<int> ExecuteNonQueryAsync(Database db, string sql, CancellationToken ct)
    {
        await using var result = await db.ExecuteAsync(sql, ct);
        if (result.IsQuery)
            await result.ToListAsync(ct);
        return result.RowsAffected;
    }

    private static async Task ExecuteInSingleTransactionAsync(Database db, CancellationToken ct, params string[] statements)
    {
        await db.BeginTransactionAsync(ct);
        try
        {
            foreach (string statement in statements)
                await ExecuteStatementAsync(db, statement, ct);

            await db.CommitAsync(ct);
        }
        catch
        {
            await db.RollbackAsync(ct);
            throw;
        }
    }

    private static TableBrowseResult PageTableResult(TableBrowseResult result, int page, int pageSize)
    {
        int normalizedPage = NormalizePage(page);
        int normalizedPageSize = NormalizePageSize(pageSize);
        int skip = (normalizedPage - 1) * normalizedPageSize;

        return new TableBrowseResult
        {
            TableName = result.TableName,
            Schema = result.Schema,
            Rows = result.Rows.Skip(skip).Take(normalizedPageSize).ToList(),
            TotalRows = result.Rows.Count,
            Page = normalizedPage,
            PageSize = normalizedPageSize,
        };
    }

    private static ViewBrowseResult PageViewResult(ViewBrowseResult result, int page, int pageSize)
    {
        int normalizedPage = NormalizePage(page);
        int normalizedPageSize = NormalizePageSize(pageSize);
        int skip = (normalizedPage - 1) * normalizedPageSize;

        return new ViewBrowseResult
        {
            ViewName = result.ViewName,
            ColumnNames = result.ColumnNames,
            Rows = result.Rows.Skip(skip).Take(normalizedPageSize).ToList(),
            TotalRows = result.Rows.Count,
            Page = normalizedPage,
            PageSize = normalizedPageSize,
        };
    }

    private static async Task<TableBrowseResult> BrowseTableInternalAsync(Database db, string tableName, CancellationToken ct)
    {
        string normalizedTableName = RequireIdentifier(tableName, nameof(tableName));
        var schema = db.GetTableSchema(normalizedTableName);
        if (schema is null || IsInternalTable(normalizedTableName))
            throw new CSharpDbClientException($"Table '{normalizedTableName}' was not found.");

        var query = await ExecuteQueryAsync(db, $"SELECT * FROM {normalizedTableName}", ct);
        return new TableBrowseResult
        {
            TableName = normalizedTableName,
            Schema = MapTableSchema(schema),
            Rows = query.Rows ?? [],
            TotalRows = query.Rows?.Count ?? 0,
            Page = 1,
            PageSize = Math.Max(query.Rows?.Count ?? 0, 1),
        };
    }

    private static Dictionary<string, object?> ToRowDictionary(CoreColumnDefinition[] schema, CSharpDB.Core.DbValue[] row)
    {
        var values = new Dictionary<string, object?>(schema.Length, StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < schema.Length && i < row.Length; i++)
            values[schema[i].Name] = ToObject(row[i]);
        return values;
    }

    private static List<object?[]> ToObjects(List<CSharpDB.Core.DbValue[]> rows)
        => rows.Select(ToObjects).ToList();

    private static object?[] ToObjects(CSharpDB.Core.DbValue[] row)
    {
        var values = new object?[row.Length];
        for (int i = 0; i < row.Length; i++)
            values[i] = ToObject(row[i]);
        return values;
    }

    private static object? ToObject(CSharpDB.Core.DbValue value) => value.Type switch
    {
        CoreDbType.Null => null,
        CoreDbType.Integer => value.AsInteger,
        CoreDbType.Real => value.AsReal,
        CoreDbType.Text => value.AsText,
        CoreDbType.Blob => value.AsBlob,
        _ => throw new CSharpDbClientException($"Unsupported DbValue type '{value.Type}'."),
    };

    private static bool ValuesEqual(CSharpDB.Core.DbValue value, object candidate)
    {
        object? normalized = NormalizeValue(candidate);
        if (normalized is null)
            return value.Type == CoreDbType.Null;

        return value.Type switch
        {
            CoreDbType.Integer when normalized is long integer => value.AsInteger == integer,
            CoreDbType.Real when normalized is double real => Math.Abs(value.AsReal - real) < double.Epsilon,
            CoreDbType.Text when normalized is string text => string.Equals(value.AsText, text, StringComparison.Ordinal),
            CoreDbType.Blob when normalized is byte[] blob => value.AsBlob.AsSpan().SequenceEqual(blob),
            CoreDbType.Integer when normalized is double real => Math.Abs(value.AsReal - real) < double.Epsilon,
            CoreDbType.Real when normalized is long integer => Math.Abs(value.AsReal - integer) < double.Epsilon,
            _ => false,
        };
    }

    private static object? NormalizeValue(object? value) => value switch
    {
        null => null,
        JsonElement json => NormalizeJsonElement(json),
        bool boolean => boolean ? 1L : 0L,
        byte or sbyte or short or ushort or int or uint or long => Convert.ToInt64(value, CultureInfo.InvariantCulture),
        float or double or decimal => Convert.ToDouble(value, CultureInfo.InvariantCulture),
        string text => text,
        byte[] blob => blob,
        _ => Convert.ToString(value, CultureInfo.InvariantCulture),
    };

    private static object? NormalizeJsonElement(JsonElement value) => value.ValueKind switch
    {
        JsonValueKind.Null => null,
        JsonValueKind.String => value.GetString(),
        JsonValueKind.False => 0L,
        JsonValueKind.True => 1L,
        JsonValueKind.Number when value.TryGetInt64(out long integer) => integer,
        JsonValueKind.Number => value.GetDouble(),
        _ => value.GetRawText(),
    };

    private static string FormatSqlLiteral(object? value)
    {
        object? normalized = NormalizeValue(value);
        return normalized switch
        {
            null => "NULL",
            long integer => integer.ToString(CultureInfo.InvariantCulture),
            double real => real.ToString(CultureInfo.InvariantCulture),
            string text => $"'{text.Replace("'", "''", StringComparison.Ordinal)}'",
            byte[] => throw new CSharpDbClientException("BLOB literals are not supported by the engine-only client."),
            _ => $"'{Convert.ToString(normalized, CultureInfo.InvariantCulture)?.Replace("'", "''", StringComparison.Ordinal) ?? string.Empty}'",
        };
    }

    private static string RequireIdentifier(string value, string paramName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value, paramName);
        if (!s_identifierPattern.IsMatch(value))
            throw new CSharpDbClientException($"Identifier '{value}' is not supported by the engine-only client.");
        return value;
    }

    private static bool IsInternalTable(string tableName)
        => tableName.StartsWith(CollectionPrefix, StringComparison.Ordinal)
           || string.Equals(tableName, ProcedureTableName, StringComparison.OrdinalIgnoreCase)
           || string.Equals(tableName, SavedQueryTableName, StringComparison.OrdinalIgnoreCase);

    private static int NormalizePage(int page) => page < 1 ? 1 : page;

    private static int NormalizePageSize(int pageSize)
    {
        if (pageSize < 1)
            return 50;
        return Math.Min(pageSize, 1000);
    }

    private static string BuildCreateIndexSql(string indexName, string tableName, string columnName, bool isUnique)
    {
        string unique = isUnique ? "UNIQUE " : string.Empty;
        return $"CREATE {unique}INDEX {RequireIdentifier(indexName, nameof(indexName))} ON {RequireIdentifier(tableName, nameof(tableName))} ({RequireIdentifier(columnName, nameof(columnName))})";
    }

    private static string BuildCreateTriggerSql(string triggerName, string tableName, TriggerTiming timing, TriggerEvent triggerEvent, string bodySql)
    {
        string normalizedBody = NormalizeEmbeddedSql(bodySql);
        return $"CREATE TRIGGER {RequireIdentifier(triggerName, nameof(triggerName))} {timing.ToString().ToUpperInvariant()} {triggerEvent.ToString().ToUpperInvariant()} ON {RequireIdentifier(tableName, nameof(tableName))} BEGIN {normalizedBody}; END";
    }

    private static string NormalizeEmbeddedSql(string sql)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(sql);
        return sql.Trim().TrimEnd(';');
    }

    private static string MapDbType(Models.DbType type) => type switch
    {
        Models.DbType.Integer => "INTEGER",
        Models.DbType.Real => "REAL",
        Models.DbType.Text => "TEXT",
        Models.DbType.Blob => "BLOB",
        _ => throw new CSharpDbClientException($"Unsupported DbType '{type}'."),
    };

    private Database GetTransactionDatabase(string transactionId)
    {
        if (!_transactions.TryGetValue(transactionId, out var db))
            throw new CSharpDbClientException($"Transaction '{transactionId}' was not found.");
        return db;
    }

    private Database TakeTransactionDatabase(string transactionId)
    {
        if (!_transactions.TryRemove(transactionId, out var db))
            throw new CSharpDbClientException($"Transaction '{transactionId}' was not found.");
        return db;
    }
}
