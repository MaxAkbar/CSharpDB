using System.Globalization;
using System.Text.Json;
using CSharpDB.Client;
using CSharpDB.Client.Models;
using CSharpDB.Execution;
using CSharpDB.Primitives;
using CSharpDB.Sql;
using CoreColumnDefinition = CSharpDB.Primitives.ColumnDefinition;
using CoreDbType = CSharpDB.Primitives.DbType;
using CoreForeignKeyDefinition = CSharpDB.Primitives.ForeignKeyDefinition;
using CoreForeignKeyOnDeleteAction = CSharpDB.Primitives.ForeignKeyOnDeleteAction;
using CoreIndexSchema = CSharpDB.Primitives.IndexSchema;
using CoreTableSchema = CSharpDB.Primitives.TableSchema;
using CoreTriggerEvent = CSharpDB.Primitives.TriggerEvent;
using CoreTriggerSchema = CSharpDB.Primitives.TriggerSchema;
using CoreTriggerTiming = CSharpDB.Primitives.TriggerTiming;

namespace CSharpDB.Data;

internal sealed class RemoteDatabaseSession : ICSharpDbSession
{
    private ICSharpDbClient? _client;
    private readonly Func<ICSharpDbClient, ValueTask>? _releaseAsync;
    private string? _transactionId;

    public bool SupportsStructuredExecution => false;

    internal RemoteDatabaseSession(
        ICSharpDbClient client,
        Func<ICSharpDbClient, ValueTask>? releaseAsync = null)
    {
        _client = client ?? throw new ArgumentNullException(nameof(client));
        _releaseAsync = releaseAsync;
    }

    public ValueTask<QueryResult> ExecuteAsync(string sql, CancellationToken cancellationToken = default)
        => ExecuteSqlCoreAsync(sql, cancellationToken);

    public ValueTask<QueryResult> ExecuteAsync(Statement statement, CancellationToken cancellationToken = default)
        => throw new NotSupportedException("Structured statement execution is not supported for remote ADO.NET sessions.");

    public ValueTask<QueryResult> ExecuteAsync(SimpleInsertSql insert, CancellationToken cancellationToken = default)
        => throw new NotSupportedException("Structured insert execution is not supported for remote ADO.NET sessions.");

    public async ValueTask BeginTransactionAsync(CancellationToken cancellationToken = default)
    {
        if (_transactionId is not null)
            throw new InvalidOperationException("A transaction is already active.");

        TransactionSessionInfo info = await GetClient().BeginTransactionAsync(cancellationToken);
        _transactionId = info.TransactionId;
    }

    public async ValueTask CommitAsync(CancellationToken cancellationToken = default)
    {
        string transactionId = _transactionId ?? throw new InvalidOperationException("No transaction is active.");
        await GetClient().CommitTransactionAsync(transactionId, cancellationToken);
        _transactionId = null;
    }

    public async ValueTask RollbackAsync(CancellationToken cancellationToken = default)
    {
        string transactionId = _transactionId ?? throw new InvalidOperationException("No transaction is active.");
        await GetClient().RollbackTransactionAsync(transactionId, cancellationToken);
        _transactionId = null;
    }

    public async ValueTask SaveToFileAsync(string filePath, CancellationToken cancellationToken = default)
    {
        await GetClient().BackupAsync(new BackupRequest
        {
            DestinationPath = filePath,
        }, cancellationToken);
    }

    public IReadOnlyCollection<string> GetTableNames()
        => AwaitSync(() => GetClient().GetTableNamesAsync());

    public CoreTableSchema? GetTableSchema(string tableName)
        => MapTableSchema(AwaitSync(() => GetClient().GetTableSchemaAsync(tableName)));

    public IReadOnlyCollection<CoreIndexSchema> GetIndexes()
        => AwaitSync(() => GetClient().GetIndexesAsync()).Select(MapIndexSchema).ToArray();

    public IReadOnlyCollection<string> GetViewNames()
        => AwaitSync(() => GetClient().GetViewNamesAsync());

    public string? GetViewSql(string viewName)
        => AwaitSync(() => GetClient().GetViewSqlAsync(viewName));

    public IReadOnlyCollection<CoreTriggerSchema> GetTriggers()
        => AwaitSync(() => GetClient().GetTriggersAsync()).Select(MapTriggerSchema).ToArray();

    public async ValueTask DisposeAsync()
    {
        var client = _client;
        _client = null;
        _transactionId = null;

        if (client is not null)
        {
            if (_releaseAsync is null)
                await client.DisposeAsync();
            else
                await _releaseAsync(client);
        }
    }

    private async ValueTask<QueryResult> ExecuteSqlCoreAsync(string sql, CancellationToken cancellationToken)
    {
        SqlExecutionResult result = _transactionId is null
            ? await GetClient().ExecuteSqlAsync(sql, cancellationToken)
            : await GetClient().ExecuteInTransactionAsync(_transactionId, sql, cancellationToken);

        if (!string.IsNullOrWhiteSpace(result.Error))
            throw new CSharpDbException(ErrorCode.Unknown, result.Error);

        if (!result.IsQuery)
            return new QueryResult(result.RowsAffected);

        List<object?[]> sourceRows = result.Rows ?? [];
        var rows = new List<DbValue[]>(sourceRows.Count);
        foreach (object?[] row in sourceRows)
            rows.Add(row.Select(ToDbValue).ToArray());

        CoreColumnDefinition[] schema = BuildQuerySchema(result.ColumnNames ?? [], sourceRows);
        return QueryResult.FromMaterializedRows(schema, rows);
    }

    private static T AwaitSync<T>(Func<Task<T>> operation)
        => operation().GetAwaiter().GetResult();

    private ICSharpDbClient GetClient()
        => _client ?? throw new InvalidOperationException("Session is closed.");

    private static CoreTableSchema? MapTableSchema(CSharpDB.Client.Models.TableSchema? schema)
        => schema is null
            ? null
            : new CoreTableSchema
            {
                TableName = schema.TableName,
                Columns = schema.Columns.Select(MapColumnDefinition).ToArray(),
                ForeignKeys = schema.ForeignKeys.Select(MapForeignKeyDefinition).ToArray(),
            };

    private static CoreColumnDefinition MapColumnDefinition(CSharpDB.Client.Models.ColumnDefinition column)
        => new()
        {
            Name = column.Name,
            Type = MapDbType(column.Type),
            Nullable = column.Nullable,
            IsPrimaryKey = column.IsPrimaryKey,
            IsIdentity = column.IsIdentity,
            Collation = column.Collation,
        };

    private static CoreForeignKeyDefinition MapForeignKeyDefinition(CSharpDB.Client.Models.ForeignKeyDefinition foreignKey)
        => new()
        {
            ConstraintName = foreignKey.ConstraintName,
            ColumnName = foreignKey.ColumnName,
            ReferencedTableName = foreignKey.ReferencedTableName,
            ReferencedColumnName = foreignKey.ReferencedColumnName,
            OnDelete = foreignKey.OnDelete == CSharpDB.Client.Models.ForeignKeyOnDeleteAction.Cascade
                ? CoreForeignKeyOnDeleteAction.Cascade
                : CoreForeignKeyOnDeleteAction.Restrict,
            SupportingIndexName = foreignKey.SupportingIndexName,
        };

    private static CoreIndexSchema MapIndexSchema(CSharpDB.Client.Models.IndexSchema index)
        => new()
        {
            IndexName = index.IndexName,
            TableName = index.TableName,
            Columns = index.Columns,
            ColumnCollations = index.ColumnCollations,
            IsUnique = index.IsUnique,
        };

    private static CoreTriggerSchema MapTriggerSchema(CSharpDB.Client.Models.TriggerSchema trigger)
        => new()
        {
            TriggerName = trigger.TriggerName,
            TableName = trigger.TableName,
            Timing = trigger.Timing == CSharpDB.Client.Models.TriggerTiming.Before ? CoreTriggerTiming.Before : CoreTriggerTiming.After,
            Event = trigger.Event switch
            {
                CSharpDB.Client.Models.TriggerEvent.Insert => CoreTriggerEvent.Insert,
                CSharpDB.Client.Models.TriggerEvent.Update => CoreTriggerEvent.Update,
                _ => CoreTriggerEvent.Delete,
            },
            BodySql = trigger.BodySql,
        };

    private static CoreColumnDefinition[] BuildQuerySchema(IReadOnlyList<string> columnNames, IReadOnlyList<object?[]> rows)
    {
        var schema = new CoreColumnDefinition[columnNames.Count];
        for (int i = 0; i < columnNames.Count; i++)
        {
            schema[i] = new CoreColumnDefinition
            {
                Name = columnNames[i],
                Type = InferColumnType(rows, i),
                Nullable = true,
            };
        }

        return schema;
    }

    private static CoreDbType InferColumnType(IReadOnlyList<object?[]> rows, int ordinal)
    {
        foreach (object?[] row in rows)
        {
            if (ordinal >= row.Length)
                continue;

            object? value = row[ordinal];
            if (value is null or DBNull)
                continue;

            return value switch
            {
                bool or byte or sbyte or short or ushort or int or uint or long or ulong => CoreDbType.Integer,
                float or double or decimal => CoreDbType.Real,
                byte[] or ReadOnlyMemory<byte> => CoreDbType.Blob,
                Guid or DateTime or DateTimeOffset or char or string or JsonElement => CoreDbType.Text,
                _ => CoreDbType.Text,
            };
        }

        return CoreDbType.Null;
    }

    private static CoreDbType MapDbType(CSharpDB.Client.Models.DbType type)
        => type switch
        {
            CSharpDB.Client.Models.DbType.Integer => CoreDbType.Integer,
            CSharpDB.Client.Models.DbType.Real => CoreDbType.Real,
            CSharpDB.Client.Models.DbType.Text => CoreDbType.Text,
            CSharpDB.Client.Models.DbType.Blob => CoreDbType.Blob,
            _ => CoreDbType.Null,
        };

    private static DbValue ToDbValue(object? value)
        => value switch
        {
            null or DBNull => DbValue.Null,
            bool boolean => DbValue.FromInteger(boolean ? 1 : 0),
            byte number => DbValue.FromInteger(number),
            sbyte number => DbValue.FromInteger(number),
            short number => DbValue.FromInteger(number),
            ushort number => DbValue.FromInteger(number),
            int number => DbValue.FromInteger(number),
            uint number => DbValue.FromInteger(number),
            long number => DbValue.FromInteger(number),
            ulong number when number <= long.MaxValue => DbValue.FromInteger((long)number),
            float number => DbValue.FromReal(number),
            double number => DbValue.FromReal(number),
            decimal number => DbValue.FromReal((double)number),
            string text => DbValue.FromText(text),
            char character => DbValue.FromText(character.ToString()),
            Guid guid => DbValue.FromText(guid.ToString()),
            DateTime dateTime => DbValue.FromText(dateTime.ToString("O", CultureInfo.InvariantCulture)),
            DateTimeOffset dateTimeOffset => DbValue.FromText(dateTimeOffset.ToString("O", CultureInfo.InvariantCulture)),
            byte[] blob => DbValue.FromBlob(blob),
            ReadOnlyMemory<byte> blob => DbValue.FromBlob(blob.ToArray()),
            JsonElement json => json.ValueKind == JsonValueKind.Null
                ? DbValue.Null
                : DbValue.FromText(json.GetRawText()),
            _ => DbValue.FromText(Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty),
        };
}
