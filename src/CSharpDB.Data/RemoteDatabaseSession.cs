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
using CoreSqlTypeDescriptor = CSharpDB.Primitives.SqlTypeDescriptor;
using CoreSqlTypeKind = CSharpDB.Primitives.SqlTypeKind;
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

        CoreColumnDefinition[] schema = BuildQuerySchema(
            result.ColumnNames ?? [],
            result.ColumnTypes,
            result.ColumnNullability,
            sourceRows,
            result.Columns);
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
                SchemaId = schema.SchemaId,
                TableName = schema.TableName,
                Columns = schema.Columns.Select(MapColumnDefinition).ToArray(),
                ForeignKeys = schema.ForeignKeys.Select(MapForeignKeyDefinition).ToArray(),
                CheckConstraints = schema.CheckConstraints.Select(check => new CSharpDB.Primitives.CheckConstraintDefinition
                {
                    SchemaId = check.SchemaId,
                    ConstraintName = check.ConstraintName,
                    ExpressionSql = check.ExpressionSql,
                    ColumnName = check.ColumnName,
                }).ToArray(),
                KeyConstraints = schema.KeyConstraints.Select(key => new CSharpDB.Primitives.KeyConstraintDefinition
                {
                    SchemaId = key.SchemaId,
                    ConstraintName = key.ConstraintName,
                    Kind = key.Kind switch
                    {
                        CSharpDB.Client.Models.KeyConstraintKind.PrimaryKey => CSharpDB.Primitives.KeyConstraintKind.PrimaryKey,
                        CSharpDB.Client.Models.KeyConstraintKind.Unique => CSharpDB.Primitives.KeyConstraintKind.Unique,
                        _ => throw new InvalidOperationException($"Unsupported key constraint kind '{key.Kind}'."),
                    },
                    Columns = key.Columns.ToArray(),
                    BackingIndexName = key.BackingIndexName,
                }).ToArray(),
            };

    private static CoreColumnDefinition MapColumnDefinition(CSharpDB.Client.Models.ColumnDefinition column)
        => new()
        {
            SchemaId = column.SchemaId,
            Name = column.Name,
            Type = MapDbType(column.Type),
            DeclaredType = column.DeclaredType is null
                ? null
                : MapSqlTypeDescriptor(column.DeclaredType),
            Nullable = column.Nullable,
            IsPrimaryKey = column.IsPrimaryKey,
            IsIdentity = column.IsIdentity,
            IsRowVersion = column.IsRowVersion,
            Collation = column.Collation,
            DefaultSql = column.DefaultSql,
        };

    private static CSharpDB.Primitives.SqlTypeDescriptor MapSqlTypeDescriptor(
        CSharpDB.Client.Models.SqlTypeDescriptor type)
        => CSharpDB.Primitives.SqlTypeDescriptor.Create(
            type.Kind switch
            {
                CSharpDB.Client.Models.SqlTypeKind.Boolean => CSharpDB.Primitives.SqlTypeKind.Boolean,
                CSharpDB.Client.Models.SqlTypeKind.TinyInt => CSharpDB.Primitives.SqlTypeKind.TinyInt,
                CSharpDB.Client.Models.SqlTypeKind.SmallInt => CSharpDB.Primitives.SqlTypeKind.SmallInt,
                CSharpDB.Client.Models.SqlTypeKind.Integer => CSharpDB.Primitives.SqlTypeKind.Integer,
                CSharpDB.Client.Models.SqlTypeKind.BigInt => CSharpDB.Primitives.SqlTypeKind.BigInt,
                CSharpDB.Client.Models.SqlTypeKind.Real => CSharpDB.Primitives.SqlTypeKind.Real,
                CSharpDB.Client.Models.SqlTypeKind.Double => CSharpDB.Primitives.SqlTypeKind.Double,
                CSharpDB.Client.Models.SqlTypeKind.Decimal => CSharpDB.Primitives.SqlTypeKind.Decimal,
                CSharpDB.Client.Models.SqlTypeKind.Char => CSharpDB.Primitives.SqlTypeKind.Char,
                CSharpDB.Client.Models.SqlTypeKind.VarChar => CSharpDB.Primitives.SqlTypeKind.VarChar,
                CSharpDB.Client.Models.SqlTypeKind.Text => CSharpDB.Primitives.SqlTypeKind.Text,
                CSharpDB.Client.Models.SqlTypeKind.Binary => CSharpDB.Primitives.SqlTypeKind.Binary,
                CSharpDB.Client.Models.SqlTypeKind.VarBinary => CSharpDB.Primitives.SqlTypeKind.VarBinary,
                CSharpDB.Client.Models.SqlTypeKind.Blob => CSharpDB.Primitives.SqlTypeKind.Blob,
                CSharpDB.Client.Models.SqlTypeKind.Uuid => CSharpDB.Primitives.SqlTypeKind.Uuid,
                CSharpDB.Client.Models.SqlTypeKind.Date => CSharpDB.Primitives.SqlTypeKind.Date,
                CSharpDB.Client.Models.SqlTypeKind.Time => CSharpDB.Primitives.SqlTypeKind.Time,
                CSharpDB.Client.Models.SqlTypeKind.Timestamp => CSharpDB.Primitives.SqlTypeKind.Timestamp,
                CSharpDB.Client.Models.SqlTypeKind.TimestampWithTimeZone => CSharpDB.Primitives.SqlTypeKind.TimestampWithTimeZone,
                CSharpDB.Client.Models.SqlTypeKind.IntervalYearToMonth => CSharpDB.Primitives.SqlTypeKind.IntervalYearToMonth,
                CSharpDB.Client.Models.SqlTypeKind.IntervalDayToSecond => CSharpDB.Primitives.SqlTypeKind.IntervalDayToSecond,
                CSharpDB.Client.Models.SqlTypeKind.Json => CSharpDB.Primitives.SqlTypeKind.Json,
                CSharpDB.Client.Models.SqlTypeKind.Xml => CSharpDB.Primitives.SqlTypeKind.Xml,
                CSharpDB.Client.Models.SqlTypeKind.Bit => CSharpDB.Primitives.SqlTypeKind.Bit,
                CSharpDB.Client.Models.SqlTypeKind.VarBit => CSharpDB.Primitives.SqlTypeKind.VarBit,
                _ => throw new InvalidOperationException($"Unsupported logical SQL type '{type.Kind}'."),
            },
            type.Length,
            type.Precision,
            type.Scale,
            type.FractionalSecondsPrecision);

    private static CoreForeignKeyDefinition MapForeignKeyDefinition(CSharpDB.Client.Models.ForeignKeyDefinition foreignKey)
        => new()
        {
            SchemaId = foreignKey.SchemaId,
            ColumnSchemaIds = foreignKey.ColumnSchemaIds.ToArray(),
            ReferencedTableSchemaId = foreignKey.ReferencedTableSchemaId,
            ReferencedColumnSchemaIds = foreignKey.ReferencedColumnSchemaIds.ToArray(),
            ReferencedKeySchemaId = foreignKey.ReferencedKeySchemaId,
            ConstraintName = foreignKey.ConstraintName,
            ColumnName = foreignKey.ColumnName,
            ReferencedTableName = foreignKey.ReferencedTableName,
            ReferencedColumnName = foreignKey.ReferencedColumnName,
            ColumnNames = foreignKey.ColumnNames.Count > 0
                ? foreignKey.ColumnNames.ToArray()
                : [foreignKey.ColumnName],
            ReferencedColumnNames = foreignKey.ReferencedColumnNames.Count > 0
                ? foreignKey.ReferencedColumnNames.ToArray()
                : [foreignKey.ReferencedColumnName],
            OnDelete = MapForeignKeyAction(foreignKey.OnDelete),
            OnUpdate = MapForeignKeyAction(foreignKey.OnUpdate),
            SupportingIndexName = foreignKey.SupportingIndexName,
        };

    private static CoreForeignKeyOnDeleteAction MapForeignKeyAction(
        CSharpDB.Client.Models.ForeignKeyOnDeleteAction action) =>
        action switch
        {
            CSharpDB.Client.Models.ForeignKeyOnDeleteAction.Restrict =>
                CoreForeignKeyOnDeleteAction.Restrict,
            CSharpDB.Client.Models.ForeignKeyOnDeleteAction.Cascade =>
                CoreForeignKeyOnDeleteAction.Cascade,
            CSharpDB.Client.Models.ForeignKeyOnDeleteAction.NoAction =>
                CoreForeignKeyOnDeleteAction.NoAction,
            CSharpDB.Client.Models.ForeignKeyOnDeleteAction.SetNull =>
                CoreForeignKeyOnDeleteAction.SetNull,
            CSharpDB.Client.Models.ForeignKeyOnDeleteAction.SetDefault =>
                CoreForeignKeyOnDeleteAction.SetDefault,
            _ => throw new InvalidDataException(
                $"Unsupported foreign key referential action '{action}'."),
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

    private static CoreColumnDefinition[] BuildQuerySchema(
        IReadOnlyList<string> columnNames,
        IReadOnlyList<string>? columnTypes,
        IReadOnlyList<bool>? columnNullability,
        IReadOnlyList<object?[]> rows,
        IReadOnlyList<CSharpDB.Client.Models.ColumnDefinition>? columns)
    {
        if (columns is not null && columns.Count == columnNames.Count)
            return columns.Select(MapColumnDefinition).ToArray();

        var schema = new CoreColumnDefinition[columnNames.Count];
        for (int i = 0; i < columnNames.Count; i++)
        {
            CoreDbType storageType = ResolveColumnType(columnTypes, rows, i);
            CoreSqlTypeDescriptor? declaredType = null;
            bool isRowVersion = false;
            if (columnTypes is not null && i < columnTypes.Count)
            {
                TryResolveLogicalColumnType(
                    columnTypes[i],
                    ref storageType,
                    out declaredType,
                    out isRowVersion);
            }

            schema[i] = new CoreColumnDefinition
            {
                Name = columnNames[i],
                Type = storageType,
                DeclaredType = declaredType,
                Nullable = columnNullability is not null &&
                           i < columnNullability.Count
                    ? columnNullability[i]
                    : true,
                IsRowVersion = isRowVersion,
            };
        }

        return schema;
    }

    private static CoreDbType ResolveColumnType(
        IReadOnlyList<string>? columnTypes,
        IReadOnlyList<object?[]> rows,
        int ordinal)
    {
        if (columnTypes is not null
            && ordinal < columnTypes.Count
            && Enum.TryParse(
                columnTypes[ordinal],
                ignoreCase: true,
                out CoreDbType declaredType))
        {
            return declaredType;
        }

        return InferColumnType(rows, ordinal);
    }

    private static void TryResolveLogicalColumnType(
        string reportedType,
        ref CoreDbType storageType,
        out CoreSqlTypeDescriptor? declaredType,
        out bool isRowVersion)
    {
        declaredType = null;
        isRowVersion = false;

        if (string.Equals(reportedType, "NULL", StringComparison.OrdinalIgnoreCase))
        {
            storageType = CoreDbType.Null;
            return;
        }

        if (string.Equals(reportedType, "ROWVERSION", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(reportedType, "TIMESTAMP", StringComparison.OrdinalIgnoreCase))
        {
            storageType = CoreDbType.Blob;
            declaredType = CoreSqlTypeDescriptor.Create(CoreSqlTypeKind.Blob);
            isRowVersion = true;
            return;
        }

        try
        {
            if (Parser.Parse($"SELECT CAST(NULL AS {reportedType})") is SelectStatement
                {
                    Columns: [{ Expression: CastExpression cast }],
                })
            {
                declaredType = cast.TargetType;
                storageType = declaredType.StorageType;
            }
        }
        catch (CSharpDbException)
        {
            // Older servers can return provider-specific labels. Retain the
            // physical/inferred fallback when a label is not valid CSharpDB SQL.
        }
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
                float or double => CoreDbType.Real,
                decimal => CoreDbType.Decimal,
                SqlBitString => CoreDbType.Blob,
                byte[] or ReadOnlyMemory<byte> => CoreDbType.Blob,
                Guid or DateOnly or TimeOnly or DateTime or DateTimeOffset or char or string or JsonElement => CoreDbType.Text,
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
            CSharpDB.Client.Models.DbType.Decimal => CoreDbType.Decimal,
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
            decimal number => DbValue.FromDecimal(number),
            string text => DbValue.FromText(text),
            char character => DbValue.FromText(character.ToString()),
            Guid guid => DbValue.FromText(CSharpDbTextCodec.FormatGuid(guid)),
            DateOnly date => DbValue.FromText(CSharpDbTextCodec.FormatDate(date)),
            TimeOnly time => DbValue.FromText(CSharpDbTextCodec.FormatTime(time)),
            DateTime dateTime => DbValue.FromText(CSharpDbTextCodec.FormatDateTime(dateTime)),
            DateTimeOffset dateTimeOffset => DbValue.FromText(
                CSharpDbTextCodec.FormatDateTimeOffset(dateTimeOffset)),
            SqlBitString bits => DbValue.FromBitString(
                bits.PackedBytes.ToArray(),
                bits.BitLength),
            byte[] blob => DbValue.FromBlob(blob),
            ReadOnlyMemory<byte> blob => DbValue.FromBlob(blob.ToArray()),
            JsonElement json => json.ValueKind == JsonValueKind.Null
                ? DbValue.Null
                : DbValue.FromText(json.GetRawText()),
            _ => DbValue.FromText(Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty),
        };
}
