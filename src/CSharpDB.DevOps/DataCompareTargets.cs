using System.Globalization;
using System.Runtime.CompilerServices;
using CSharpDB.Client;
using CSharpDB.Client.Models;
using CSharpDB.ImportExport.TableArchives;
using CSharpDB.Primitives;
using ClientColumnDefinition = CSharpDB.Client.Models.ColumnDefinition;
using ClientDbType = CSharpDB.Client.Models.DbType;
using ClientForeignKeyDefinition = CSharpDB.Client.Models.ForeignKeyDefinition;
using ClientForeignKeyOnDeleteAction = CSharpDB.Client.Models.ForeignKeyOnDeleteAction;
using ClientTableSchema = CSharpDB.Client.Models.TableSchema;
using PrimitiveDbType = CSharpDB.Primitives.DbType;
using PrimitiveForeignKeyOnDeleteAction = CSharpDB.Primitives.ForeignKeyOnDeleteAction;

namespace CSharpDB.DevOps;

public sealed class ClientDataCompareTarget : IDataCompareTarget
{
    private readonly ICSharpDbClient _client;

    public ClientDataCompareTarget(ICSharpDbClient client, string? displayName = null)
    {
        _client = client ?? throw new ArgumentNullException(nameof(client));
        Descriptor = new SchemaTargetDescriptor
        {
            Kind = DevOpsTargetKind.Database,
            DisplayName = string.IsNullOrWhiteSpace(displayName) ? client.DataSource : displayName,
            Location = client.DataSource,
        };
    }

    public SchemaTargetDescriptor Descriptor { get; }

    public Task<ClientTableSchema?> GetTableSchemaAsync(string tableName, CancellationToken ct = default)
        => _client.GetTableSchemaAsync(tableName, ct);

    public async IAsyncEnumerable<IReadOnlyDictionary<string, object?>> ReadRowsAsync(
        ClientTableSchema schema,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        string sql = $"SELECT {string.Join(", ", schema.Columns.Select(column => SchemaScriptRenderer.Identifier(column.Name)))} FROM {SchemaScriptRenderer.Identifier(schema.TableName)};";
        SqlExecutionResult result = await _client.ExecuteSqlAsync(sql, ct);

        if (!string.IsNullOrWhiteSpace(result.Error))
            throw new InvalidOperationException(result.Error);

        if (result.Rows is null)
            yield break;

        foreach (object?[] row in result.Rows)
        {
            ct.ThrowIfCancellationRequested();
            yield return MaterializeRow(schema, row);
        }
    }

    private static IReadOnlyDictionary<string, object?> MaterializeRow(ClientTableSchema schema, object?[] values)
    {
        var row = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < schema.Columns.Count; i++)
            row[schema.Columns[i].Name] = i < values.Length ? NormalizeValue(values[i]) : null;
        return row;
    }

    private static object? NormalizeValue(object? value) => value;
}

public sealed class TableArchiveDataCompareTarget : IDataCompareTarget
{
    private readonly string _path;
    private readonly string? _tableNameOverride;

    public TableArchiveDataCompareTarget(string path, string? tableNameOverride = null)
    {
        if (string.IsNullOrWhiteSpace(path))
            throw new ArgumentException("Archive path is required.", nameof(path));

        _path = Path.GetFullPath(path);
        _tableNameOverride = string.IsNullOrWhiteSpace(tableNameOverride) ? null : tableNameOverride.Trim();
        Descriptor = new SchemaTargetDescriptor
        {
            Kind = DevOpsTargetKind.TableArchive,
            DisplayName = Path.GetFileName(_path),
            Location = _path,
        };
    }

    public SchemaTargetDescriptor Descriptor { get; }

    public async Task<ClientTableSchema?> GetTableSchemaAsync(string tableName, CancellationToken ct = default)
    {
        CSharpDB.Primitives.TableSchema schema =
            await TableArchiveReader.ReadTableSchemaAsync(_path, _tableNameOverride ?? tableName, ct);
        return string.Equals(schema.TableName, tableName, StringComparison.OrdinalIgnoreCase)
            ? MapTableSchema(schema)
            : null;
    }

    public async IAsyncEnumerable<IReadOnlyDictionary<string, object?>> ReadRowsAsync(
        ClientTableSchema schema,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        await foreach (DbValue[] row in TableArchiveReader.ReadRowsAsync(_path, ct))
        {
            ct.ThrowIfCancellationRequested();
            yield return MaterializeRow(schema, row);
        }
    }

    private static IReadOnlyDictionary<string, object?> MaterializeRow(ClientTableSchema schema, DbValue[] values)
    {
        var row = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < schema.Columns.Count; i++)
            row[schema.Columns[i].Name] = i < values.Length ? ConvertValue(values[i]) : null;
        return row;
    }

    private static object? ConvertValue(DbValue value)
    {
        if (value.IsNull)
            return null;

        return value.Type switch
        {
            CSharpDB.Primitives.DbType.Integer => value.AsInteger,
            CSharpDB.Primitives.DbType.Real => value.AsReal,
            CSharpDB.Primitives.DbType.Text => value.AsText,
            CSharpDB.Primitives.DbType.Blob => value.AsBlob,
            _ => Convert.ToString(value, CultureInfo.InvariantCulture),
        };
    }

    private static ClientTableSchema MapTableSchema(CSharpDB.Primitives.TableSchema schema)
        => new()
        {
            TableName = schema.TableName,
            Columns = schema.Columns.Select(column => new ClientColumnDefinition
            {
                Name = column.Name,
                Type = MapDbType(column.Type),
                Nullable = column.Nullable,
                IsPrimaryKey = column.IsPrimaryKey,
                IsIdentity = column.IsIdentity,
                Collation = column.Collation,
                DefaultSql = column.DefaultSql,
            }).ToArray(),
            ForeignKeys = schema.ForeignKeys.Select(foreignKey => new ClientForeignKeyDefinition
            {
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
                OnDelete = foreignKey.OnDelete switch
                {
                    PrimitiveForeignKeyOnDeleteAction.Restrict => ClientForeignKeyOnDeleteAction.Restrict,
                    PrimitiveForeignKeyOnDeleteAction.Cascade => ClientForeignKeyOnDeleteAction.Cascade,
                    _ => throw new ArgumentOutOfRangeException(nameof(foreignKey.OnDelete), foreignKey.OnDelete, null),
                },
                SupportingIndexName = foreignKey.SupportingIndexName,
            }).ToArray(),
            KeyConstraints = schema.KeyConstraints.Select(key => new CSharpDB.Client.Models.KeyConstraintDefinition
            {
                ConstraintName = key.ConstraintName,
                Kind = key.Kind switch
                {
                    CSharpDB.Primitives.KeyConstraintKind.PrimaryKey => CSharpDB.Client.Models.KeyConstraintKind.PrimaryKey,
                    CSharpDB.Primitives.KeyConstraintKind.Unique => CSharpDB.Client.Models.KeyConstraintKind.Unique,
                    _ => throw new InvalidOperationException($"Unsupported key constraint kind '{key.Kind}'."),
                },
                Columns = key.Columns.ToArray(),
                BackingIndexName = key.BackingIndexName,
            }).ToArray(),
            CheckConstraints = schema.CheckConstraints.Select(check => new CSharpDB.Client.Models.CheckConstraintDefinition
            {
                ConstraintName = check.ConstraintName,
                ExpressionSql = check.ExpressionSql,
                ColumnName = check.ColumnName,
            }).ToArray(),
        };

    private static ClientDbType MapDbType(PrimitiveDbType type) => type switch
    {
        PrimitiveDbType.Integer => ClientDbType.Integer,
        PrimitiveDbType.Real => ClientDbType.Real,
        PrimitiveDbType.Text => ClientDbType.Text,
        PrimitiveDbType.Blob => ClientDbType.Blob,
        _ => throw new ArgumentOutOfRangeException(nameof(type), type, null),
    };
}
