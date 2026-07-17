using System.Globalization;
using System.Text.Json;
using CSharpDB.Client;
using CSharpDB.Client.Internal;
using CSharpDB.Client.Models;
using CSharpDB.Engine;
using CSharpDB.ImportExport.TableArchives;
using CoreIndexSchema = CSharpDB.Primitives.IndexSchema;
using CoreTriggerEvent = CSharpDB.Primitives.TriggerEvent;
using CoreTriggerSchema = CSharpDB.Primitives.TriggerSchema;
using CoreTriggerTiming = CSharpDB.Primitives.TriggerTiming;
using ClientColumnDefinition = CSharpDB.Client.Models.ColumnDefinition;
using ClientDbType = CSharpDB.Client.Models.DbType;
using ClientIndexSchema = CSharpDB.Client.Models.IndexSchema;
using ClientForeignKeyDefinition = CSharpDB.Client.Models.ForeignKeyDefinition;
using ClientForeignKeyOnDeleteAction = CSharpDB.Client.Models.ForeignKeyOnDeleteAction;
using ClientTableSchema = CSharpDB.Client.Models.TableSchema;
using ClientTriggerEvent = CSharpDB.Client.Models.TriggerEvent;
using ClientTriggerSchema = CSharpDB.Client.Models.TriggerSchema;
using ClientTriggerTiming = CSharpDB.Client.Models.TriggerTiming;
using PrimitiveDbValue = CSharpDB.Primitives.DbValue;
using PrimitiveDbType = CSharpDB.Primitives.DbType;
using PrimitiveForeignKeyOnDeleteAction = CSharpDB.Primitives.ForeignKeyOnDeleteAction;

namespace CSharpDB.DevOps;

public sealed class ClientSchemaCompareTarget : ISchemaCompareTarget
{
    private readonly ICSharpDbClient _client;

    public ClientSchemaCompareTarget(ICSharpDbClient client, string? displayName = null)
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

    public async Task<SchemaSnapshot> LoadSchemaAsync(CancellationToken ct = default)
    {
        SchemaSnapshot? directSnapshot = await TryLoadDirectEngineSchemaAsync(ct);
        if (directSnapshot is not null)
            return directSnapshot;

        IReadOnlyList<string> tableNames = await _client.GetTableNamesAsync(ct);
        var tables = new List<ClientTableSchema>(tableNames.Count);
        foreach (string tableName in tableNames.OrderBy(name => name, StringComparer.OrdinalIgnoreCase))
        {
            ClientTableSchema? schema = await _client.GetTableSchemaAsync(tableName, ct);
            if (schema is not null)
                tables.Add(schema);
        }

        return new SchemaSnapshot
        {
            Target = Descriptor,
            Tables = tables,
            Indexes = await _client.GetIndexesAsync(ct),
            Views = await _client.GetViewsAsync(ct),
            Triggers = await _client.GetTriggersAsync(ct),
            Procedures = await LoadProceduresWithoutInitializingCatalogAsync(ct),
        };
    }

    private async Task<SchemaSnapshot?> TryLoadDirectEngineSchemaAsync(CancellationToken ct)
    {
        if (_client is not IEngineBackedClient engineBacked)
            return null;

        Database? db = await engineBacked.TryGetDatabaseAsync(ct);
        if (db is null)
            return null;

        var tableNames = db.GetTableNames()
            .Where(name => !IsInternalTable(name))
            .OrderBy(name => name, StringComparer.OrdinalIgnoreCase)
            .ToArray();

        var tables = new List<ClientTableSchema>(tableNames.Length);
        foreach (string tableName in tableNames)
        {
            CSharpDB.Primitives.TableSchema? schema = db.GetTableSchema(tableName);
            if (schema is not null)
                tables.Add(MapTableSchema(schema));
        }

        return new SchemaSnapshot
        {
            Target = Descriptor,
            Tables = tables,
            Indexes = db.GetIndexes()
                .Where(index =>
                    !IsInternalTable(index.TableName) &&
                    index.Kind is not (
                        CSharpDB.Primitives.IndexKind.ForeignKeyInternal or
                        CSharpDB.Primitives.IndexKind.ConstraintInternal))
                .Select(MapIndexSchema)
                .OrderBy(index => index.IndexName, StringComparer.OrdinalIgnoreCase)
                .ToArray(),
            Views = db.GetViewNames()
                .OrderBy(name => name, StringComparer.OrdinalIgnoreCase)
                .Select(name => new ViewDefinition { Name = name, Sql = db.GetViewSql(name) ?? string.Empty })
                .ToArray(),
            Triggers = db.GetTriggers()
                .Select(MapTriggerSchema)
                .OrderBy(trigger => trigger.TriggerName, StringComparer.OrdinalIgnoreCase)
                .ToArray(),
            Procedures = await LoadDirectProceduresAsync(db, ct),
        };
    }

    private static async Task<IReadOnlyList<ProcedureDefinition>> LoadDirectProceduresAsync(Database db, CancellationToken ct)
    {
        if (db.GetTableSchema("__procedures") is null)
            return [];

        await using var result = await db.ExecuteAsync(
            """
            SELECT name, body_sql, params_json, description, is_enabled, created_utc, updated_utc
            FROM __procedures
            ORDER BY name;
            """,
            ct);

        var procedures = new List<ProcedureDefinition>();
        while (await result.MoveNextAsync(ct))
            procedures.Add(ReadProcedureDefinition(result.Current));

        return procedures;
    }

    private async Task<IReadOnlyList<ProcedureDefinition>> LoadProceduresWithoutInitializingCatalogAsync(CancellationToken ct)
    {
        SqlExecutionResult result = await _client.ExecuteSqlAsync(
            """
            SELECT name, body_sql, params_json, description, is_enabled, created_utc, updated_utc
            FROM __procedures
            ORDER BY name;
            """,
            ct);

        if (!string.IsNullOrWhiteSpace(result.Error))
        {
            if (result.Error.Contains("Table '__procedures' not found", StringComparison.OrdinalIgnoreCase))
                return [];

            throw new InvalidOperationException(result.Error);
        }

        if (result.Rows is null || result.Rows.Count == 0)
            return [];

        return result.Rows.Select(ReadProcedureDefinition).ToArray();
    }

    private static ProcedureDefinition ReadProcedureDefinition(object?[] row)
        => new()
        {
            Name = Convert.ToString(row[0], CultureInfo.InvariantCulture) ?? string.Empty,
            BodySql = Convert.ToString(row[1], CultureInfo.InvariantCulture) ?? string.Empty,
            Parameters = DeserializeProcedureParameters(Convert.ToString(row[2], CultureInfo.InvariantCulture) ?? string.Empty),
            Description = row[3] is null ? null : Convert.ToString(row[3], CultureInfo.InvariantCulture),
            IsEnabled = row[4] is not null && Convert.ToInt64(row[4], CultureInfo.InvariantCulture) != 0,
            CreatedUtc = ParseStoredUtc(Convert.ToString(row[5], CultureInfo.InvariantCulture) ?? string.Empty),
            UpdatedUtc = ParseStoredUtc(Convert.ToString(row[6], CultureInfo.InvariantCulture) ?? string.Empty),
        };

    private static ProcedureDefinition ReadProcedureDefinition(PrimitiveDbValue[] row)
        => new()
        {
            Name = ReadText(row[0]),
            BodySql = ReadText(row[1]),
            Parameters = DeserializeProcedureParameters(ReadText(row[2])),
            Description = row[3].IsNull ? null : ReadText(row[3]),
            IsEnabled = !row[4].IsNull && row[4].AsInteger != 0,
            CreatedUtc = ParseStoredUtc(ReadText(row[5])),
            UpdatedUtc = ParseStoredUtc(ReadText(row[6])),
        };

    private static IReadOnlyList<ProcedureParameterDefinition> DeserializeProcedureParameters(string json)
    {
        if (string.IsNullOrWhiteSpace(json))
            return [];

        var storage = JsonSerializer.Deserialize<List<ProcedureParameterStorage>>(json, s_jsonOptions);
        if (storage is null || storage.Count == 0)
            return [];

        return storage.Select(item => new ProcedureParameterDefinition
        {
            Name = item.Name ?? string.Empty,
            Type = Enum.TryParse<DbType>(item.Type, ignoreCase: true, out var parsedType) ? parsedType : DbType.Text,
            Required = item.Required,
            Default = item.Default is JsonElement element ? ConvertJsonElement(element) : item.Default,
            Description = item.Description,
        }).ToArray();
    }

    private static object? ConvertJsonElement(JsonElement element) => element.ValueKind switch
    {
        JsonValueKind.Null => null,
        JsonValueKind.True => true,
        JsonValueKind.False => false,
        JsonValueKind.String => element.GetString(),
        JsonValueKind.Number when element.TryGetInt64(out long integer) => integer,
        JsonValueKind.Number when element.TryGetDouble(out double real) => real,
        _ => element.GetRawText(),
    };

    private static DateTime ParseStoredUtc(string value)
        => DateTime.TryParse(
            value,
            CultureInfo.InvariantCulture,
            DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal,
            out DateTime parsed)
            ? parsed
            : DateTime.MinValue;

    private static readonly JsonSerializerOptions s_jsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        PropertyNameCaseInsensitive = true,
    };

    private sealed class ProcedureParameterStorage
    {
        public string? Name { get; init; }
        public string Type { get; init; } = "TEXT";
        public bool Required { get; init; }
        public object? Default { get; init; }
        public string? Description { get; init; }
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

    private static ClientIndexSchema MapIndexSchema(CoreIndexSchema index)
        => new()
        {
            IndexName = index.IndexName,
            TableName = index.TableName,
            Columns = index.Columns.ToArray(),
            ColumnCollations = index.ColumnCollations.ToArray(),
            IsUnique = index.IsUnique,
        };

    private static ClientTriggerSchema MapTriggerSchema(CoreTriggerSchema trigger)
        => new()
        {
            TriggerName = trigger.TriggerName,
            TableName = trigger.TableName,
            Timing = trigger.Timing switch
            {
                CoreTriggerTiming.Before => ClientTriggerTiming.Before,
                CoreTriggerTiming.After => ClientTriggerTiming.After,
                _ => throw new ArgumentOutOfRangeException(nameof(trigger.Timing), trigger.Timing, null),
            },
            Event = trigger.Event switch
            {
                CoreTriggerEvent.Insert => ClientTriggerEvent.Insert,
                CoreTriggerEvent.Update => ClientTriggerEvent.Update,
                CoreTriggerEvent.Delete => ClientTriggerEvent.Delete,
                _ => throw new ArgumentOutOfRangeException(nameof(trigger.Event), trigger.Event, null),
            },
            BodySql = trigger.BodySql,
        };

    private static bool IsInternalTable(string name)
        => name.StartsWith("__", StringComparison.Ordinal)
           || name.StartsWith("_col_", StringComparison.Ordinal);

    private static string ReadText(PrimitiveDbValue value)
        => value.IsNull ? string.Empty : value.AsText;
}

public sealed class TableArchiveSchemaCompareTarget : ISchemaCompareTarget
{
    private readonly string _path;
    private readonly string? _tableNameOverride;

    public TableArchiveSchemaCompareTarget(string path, string? tableNameOverride = null)
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

    public async Task<SchemaSnapshot> LoadSchemaAsync(CancellationToken ct = default)
    {
        CSharpDB.Primitives.TableSchema archiveSchema =
            await TableArchiveReader.ReadTableSchemaAsync(_path, _tableNameOverride, ct);

        return new SchemaSnapshot
        {
            Target = Descriptor,
            Tables = [MapTableSchema(archiveSchema)],
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
