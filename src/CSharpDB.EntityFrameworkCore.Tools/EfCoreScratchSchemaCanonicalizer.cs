using System.Data;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using CSharpDB.Data;
using CSharpDB.Migration;
using CSharpDB.Migration.Validation;
using CSharpDB.Primitives;

namespace CSharpDB.EntityFrameworkCore.Tools;

internal enum EfCoreScratchSchemaCaptureFailure
{
    None = 0,
    ConnectionRejected = 1,
    LimitExceeded = 2,
    InvalidSchema = 3,
}

internal readonly record struct EfCoreScratchSchemaCaptureLimits(
    int MaxObjects,
    int MaxInputBytes)
{
    internal static EfCoreScratchSchemaCaptureLimits Default { get; } = new(
        MaxObjects: 20_000,
        MaxInputBytes: 4 * 1024 * 1024);

    internal void Validate()
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(MaxObjects, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(MaxInputBytes, 1);
    }
}

internal sealed record EfCoreScratchSchemaCaptureResult
{
    private EfCoreScratchSchemaCaptureResult(
        EfCoreScratchSchemaCaptureFailure failure,
        MigrationNormalizedSchema? schema)
    {
        Failure = failure;
        Schema = schema;
    }

    internal EfCoreScratchSchemaCaptureFailure Failure { get; }

    internal MigrationNormalizedSchema? Schema { get; }

    internal bool Succeeded =>
        Failure == EfCoreScratchSchemaCaptureFailure.None &&
        Schema is not null;

    internal static EfCoreScratchSchemaCaptureResult Captured(
        MigrationNormalizedSchema schema) =>
        new(EfCoreScratchSchemaCaptureFailure.None, schema);

    internal static EfCoreScratchSchemaCaptureResult Failed(
        EfCoreScratchSchemaCaptureFailure failure)
    {
        if (failure == EfCoreScratchSchemaCaptureFailure.None)
            throw new ArgumentOutOfRangeException(nameof(failure));
        return new EfCoreScratchSchemaCaptureResult(failure, schema: null);
    }
}

/// <summary>
/// Captures deterministic logical-schema evidence from a tool-owned, open
/// private-memory CSharpDB connection. The result contains no rows or SQL;
/// only the normalized contract retains structural metadata, and callers
/// should publish its digest rather than its definitions.
/// </summary>
internal static class EfCoreScratchSchemaCanonicalizer
{
    internal const string HistoryTableName = "__EFMigrationsHistory";

    private const string ObjectIdDomain = "csharpdb-ef-scratch-object/v1";
    private const string UnnamedObject = "<UNNAMED>";
    private const string ReferencedTableRole = "referencedTable";
    private const string ReferencedColumnRole = "referencedColumn";

    internal static EfCoreScratchSchemaCaptureResult Capture(
        CSharpDbConnection connection,
        CancellationToken cancellationToken = default) =>
        Capture(
            connection,
            EfCoreScratchSchemaCaptureLimits.Default,
            cancellationToken);

    internal static EfCoreScratchSchemaCaptureResult Capture(
        CSharpDbConnection connection,
        EfCoreScratchSchemaCaptureLimits limits,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(connection);
        limits.Validate();
        cancellationToken.ThrowIfCancellationRequested();

        if (!IsOpenPrivateMemoryConnection(connection))
        {
            return EfCoreScratchSchemaCaptureResult.Failed(
                EfCoreScratchSchemaCaptureFailure.ConnectionRejected);
        }

        try
        {
            MigrationNormalizedSchema schema =
                new CaptureBuilder(connection, limits, cancellationToken)
                    .Capture();
            return EfCoreScratchSchemaCaptureResult.Captured(schema);
        }
        catch (CaptureLimitException)
        {
            return EfCoreScratchSchemaCaptureResult.Failed(
                EfCoreScratchSchemaCaptureFailure.LimitExceeded);
        }
        catch (OperationCanceledException)
            when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception error) when (IsRecoverable(error))
        {
            return EfCoreScratchSchemaCaptureResult.Failed(
                EfCoreScratchSchemaCaptureFailure.InvalidSchema);
        }
    }

    private static bool IsOpenPrivateMemoryConnection(
        CSharpDbConnection connection)
    {
        if (connection.State != ConnectionState.Open)
            return false;

        try
        {
            var builder = new CSharpDbConnectionStringBuilder(
                connection.ConnectionString);
            return string.Equals(
                    builder.DataSource,
                    ":memory:",
                    StringComparison.OrdinalIgnoreCase) &&
                !builder.Pooling &&
                string.IsNullOrWhiteSpace(builder.Endpoint) &&
                string.IsNullOrWhiteSpace(builder.LoadFrom) &&
                string.IsNullOrWhiteSpace(builder.Transport);
        }
        catch (Exception error) when (IsRecoverable(error))
        {
            return false;
        }
    }

    private static bool IsRecoverable(Exception error) => error is not
        OutOfMemoryException and not
        StackOverflowException and not
        AccessViolationException;

    private sealed class CaptureBuilder
    {
        private readonly CSharpDbConnection _connection;
        private readonly EfCoreScratchSchemaCaptureLimits _limits;
        private readonly CancellationToken _cancellationToken;
        private readonly List<MigrationNormalizedSchemaObject> _objects = [];
        private int _inputBytes;

        internal CaptureBuilder(
            CSharpDbConnection connection,
            EfCoreScratchSchemaCaptureLimits limits,
            CancellationToken cancellationToken)
        {
            _connection = connection;
            _limits = limits;
            _cancellationToken = cancellationToken;
        }

        internal MigrationNormalizedSchema Capture()
        {
            IReadOnlyList<TableShape> tables = CaptureTables();
            IReadOnlyDictionary<string, TableShape> tablesByName = tables
                .ToDictionary(table => table.NormalizedName, StringComparer.Ordinal);

            CaptureKeysAndChecks(tables);
            CaptureForeignKeys(tables, tablesByName);
            CaptureIndexes(tablesByName);
            CaptureViews();
            CaptureTriggers(tablesByName);

            _cancellationToken.ThrowIfCancellationRequested();
            return MigrationNormalizedSchemaContract.Create(_objects);
        }

        private IReadOnlyList<TableShape> CaptureTables()
        {
            var tableNames = new List<(string Actual, string Normalized)>();
            foreach (string tableName in _connection.GetTableNames())
            {
                _cancellationToken.ThrowIfCancellationRequested();
                TrackInput(tableName);
                string normalizedName = NormalizeIdentifier(tableName);
                if (!IsHistoryTable(normalizedName))
                {
                    if (tableNames.Count >= _limits.MaxObjects)
                        throw new CaptureLimitException();
                    tableNames.Add((tableName, normalizedName));
                }
            }

            var result = new List<TableShape>(tableNames.Count);
            var knownNames = new HashSet<string>(StringComparer.Ordinal);
            foreach ((string actualName, string normalizedName) in tableNames
                         .OrderBy(item => item.Normalized, StringComparer.Ordinal))
            {
                _cancellationToken.ThrowIfCancellationRequested();
                if (!knownNames.Add(normalizedName))
                    throw new InvalidDataException();

                TableSchema schema = _connection.GetTableSchema(actualName) ??
                    throw new InvalidDataException();
                TrackInput(schema.TableName);
                if (!string.Equals(
                        normalizedName,
                        NormalizeIdentifier(schema.TableName),
                        StringComparison.Ordinal))
                {
                    throw new InvalidDataException();
                }

                string tableId = CreateObjectId(
                    MigrationObjectKind.Table,
                    normalizedName);
                AddObject(MigrationNormalizedSchemaContract.CreateObject(
                    tableId,
                    MigrationObjectKind.Table,
                    parentObjectId: null,
                    targetName: normalizedName));

                var columnIds = new Dictionary<string, string>(
                    StringComparer.Ordinal);
                for (int ordinal = 0; ordinal < schema.Columns.Count; ordinal++)
                {
                    _cancellationToken.ThrowIfCancellationRequested();
                    ColumnDefinition column = schema.Columns[ordinal] ??
                        throw new InvalidDataException();
                    TrackInput(
                        column.Name,
                        column.Collation,
                        column.DefaultSql);
                    string columnName = NormalizeIdentifier(column.Name);
                    string columnId = CreateObjectId(
                        MigrationObjectKind.Column,
                        normalizedName,
                        columnName);
                    if (!columnIds.TryAdd(columnName, columnId))
                        throw new InvalidDataException();

                    var attributes =
                        new List<MigrationNormalizedSchemaAttribute>
                        {
                            Attribute(
                                "ordinal",
                                ordinal.ToString(CultureInfo.InvariantCulture)),
                            Attribute(
                                "storeType",
                                NormalizeStoreType(column.Type)),
                            Attribute(
                                "nullable",
                                BooleanToken(column.Nullable)),
                            Attribute(
                                "identity",
                                BooleanToken(column.IsIdentity)),
                            Attribute(
                                "rowVersion",
                                BooleanToken(column.IsRowVersion)),
                        };
                    if (column.Collation is not null)
                    {
                        attributes.Add(Attribute(
                            "collation",
                            NormalizeToken(column.Collation)));
                    }
                    if (column.DefaultSql is not null)
                    {
                        attributes.Add(Attribute(
                            "defaultSqlDigest",
                            SqlDigest(column.DefaultSql)));
                    }

                    AddObject(MigrationNormalizedSchemaContract.CreateObject(
                        columnId,
                        MigrationObjectKind.Column,
                        tableId,
                        columnName,
                        attributes));
                }

                result.Add(new TableShape(
                    schema,
                    normalizedName,
                    tableId,
                    columnIds));
            }

            return result;
        }

        private void CaptureKeysAndChecks(
            IReadOnlyList<TableShape> tables)
        {
            foreach (TableShape table in tables)
            {
                _cancellationToken.ThrowIfCancellationRequested();
                var keys = table.Schema.KeyConstraints
                    .Select(key => key ?? throw new InvalidDataException())
                    .ToList();
                if (!keys.Any(key =>
                        key.Kind == KeyConstraintKind.PrimaryKey))
                {
                    string[] legacyPrimaryKeyColumns = table.Schema.Columns
                        .Where(column => column.IsPrimaryKey)
                        .Select(column => column.Name)
                        .ToArray();
                    if (legacyPrimaryKeyColumns.Length > 0)
                    {
                        keys.Add(new KeyConstraintDefinition
                        {
                            ConstraintName = null,
                            Kind = KeyConstraintKind.PrimaryKey,
                            Columns = legacyPrimaryKeyColumns,
                        });
                    }
                }

                foreach (KeyConstraintDefinition key in keys)
                {
                    _cancellationToken.ThrowIfCancellationRequested();
                    TrackInput(key.ConstraintName);
                    if (key.Columns.Count == 0)
                        throw new InvalidDataException();

                    string[] normalizedColumns = key.Columns
                        .Select(column =>
                        {
                            TrackInput(column);
                            return NormalizeIdentifier(column);
                        })
                        .ToArray();
                    EnsureDistinct(normalizedColumns);
                    string? normalizedName = key.ConstraintName is null
                        ? null
                        : NormalizeIdentifier(key.ConstraintName);
                    string kind = key.Kind switch
                    {
                        KeyConstraintKind.PrimaryKey => "primary",
                        KeyConstraintKind.Unique => "unique",
                        _ => throw new InvalidDataException(),
                    };
                    string keyId = CreateObjectId(
                        MigrationObjectKind.Key,
                        [
                            table.NormalizedName,
                            normalizedName ?? UnnamedObject,
                            kind,
                            .. normalizedColumns,
                        ]);

                    AddObject(MigrationNormalizedSchemaContract.CreateObject(
                        keyId,
                        MigrationObjectKind.Key,
                        table.ObjectId,
                        normalizedName ?? UnnamedObject,
                        [Attribute("kind", kind)],
                        normalizedColumns.Select((column, ordinal) =>
                            new MigrationNormalizedSchemaMember
                            {
                                Role =
                                    MigrationObjectReferenceRoles.Column,
                                Ordinal = ordinal,
                                ObjectId = ResolveColumnId(table, column),
                            }).ToArray()));
                }

                foreach (CheckConstraintDefinition check in
                             table.Schema.CheckConstraints.Select(check =>
                                 check ?? throw new InvalidDataException()))
                {
                    _cancellationToken.ThrowIfCancellationRequested();
                    TrackInput(
                        check.ConstraintName,
                        check.ExpressionSql,
                        check.ColumnName);
                    string sqlDigest = SqlDigest(check.ExpressionSql);
                    string? normalizedName =
                        check.ConstraintName is null
                            ? null
                            : NormalizeIdentifier(check.ConstraintName);
                    string checkId = CreateObjectId(
                        MigrationObjectKind.CheckConstraint,
                        table.NormalizedName,
                        normalizedName ?? UnnamedObject,
                        sqlDigest);
                    MigrationNormalizedSchemaMember[] members =
                        check.ColumnName is null
                            ? []
                            :
                            [
                                new MigrationNormalizedSchemaMember
                                {
                                    Role =
                                        MigrationObjectReferenceRoles.Column,
                                    Ordinal = 0,
                                    ObjectId = ResolveColumnId(
                                        table,
                                        NormalizeIdentifier(
                                            check.ColumnName)),
                                },
                            ];

                    AddObject(MigrationNormalizedSchemaContract.CreateObject(
                        checkId,
                        MigrationObjectKind.CheckConstraint,
                        table.ObjectId,
                        normalizedName ?? UnnamedObject,
                        [Attribute("targetSqlDigest", sqlDigest)],
                        members));
                }
            }
        }

        private void CaptureForeignKeys(
            IReadOnlyList<TableShape> tables,
            IReadOnlyDictionary<string, TableShape> tablesByName)
        {
            foreach (TableShape table in tables)
            {
                foreach (ForeignKeyDefinition foreignKey in
                             table.Schema.ForeignKeys.Select(foreignKey =>
                                 foreignKey ??
                                 throw new InvalidDataException()))
                {
                    _cancellationToken.ThrowIfCancellationRequested();
                    TrackInput(
                        foreignKey.ConstraintName,
                        foreignKey.ColumnName,
                        foreignKey.ReferencedTableName,
                        foreignKey.ReferencedColumnName);
                    IReadOnlyList<string> sourceColumns =
                        foreignKey.ColumnNames.Count > 0
                            ? foreignKey.ColumnNames
                            : [foreignKey.ColumnName];
                    IReadOnlyList<string> referencedColumns =
                        foreignKey.ReferencedColumnNames.Count > 0
                            ? foreignKey.ReferencedColumnNames
                            : [foreignKey.ReferencedColumnName];
                    if (sourceColumns.Count == 0 ||
                        sourceColumns.Count != referencedColumns.Count)
                    {
                        throw new InvalidDataException();
                    }

                    string[] normalizedSourceColumns = sourceColumns
                        .Select(column =>
                        {
                            TrackInput(column);
                            return NormalizeIdentifier(column);
                        })
                        .ToArray();
                    string[] normalizedReferencedColumns =
                        referencedColumns
                            .Select(column =>
                            {
                                TrackInput(column);
                                return NormalizeIdentifier(column);
                            })
                            .ToArray();
                    EnsureDistinct(normalizedSourceColumns);
                    EnsureDistinct(normalizedReferencedColumns);

                    string referencedTableName = NormalizeIdentifier(
                        foreignKey.ReferencedTableName);
                    if (!tablesByName.TryGetValue(
                            referencedTableName,
                            out TableShape? referencedTable))
                    {
                        throw new InvalidDataException();
                    }

                    string? normalizedName =
                        string.IsNullOrWhiteSpace(
                            foreignKey.ConstraintName)
                            ? null
                            : NormalizeIdentifier(
                                foreignKey.ConstraintName);
                    string foreignKeyId = CreateObjectId(
                        MigrationObjectKind.ForeignKey,
                        [
                            table.NormalizedName,
                            normalizedName ?? UnnamedObject,
                            referencedTableName,
                            .. normalizedSourceColumns,
                            .. normalizedReferencedColumns,
                        ]);
                    var members =
                        new List<MigrationNormalizedSchemaMember>(
                            1 + (sourceColumns.Count * 2))
                        {
                            new()
                            {
                                Role = ReferencedTableRole,
                                Ordinal = 0,
                                ObjectId = referencedTable.ObjectId,
                            },
                        };
                    members.AddRange(normalizedSourceColumns.Select(
                        (column, ordinal) =>
                            new MigrationNormalizedSchemaMember
                            {
                                Role =
                                    MigrationObjectReferenceRoles
                                        .SourceColumn,
                                Ordinal = ordinal,
                                ObjectId = ResolveColumnId(
                                    table,
                                    column),
                            }));
                    members.AddRange(
                        normalizedReferencedColumns.Select(
                            (column, ordinal) =>
                                new MigrationNormalizedSchemaMember
                                {
                                    Role = ReferencedColumnRole,
                                    Ordinal = ordinal,
                                    ObjectId = ResolveColumnId(
                                        referencedTable,
                                        column),
                                }));

                    AddObject(MigrationNormalizedSchemaContract.CreateObject(
                        foreignKeyId,
                        MigrationObjectKind.ForeignKey,
                        table.ObjectId,
                        normalizedName ?? UnnamedObject,
                        [
                            Attribute(
                                "onDelete",
                                foreignKey.OnDelete switch
                                {
                                    ForeignKeyOnDeleteAction.Restrict =>
                                        "restrict",
                                    ForeignKeyOnDeleteAction.Cascade =>
                                        "cascade",
                                    _ => throw new InvalidDataException(),
                                }),
                            Attribute("onUpdate", "restrict"),
                        ],
                        members));
                }
            }
        }

        private void CaptureIndexes(
            IReadOnlyDictionary<string, TableShape> tablesByName)
        {
            foreach (IndexSchema index in _connection.GetIndexes()
                         .Where(index => index.Kind == IndexKind.Sql))
            {
                _cancellationToken.ThrowIfCancellationRequested();
                TrackInput(
                    index.IndexName,
                    index.TableName,
                    index.OwnerIndexName,
                    index.OptionsJson);
                string tableName = NormalizeIdentifier(index.TableName);
                if (IsHistoryTable(tableName))
                    continue;
                if (!tablesByName.TryGetValue(
                        tableName,
                        out TableShape? table) ||
                    index.Columns.Count == 0)
                {
                    throw new InvalidDataException();
                }

                string indexName = NormalizeIdentifier(index.IndexName);
                string indexId = CreateObjectId(
                    MigrationObjectKind.Index,
                    tableName,
                    indexName);
                var attributes =
                    new List<MigrationNormalizedSchemaAttribute>
                    {
                        Attribute(
                            "unique",
                            BooleanToken(index.IsUnique)),
                    };
                var members =
                    new MigrationNormalizedSchemaMember[
                        index.Columns.Count];
                for (int ordinal = 0;
                     ordinal < index.Columns.Count;
                     ordinal++)
                {
                    string columnName = index.Columns[ordinal];
                    TrackInput(columnName);
                    string normalizedColumn =
                        NormalizeIdentifier(columnName);
                    members[ordinal] =
                        new MigrationNormalizedSchemaMember
                        {
                            Role =
                                MigrationObjectReferenceRoles.Column,
                            Ordinal = ordinal,
                            ObjectId = ResolveColumnId(
                                table,
                                normalizedColumn),
                        };

                    string? indexCollation =
                        ordinal < index.ColumnCollations.Count
                            ? index.ColumnCollations[ordinal]
                            : null;
                    TrackInput(indexCollation);
                    string? effectiveCollation =
                        indexCollation ??
                        table.Schema.Columns.Single(column =>
                            string.Equals(
                                NormalizeIdentifier(column.Name),
                                normalizedColumn,
                                StringComparison.Ordinal)).Collation;
                    attributes.Add(Attribute(
                        $"collation.{ordinal:D6}",
                        effectiveCollation is null
                            ? "none"
                            : NormalizeToken(effectiveCollation)));
                }

                AddObject(MigrationNormalizedSchemaContract.CreateObject(
                    indexId,
                    MigrationObjectKind.Index,
                    table.ObjectId,
                    indexName,
                    attributes,
                    members));
            }
        }

        private void CaptureViews()
        {
            foreach (string viewName in _connection.GetViewNames())
            {
                _cancellationToken.ThrowIfCancellationRequested();
                TrackInput(viewName);
                string sql = _connection.GetViewSql(viewName) ??
                    throw new InvalidDataException();
                TrackInput(sql);
                string normalizedName = NormalizeIdentifier(viewName);
                string viewId = CreateObjectId(
                    MigrationObjectKind.View,
                    normalizedName);
                AddObject(MigrationNormalizedSchemaContract.CreateObject(
                    viewId,
                    MigrationObjectKind.View,
                    parentObjectId: null,
                    normalizedName,
                    [Attribute("targetSqlDigest", SqlDigest(sql))]));
            }
        }

        private void CaptureTriggers(
            IReadOnlyDictionary<string, TableShape> tablesByName)
        {
            foreach (TriggerSchema trigger in _connection.GetTriggers())
            {
                _cancellationToken.ThrowIfCancellationRequested();
                TrackInput(
                    trigger.TriggerName,
                    trigger.TableName,
                    trigger.BodySql);
                string tableName =
                    NormalizeIdentifier(trigger.TableName);
                if (IsHistoryTable(tableName))
                    continue;
                if (!tablesByName.TryGetValue(
                        tableName,
                        out TableShape? table))
                {
                    throw new InvalidDataException();
                }

                string triggerName =
                    NormalizeIdentifier(trigger.TriggerName);
                string triggerId = CreateObjectId(
                    MigrationObjectKind.Trigger,
                    tableName,
                    triggerName);
                AddObject(MigrationNormalizedSchemaContract.CreateObject(
                    triggerId,
                    MigrationObjectKind.Trigger,
                    table.ObjectId,
                    triggerName,
                    [
                        Attribute(
                            "timing",
                            trigger.Timing.ToString()
                                .ToLowerInvariant()),
                        Attribute(
                            "event",
                            trigger.Event.ToString()
                                .ToLowerInvariant()),
                        Attribute(
                            "bodySqlDigest",
                            SqlDigest(trigger.BodySql)),
                    ]));
            }
        }

        private void AddObject(
            MigrationNormalizedSchemaObject definition)
        {
            _cancellationToken.ThrowIfCancellationRequested();
            if (_objects.Count >= _limits.MaxObjects)
                throw new CaptureLimitException();
            _objects.Add(definition);
        }

        private void TrackInput(params string?[] values)
        {
            foreach (string? value in values)
            {
                if (value is null)
                    continue;
                int remaining = _limits.MaxInputBytes - _inputBytes;
                if (value.Length > remaining)
                    throw new CaptureLimitException();
                int byteCount = Encoding.UTF8.GetByteCount(value);
                if (byteCount > remaining)
                    throw new CaptureLimitException();
                _inputBytes += byteCount;
            }
        }

        private static string ResolveColumnId(
            TableShape table,
            string normalizedColumnName) =>
            table.ColumnIds.TryGetValue(
                normalizedColumnName,
                out string? columnId)
                ? columnId
                : throw new InvalidDataException();

    }

    private sealed record TableShape(
        TableSchema Schema,
        string NormalizedName,
        string ObjectId,
        IReadOnlyDictionary<string, string> ColumnIds);

    private sealed class CaptureLimitException : Exception;

    private static MigrationNormalizedSchemaAttribute Attribute(
        string name,
        string value) =>
        new()
        {
            Name = name,
            Value = value,
        };

    private static string BooleanToken(bool value) =>
        value ? "true" : "false";

    private static string NormalizeStoreType(
        CSharpDB.Primitives.DbType type) =>
        type.ToString().ToUpperInvariant();

    private static string NormalizeIdentifier(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
            throw new InvalidDataException();
        return value.Normalize(NormalizationForm.FormC).ToUpperInvariant();
    }

    private static string NormalizeToken(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
            throw new InvalidDataException();
        return value.Trim()
            .Normalize(NormalizationForm.FormC)
            .ToUpperInvariant();
    }

    private static bool IsHistoryTable(string normalizedName) =>
        string.Equals(
            normalizedName,
            NormalizeIdentifier(HistoryTableName),
            StringComparison.Ordinal);

    private static void EnsureDistinct(
        IReadOnlyList<string> values)
    {
        if (values.Distinct(StringComparer.Ordinal).Count() != values.Count)
            throw new InvalidDataException();
    }

    private static string CreateObjectId(
        MigrationObjectKind kind,
        params ReadOnlySpan<string> components)
    {
        using IncrementalHash hash =
            IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        AppendHashString(hash, ObjectIdDomain);
        AppendHashString(hash, kind.ToString());
        foreach (string component in components)
            AppendHashString(hash, component);
        string digest = Convert.ToHexString(
                hash.GetHashAndReset())
            .ToLowerInvariant();
        return $"ef-scratch:{kind.ToString().ToLowerInvariant()}:{digest}";
    }

    private static void AppendHashString(
        IncrementalHash hash,
        string value)
    {
        byte[] bytes = Encoding.UTF8.GetBytes(value);
        Span<byte> length = stackalloc byte[sizeof(int)];
        System.Buffers.Binary.BinaryPrimitives.WriteInt32BigEndian(
            length,
            bytes.Length);
        hash.AppendData(length);
        hash.AppendData(bytes);
    }

    private static string SqlDigest(string sql)
    {
        string normalized = sql
            .Replace("\r\n", "\n", StringComparison.Ordinal)
            .Replace('\r', '\n')
            .Trim();
        return Convert.ToHexString(
                SHA256.HashData(
                    Encoding.UTF8.GetBytes(normalized)))
            .ToLowerInvariant();
    }
}
