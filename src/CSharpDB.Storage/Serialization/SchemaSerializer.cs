using System.Text;
using CSharpDB.Primitives;

namespace CSharpDB.Storage.Serialization;

/// <summary>
/// Serializes/deserializes TableSchema to/from bytes for storage in the catalog B+tree.
/// Format: [nameLen:varint][nameUtf8][colCount:varint] then per column:
///   [nameLen:varint][nameUtf8][type:1][flags:1
///    (bit0=nullable, bit1=isPK, bit2=isIdentity, bit3=isRowVersion)]
/// </summary>
public static class SchemaSerializer
{
    private const byte NullableFlag = 0x01;
    private const byte PrimaryKeyFlag = 0x02;
    private const byte IdentityFlag = 0x04;
    private const byte RowVersionFlag = 0x08;
    private const ulong TableMetadataVersion = 9;
    private const ulong TableMetadataVersionWithCollations = 1;
    private const ulong TableMetadataVersionWithForeignKeys = 2;
    private const ulong TableMetadataVersionWithDefaultsAndChecks = 3;
    private const ulong TableMetadataVersionWithLogicalKeys = 4;
    private const ulong TableMetadataVersionWithOrderedForeignKeys = 5;
    private const ulong TableMetadataVersionWithRowVersion = 6;
    private const ulong TableMetadataVersionWithStableIdentities = 7;
    private const ulong TableMetadataVersionWithStableForeignKeyBindings = 8;
    private const ulong TableMetadataVersionWithForeignKeyUpdateActions = 9;
    private const ulong IndexMetadataVersion = 1;
    private const int MaximumSchemaCollectionCount = 65_536;
    private const int MaximumSchemaPayloadBytes = 64 * 1024 * 1024;

    public static byte[] Serialize(TableSchema schema)
    {
        ValidateSchemaForSerialization(schema);
        ValidateForeignKeyBindingsForSerialization(schema.ForeignKeys);

        using var ms = new SchemaPayloadStream(MaximumSchemaPayloadBytes);
        WriteString(ms, schema.TableName);
        WriteVarint(ms, (ulong)schema.Columns.Count);

        foreach (var col in schema.Columns)
        {
            WriteString(ms, col.Name);
            ms.WriteByte((byte)col.Type);
            byte flags = 0;
            if (col.Nullable) flags |= NullableFlag;
            if (col.IsPrimaryKey) flags |= PrimaryKeyFlag;
            if (col.IsIdentity) flags |= IdentityFlag;
            if (col.IsRowVersion) flags |= RowVersionFlag;
            ms.WriteByte(flags);
        }

        // Optional trailing metadata for forward-compatible schema evolution.
        // 0 means unknown/uninitialized (legacy compatibility path).
        ulong nextRowId = schema.NextRowId > 0 ? (ulong)schema.NextRowId : 0UL;
        WriteVarint(ms, nextRowId);
        WriteVarint(ms, TableMetadataVersion);
        WriteVarint(ms, (ulong)schema.Columns.Count);
        foreach (var col in schema.Columns)
            WriteNullableString(ms, col.Collation);
        WriteVarint(ms, (ulong)schema.ForeignKeys.Count);
        foreach (var foreignKey in schema.ForeignKeys)
        {
            WriteString(ms, foreignKey.ConstraintName);
            WriteString(ms, foreignKey.ColumnName);
            WriteString(ms, foreignKey.ReferencedTableName);
            WriteString(ms, foreignKey.ReferencedColumnName);
            WriteVarint(ms, (ulong)foreignKey.OnDelete);
            WriteString(ms, foreignKey.SupportingIndexName);
        }
        WriteVarint(ms, (ulong)schema.Columns.Count);
        foreach (var column in schema.Columns)
            WriteNullableString(ms, column.DefaultSql);
        WriteVarint(ms, (ulong)schema.CheckConstraints.Count);
        foreach (var checkConstraint in schema.CheckConstraints)
        {
            WriteNullableString(ms, checkConstraint.ConstraintName);
            WriteString(ms, checkConstraint.ExpressionSql);
            WriteNullableString(ms, checkConstraint.ColumnName);
        }
        WriteVarint(ms, (ulong)schema.KeyConstraints.Count);
        foreach (var keyConstraint in schema.KeyConstraints)
        {
            WriteNullableString(ms, keyConstraint.ConstraintName);
            WriteVarint(ms, (ulong)keyConstraint.Kind);
            WriteVarint(ms, (ulong)keyConstraint.Columns.Count);
            foreach (string columnName in keyConstraint.Columns)
                WriteString(ms, columnName);
            WriteNullableString(ms, keyConstraint.BackingIndexName);
        }
        WriteVarint(ms, (ulong)schema.ForeignKeys.Count);
        foreach (ForeignKeyDefinition foreignKey in schema.ForeignKeys)
        {
            IReadOnlyList<string> columnNames = foreignKey.ColumnNames.Count > 0
                ? foreignKey.ColumnNames
                : [foreignKey.ColumnName];
            IReadOnlyList<string> referencedColumnNames = foreignKey.ReferencedColumnNames.Count > 0
                ? foreignKey.ReferencedColumnNames
                : [foreignKey.ReferencedColumnName];
            WriteVarint(ms, (ulong)columnNames.Count);
            foreach (string columnName in columnNames)
                WriteString(ms, columnName);
            WriteVarint(ms, (ulong)referencedColumnNames.Count);
            foreach (string columnName in referencedColumnNames)
                WriteString(ms, columnName);
        }
        Guid tableId = schema.SchemaId != Guid.Empty
            ? schema.SchemaId
            : SchemaIdentity.ForLegacyTable(schema.TableName);
        WriteGuid(ms, tableId);
        WriteVarint(ms, (ulong)schema.Columns.Count);
        for (int i = 0; i < schema.Columns.Count; i++)
        {
            ColumnDefinition column = schema.Columns[i];
            WriteGuid(
                ms,
                column.SchemaId != Guid.Empty
                    ? column.SchemaId
                    : SchemaIdentity.ForLegacyColumn(tableId, column.Name, i));
        }
        WriteVarint(ms, (ulong)schema.ForeignKeys.Count);
        for (int i = 0; i < schema.ForeignKeys.Count; i++)
        {
            ForeignKeyDefinition constraint = schema.ForeignKeys[i];
            WriteGuid(
                ms,
                constraint.SchemaId != Guid.Empty
                    ? constraint.SchemaId
                    : SchemaIdentity.ForLegacyConstraint(
                        tableId,
                        "foreign-key",
                        constraint.ConstraintName,
                        i));
        }
        WriteVarint(ms, (ulong)schema.CheckConstraints.Count);
        for (int i = 0; i < schema.CheckConstraints.Count; i++)
        {
            CheckConstraintDefinition constraint = schema.CheckConstraints[i];
            WriteGuid(
                ms,
                constraint.SchemaId != Guid.Empty
                    ? constraint.SchemaId
                    : SchemaIdentity.ForLegacyConstraint(
                        tableId,
                        "check",
                        constraint.ConstraintName,
                        i));
        }
        WriteVarint(ms, (ulong)schema.KeyConstraints.Count);
        for (int i = 0; i < schema.KeyConstraints.Count; i++)
        {
            KeyConstraintDefinition constraint = schema.KeyConstraints[i];
            WriteGuid(
                ms,
                constraint.SchemaId != Guid.Empty
                    ? constraint.SchemaId
                    : SchemaIdentity.ForLegacyConstraint(
                        tableId,
                        "key",
                        constraint.ConstraintName,
                        i));
        }
        WriteVarint(ms, (ulong)schema.ForeignKeys.Count);
        foreach (ForeignKeyDefinition foreignKey in schema.ForeignKeys)
        {
            WriteGuid(ms, foreignKey.ReferencedTableSchemaId);
            WriteGuidList(ms, foreignKey.ColumnSchemaIds);
            WriteGuidList(ms, foreignKey.ReferencedColumnSchemaIds);
            WriteGuid(ms, foreignKey.ReferencedKeySchemaId);
        }
        WriteVarint(ms, (ulong)schema.ForeignKeys.Count);
        foreach (ForeignKeyDefinition foreignKey in schema.ForeignKeys)
            WriteVarint(ms, (ulong)foreignKey.OnUpdate);

        return ms.ToArray();
    }

    public static TableSchema Deserialize(ReadOnlySpan<byte> data)
    {
        if (data.Length > MaximumSchemaPayloadBytes)
        {
            throw new InvalidDataException(
                $"Table schema payload length '{data.Length}' exceeds the supported maximum of {MaximumSchemaPayloadBytes} bytes.");
        }

        try
        {
            return DeserializeTableCore(data);
        }
        catch (InvalidDataException)
        {
            throw;
        }
        catch (Exception ex) when (
            ex is ArgumentOutOfRangeException or
                IndexOutOfRangeException or
                OverflowException)
        {
            throw new InvalidDataException(
                "Table schema payload is truncated or malformed.",
                ex);
        }
    }

    private static TableSchema DeserializeTableCore(ReadOnlySpan<byte> data)
    {
        int pos = 0;
        string tableName = ReadString(data, ref pos);
        int colCount = ReadCount(
            data,
            ref pos,
            "column",
            minimumBytesPerItem: 3);

        var columnNames = new string[colCount];
        var columnTypes = new DbType[colCount];
        var columnFlags = new byte[colCount];
        for (int i = 0; i < colCount; i++)
        {
            columnNames[i] = ReadString(data, ref pos);
            DbType columnType = (DbType)data[pos++];
            if (!Enum.IsDefined(columnType))
            {
                throw new InvalidDataException(
                    $"Unsupported column type '{(byte)columnType}'.");
            }
            columnTypes[i] = columnType;
            columnFlags[i] = data[pos++];
        }

        long nextRowId = 0;
        var columnCollations = new string?[colCount];
        var columnDefaults = new string?[colCount];
        ForeignKeyDefinition[] foreignKeys = Array.Empty<ForeignKeyDefinition>();
        CheckConstraintDefinition[] checkConstraints = Array.Empty<CheckConstraintDefinition>();
        KeyConstraintDefinition[] keyConstraints = Array.Empty<KeyConstraintDefinition>();
        Guid tableId = SchemaIdentity.ForLegacyTable(tableName);
        Guid[] columnIds = new Guid[colCount];
        for (int i = 0; i < colCount; i++)
            columnIds[i] = SchemaIdentity.ForLegacyColumn(tableId, columnNames[i], i);
        Guid[] foreignKeyIds = Array.Empty<Guid>();
        Guid[] checkConstraintIds = Array.Empty<Guid>();
        Guid[] keyConstraintIds = Array.Empty<Guid>();
        Guid[] referencedTableIds = Array.Empty<Guid>();
        Guid[][] foreignKeyColumnIds = Array.Empty<Guid[]>();
        Guid[][] referencedColumnIds = Array.Empty<Guid[]>();
        Guid[] referencedKeyIds = Array.Empty<Guid>();
        ulong metadataVersion = 0;
        if (pos < data.Length)
        {
            ulong storedNextRowId = ReadVarint(data, ref pos, "next row identity");
            if (storedNextRowId <= long.MaxValue)
                nextRowId = (long)storedNextRowId;
        }

        if (pos < data.Length)
        {
            metadataVersion = ReadVarint(data, ref pos, "table metadata version");

            if (metadataVersion is not (
                    TableMetadataVersionWithCollations or
                    TableMetadataVersionWithForeignKeys or
                    TableMetadataVersionWithDefaultsAndChecks or
                    TableMetadataVersionWithLogicalKeys or
                    TableMetadataVersionWithOrderedForeignKeys or
                    TableMetadataVersionWithRowVersion or
                    TableMetadataVersionWithStableIdentities or
                    TableMetadataVersionWithStableForeignKeyBindings or
                    TableMetadataVersionWithForeignKeyUpdateActions))
                throw new InvalidDataException($"Unsupported table schema metadata version '{metadataVersion}'.");

            int metadataColumnCount = ReadCount(data, ref pos, "table metadata column");
            if (metadataColumnCount != colCount)
                throw new InvalidDataException($"Table schema metadata column count '{metadataColumnCount}' does not match schema column count '{colCount}'.");

            for (int i = 0; i < metadataColumnCount; i++)
                columnCollations[i] = ReadNullableString(data, ref pos);

            if (metadataVersion >= TableMetadataVersionWithForeignKeys)
            {
                int foreignKeyCount = ReadCount(
                    data,
                    ref pos,
                    "foreign key",
                    minimumBytesPerItem: 6);

                foreignKeys = new ForeignKeyDefinition[foreignKeyCount];
                for (int i = 0; i < foreignKeyCount; i++)
                {
                    string constraintName = ReadString(data, ref pos);
                    string columnName = ReadString(data, ref pos);
                    string referencedTableName = ReadString(data, ref pos);
                    string referencedColumnName = ReadString(data, ref pos);
                    ulong onDeleteRaw = ReadVarint(data, ref pos, "foreign key ON DELETE action");
                    string supportingIndexName = ReadString(data, ref pos);

                    if (onDeleteRaw > int.MaxValue)
                        throw new InvalidDataException($"Unsupported foreign key ON DELETE action '{onDeleteRaw}'.");
                    var onDelete = (ForeignKeyOnDeleteAction)(int)onDeleteRaw;
                    bool supportsAction =
                        metadataVersion >=
                            TableMetadataVersionWithForeignKeyUpdateActions
                            ? Enum.IsDefined(onDelete)
                            : onDelete is
                                ForeignKeyOnDeleteAction.Restrict or
                                ForeignKeyOnDeleteAction.Cascade;
                    if (!supportsAction)
                        throw new InvalidDataException($"Unsupported foreign key ON DELETE action '{onDeleteRaw}'.");

                    foreignKeys[i] = new ForeignKeyDefinition
                    {
                        ConstraintName = constraintName,
                        ColumnName = columnName,
                        ReferencedTableName = referencedTableName,
                        ReferencedColumnName = referencedColumnName,
                        ColumnNames = [columnName],
                        ReferencedColumnNames = [referencedColumnName],
                        OnDelete = onDelete,
                        SupportingIndexName = supportingIndexName,
                    };
                }
            }

            if (metadataVersion >= TableMetadataVersionWithDefaultsAndChecks)
            {
                int defaultColumnCount = ReadCount(data, ref pos, "default metadata column");
                if (defaultColumnCount != colCount)
                {
                    throw new InvalidDataException(
                        $"Table schema default metadata column count '{defaultColumnCount}' does not match schema column count '{colCount}'.");
                }

                for (int i = 0; i < defaultColumnCount; i++)
                    columnDefaults[i] = ReadNullableString(data, ref pos);

                int checkConstraintCount = ReadCount(
                    data,
                    ref pos,
                    "check constraint",
                    minimumBytesPerItem: 3);
                checkConstraints = new CheckConstraintDefinition[checkConstraintCount];
                for (int i = 0; i < checkConstraintCount; i++)
                {
                    checkConstraints[i] = new CheckConstraintDefinition
                    {
                        ConstraintName = ReadNullableString(data, ref pos),
                        ExpressionSql = ReadString(data, ref pos),
                        ColumnName = ReadNullableString(data, ref pos),
                    };
                }
            }

            if (metadataVersion >= TableMetadataVersionWithLogicalKeys)
            {
                int keyConstraintCount = ReadCount(
                    data,
                    ref pos,
                    "key constraint",
                    minimumBytesPerItem: 5);
                keyConstraints = new KeyConstraintDefinition[keyConstraintCount];
                for (int i = 0; i < keyConstraintCount; i++)
                {
                    string? constraintName = ReadNullableString(data, ref pos);
                    ulong kindRaw = ReadVarint(data, ref pos, "key constraint kind");
                    if (kindRaw > int.MaxValue)
                        throw new InvalidDataException($"Unsupported key constraint kind '{kindRaw}'.");
                    var kind = (KeyConstraintKind)(int)kindRaw;
                    if (!Enum.IsDefined(kind))
                        throw new InvalidDataException($"Unsupported key constraint kind '{kindRaw}'.");

                    int keyColumnCount = ReadCount(
                        data,
                        ref pos,
                        "key constraint column",
                        minimumBytesPerItem: 1);
                    if (keyColumnCount == 0)
                        throw new InvalidDataException("Persisted key constraint must contain at least one column.");

                    var keyColumns = new string[keyColumnCount];
                    for (int columnIndex = 0; columnIndex < keyColumnCount; columnIndex++)
                        keyColumns[columnIndex] = ReadString(data, ref pos);

                    keyConstraints[i] = new KeyConstraintDefinition
                    {
                        ConstraintName = constraintName,
                        Kind = kind,
                        Columns = keyColumns,
                        BackingIndexName = ReadNullableString(data, ref pos),
                    };
                }
            }

            if (metadataVersion >= TableMetadataVersionWithOrderedForeignKeys)
            {
                int orderedForeignKeyCount = ReadCount(
                    data,
                    ref pos,
                    "ordered foreign key");
                if (orderedForeignKeyCount != foreignKeys.Length)
                {
                    throw new InvalidDataException(
                        $"Ordered foreign key metadata count '{orderedForeignKeyCount}' does not match foreign key count '{foreignKeys.Length}'.");
                }

                for (int i = 0; i < orderedForeignKeyCount; i++)
                {
                    int columnCount = ReadCount(
                        data,
                        ref pos,
                        "ordered foreign key child column",
                        minimumBytesPerItem: 1);
                    if (columnCount == 0)
                        throw new InvalidDataException("Persisted foreign key must contain at least one child column.");
                    var orderedChildColumnNames = new string[columnCount];
                    for (int columnIndex = 0; columnIndex < columnCount; columnIndex++)
                        orderedChildColumnNames[columnIndex] = ReadString(data, ref pos);

                    int referencedColumnCount = ReadCount(
                        data,
                        ref pos,
                        "ordered foreign key referenced column",
                        minimumBytesPerItem: 1);
                    if (referencedColumnCount != columnCount)
                    {
                        throw new InvalidDataException(
                            "Persisted foreign key child and referenced column counts must match.");
                    }
                    var referencedColumnNames = new string[referencedColumnCount];
                    for (int columnIndex = 0; columnIndex < referencedColumnCount; columnIndex++)
                        referencedColumnNames[columnIndex] = ReadString(data, ref pos);

                    ForeignKeyDefinition scalar = foreignKeys[i];
                    if (!string.Equals(
                            scalar.ColumnName,
                            orderedChildColumnNames[0],
                            StringComparison.OrdinalIgnoreCase) ||
                        !string.Equals(
                            scalar.ReferencedColumnName,
                            referencedColumnNames[0],
                            StringComparison.OrdinalIgnoreCase))
                    {
                        throw new InvalidDataException(
                            "Ordered foreign key metadata does not match its scalar compatibility fields.");
                    }

                    foreignKeys[i] = new ForeignKeyDefinition
                    {
                        ConstraintName = scalar.ConstraintName,
                        ColumnName = scalar.ColumnName,
                        ReferencedTableName = scalar.ReferencedTableName,
                        ReferencedColumnName = scalar.ReferencedColumnName,
                        ColumnNames = orderedChildColumnNames,
                        ReferencedColumnNames = referencedColumnNames,
                        OnDelete = scalar.OnDelete,
                        OnUpdate = scalar.OnUpdate,
                        SupportingIndexName = scalar.SupportingIndexName,
                    };
                }
            }

            if (metadataVersion >= TableMetadataVersionWithStableIdentities)
            {
                tableId = ReadGuid(data, ref pos);
                columnIds = ReadIdentityList(
                    data,
                    ref pos,
                    colCount,
                    "column");
                foreignKeyIds = ReadIdentityList(
                    data,
                    ref pos,
                    foreignKeys.Length,
                    "foreign key");
                checkConstraintIds = ReadIdentityList(
                    data,
                    ref pos,
                    checkConstraints.Length,
                    "check constraint");
                keyConstraintIds = ReadIdentityList(
                    data,
                    ref pos,
                    keyConstraints.Length,
                    "key constraint");
            }

            if (metadataVersion >= TableMetadataVersionWithStableForeignKeyBindings)
            {
                int bindingCount = ReadCount(
                    data,
                    ref pos,
                    "stable foreign key binding");
                if (bindingCount != foreignKeys.Length)
                {
                    throw new InvalidDataException(
                        $"Stable foreign key binding count '{bindingCount}' does not match foreign key count '{foreignKeys.Length}'.");
                }

                referencedTableIds = new Guid[bindingCount];
                foreignKeyColumnIds = new Guid[bindingCount][];
                referencedColumnIds = new Guid[bindingCount][];
                referencedKeyIds = new Guid[bindingCount];
                for (int i = 0; i < bindingCount; i++)
                {
                    referencedTableIds[i] = ReadOptionalGuid(data, ref pos);
                    int foreignKeyArity = foreignKeys[i].ColumnNames.Count;
                    foreignKeyColumnIds[i] = ReadGuidList(
                        data,
                        ref pos,
                        foreignKeyArity,
                        "child column");
                    referencedColumnIds[i] = ReadGuidList(
                        data,
                        ref pos,
                        foreignKeyArity,
                        "referenced column");
                    referencedKeyIds[i] = ReadOptionalGuid(data, ref pos);

                    // Early v8 maintenance rewrites could persist a child-side
                    // binding before its referenced table had been recreated.
                    // That shape used valid child IDs plus an empty target and
                    // all-zero referenced IDs. Treat it as an absent legacy
                    // binding so the catalog can hydrate it by name.
                    bool isLegacyUnresolvedBindingCandidate =
                        referencedTableIds[i] == Guid.Empty &&
                        referencedKeyIds[i] == Guid.Empty &&
                        referencedColumnIds[i].All(static identity =>
                            identity == Guid.Empty);
                    if (isLegacyUnresolvedBindingCandidate)
                    {
                        bool bindingListsAreAbsent =
                            foreignKeyColumnIds[i].Length == 0 &&
                            referencedColumnIds[i].Length == 0;
                        bool isHistoricalUnresolvedBinding =
                            referencedColumnIds[i].Length ==
                                foreignKeyArity &&
                            ForeignKeyChildBindingMatchesColumns(
                                foreignKeys[i],
                                columnNames,
                                columnIds,
                                foreignKeyColumnIds[i]);
                        if (!bindingListsAreAbsent &&
                            !isHistoricalUnresolvedBinding)
                        {
                            throw new InvalidDataException(
                                $"Legacy unresolved foreign key binding '{foreignKeys[i].ConstraintName}' contains child column identities that do not match its named columns.");
                        }

                        foreignKeyColumnIds[i] = [];
                        referencedColumnIds[i] = [];
                    }
                }
            }

            if (metadataVersion >=
                TableMetadataVersionWithForeignKeyUpdateActions)
            {
                int updateActionCount = ReadCount(
                    data,
                    ref pos,
                    "foreign key ON UPDATE action");
                if (updateActionCount != foreignKeys.Length)
                {
                    throw new InvalidDataException(
                        $"Foreign key ON UPDATE action count '{updateActionCount}' does not match foreign key count '{foreignKeys.Length}'.");
                }

                for (int i = 0; i < updateActionCount; i++)
                {
                    ulong onUpdateRaw = ReadVarint(
                        data,
                        ref pos,
                        "foreign key ON UPDATE action");
                    if (onUpdateRaw > int.MaxValue)
                    {
                        throw new InvalidDataException(
                            $"Unsupported foreign key ON UPDATE action '{onUpdateRaw}'.");
                    }

                    var onUpdate =
                        (ForeignKeyOnDeleteAction)(int)onUpdateRaw;
                    if (!Enum.IsDefined(onUpdate))
                    {
                        throw new InvalidDataException(
                            $"Unsupported foreign key ON UPDATE action '{onUpdateRaw}'.");
                    }

                    foreignKeys[i] = CloneWithOnUpdate(
                        foreignKeys[i],
                        onUpdate);
                }
            }

            if (pos != data.Length)
            {
                throw new InvalidDataException(
                    $"Table schema metadata version '{metadataVersion}' contains {data.Length - pos} trailing byte(s).");
            }
        }

        if (foreignKeyIds.Length == 0)
        {
            foreignKeyIds = new Guid[foreignKeys.Length];
            for (int i = 0; i < foreignKeys.Length; i++)
                foreignKeyIds[i] = SchemaIdentity.ForLegacyConstraint(
                    tableId,
                    "foreign-key",
                    foreignKeys[i].ConstraintName,
                    i);
        }
        if (checkConstraintIds.Length == 0)
        {
            checkConstraintIds = new Guid[checkConstraints.Length];
            for (int i = 0; i < checkConstraints.Length; i++)
                checkConstraintIds[i] = SchemaIdentity.ForLegacyConstraint(
                    tableId,
                    "check",
                    checkConstraints[i].ConstraintName,
                    i);
        }
        if (keyConstraintIds.Length == 0)
        {
            keyConstraintIds = new Guid[keyConstraints.Length];
            for (int i = 0; i < keyConstraints.Length; i++)
                keyConstraintIds[i] = SchemaIdentity.ForLegacyConstraint(
                    tableId,
                    "key",
                    keyConstraints[i].ConstraintName,
                    i);
        }

        for (int i = 0; i < foreignKeys.Length; i++)
        {
            foreignKeys[i] = CloneWithSchemaIdAndBindings(
                foreignKeys[i],
                foreignKeyIds[i],
                referencedTableIds.Length > i ? referencedTableIds[i] : Guid.Empty,
                foreignKeyColumnIds.Length > i ? foreignKeyColumnIds[i] : [],
                referencedColumnIds.Length > i ? referencedColumnIds[i] : [],
                referencedKeyIds.Length > i ? referencedKeyIds[i] : Guid.Empty);
        }
        ValidateForeignKeyBindingsForSerialization(foreignKeys);
        for (int i = 0; i < checkConstraints.Length; i++)
            checkConstraints[i] = CloneWithSchemaId(checkConstraints[i], checkConstraintIds[i]);
        for (int i = 0; i < keyConstraints.Length; i++)
            keyConstraints[i] = CloneWithSchemaId(keyConstraints[i], keyConstraintIds[i]);

        var columns = new ColumnDefinition[colCount];
        for (int i = 0; i < colCount; i++)
        {
            byte flags = columnFlags[i];
            DbType type = columnTypes[i];
            bool isPrimaryKey = (flags & PrimaryKeyFlag) != 0;
            bool hasIdentityFlag = (flags & IdentityFlag) != 0;
            columns[i] = new ColumnDefinition
            {
                SchemaId = columnIds[i],
                Name = columnNames[i],
                Type = type,
                Nullable = (flags & NullableFlag) != 0,
                IsPrimaryKey = isPrimaryKey,
                // Backward compatibility: historical INTEGER PRIMARY KEY behavior auto-generated rowid.
                // Logical-key metadata makes an explicit distinction between a physical
                // single INTEGER key and composite INTEGER key components.
                IsIdentity = hasIdentityFlag ||
                    (metadataVersion < TableMetadataVersionWithLogicalKeys &&
                     isPrimaryKey &&
                     type == DbType.Integer),
                IsRowVersion =
                    metadataVersion >= TableMetadataVersionWithRowVersion &&
                    (flags & RowVersionFlag) != 0,
                Collation = columnCollations[i],
                DefaultSql = columnDefaults[i],
            };
        }

        return new TableSchema
        {
            SchemaId = tableId,
            TableName = tableName,
            Columns = columns,
            NextRowId = nextRowId,
            ForeignKeys = foreignKeys,
            CheckConstraints = checkConstraints,
            KeyConstraints = keyConstraints,
        };
    }

    private static void ValidateSchemaForSerialization(TableSchema schema)
    {
        ArgumentNullException.ThrowIfNull(schema);

        if (schema.Columns is null ||
            schema.ForeignKeys is null ||
            schema.CheckConstraints is null ||
            schema.KeyConstraints is null)
        {
            throw new InvalidDataException(
                "Table schema collections cannot be null.");
        }

        ValidateCollectionCountForSerialization(
            schema.Columns.Count,
            "column");
        ValidateCollectionCountForSerialization(
            schema.ForeignKeys.Count,
            "foreign key");
        ValidateCollectionCountForSerialization(
            schema.CheckConstraints.Count,
            "check constraint");
        ValidateCollectionCountForSerialization(
            schema.KeyConstraints.Count,
            "key constraint");

        foreach (ColumnDefinition column in schema.Columns)
        {
            if (!Enum.IsDefined(column.Type))
            {
                throw new InvalidDataException(
                    $"Unsupported column type '{(byte)column.Type}'.");
            }
        }

        foreach (ForeignKeyDefinition foreignKey in schema.ForeignKeys)
        {
            if (!Enum.IsDefined(foreignKey.OnDelete))
            {
                throw new InvalidDataException(
                    $"Unsupported foreign key ON DELETE action '{(int)foreignKey.OnDelete}'.");
            }
            if (!Enum.IsDefined(foreignKey.OnUpdate))
            {
                throw new InvalidDataException(
                    $"Unsupported foreign key ON UPDATE action '{(int)foreignKey.OnUpdate}'.");
            }

            if (foreignKey.ColumnNames is not null)
            {
                ValidateCollectionCountForSerialization(
                    foreignKey.ColumnNames.Count,
                    "ordered foreign key child column");
            }
            if (foreignKey.ReferencedColumnNames is not null)
            {
                ValidateCollectionCountForSerialization(
                    foreignKey.ReferencedColumnNames.Count,
                    "ordered foreign key referenced column");
            }
            if (foreignKey.ColumnSchemaIds is not null)
            {
                ValidateCollectionCountForSerialization(
                    foreignKey.ColumnSchemaIds.Count,
                    "stable foreign key child column");
            }
            if (foreignKey.ReferencedColumnSchemaIds is not null)
            {
                ValidateCollectionCountForSerialization(
                    foreignKey.ReferencedColumnSchemaIds.Count,
                    "stable foreign key referenced column");
            }
        }

        foreach (KeyConstraintDefinition keyConstraint in schema.KeyConstraints)
        {
            if (!Enum.IsDefined(keyConstraint.Kind))
            {
                throw new InvalidDataException(
                    $"Unsupported key constraint kind '{(int)keyConstraint.Kind}'.");
            }
            if (keyConstraint.Columns is null)
            {
                throw new InvalidDataException(
                    "Key constraint column list cannot be null.");
            }

            ValidateCollectionCountForSerialization(
                keyConstraint.Columns.Count,
                "key constraint column");
        }
    }

    private static void ValidateCollectionCountForSerialization(
        int count,
        string valueKind)
    {
        if (count < 0 || count > MaximumSchemaCollectionCount)
        {
            throw new InvalidDataException(
                $"{valueKind} count '{count}' exceeds the supported maximum of {MaximumSchemaCollectionCount}.");
        }
    }

    private static void WriteGuid(Stream stream, Guid value) =>
        stream.Write(value.ToByteArray());

    private static void WriteGuidList(
        MemoryStream stream,
        IReadOnlyList<Guid> values)
    {
        if (values is null)
            throw new InvalidDataException("Stable schema binding list cannot be null.");

        WriteVarint(stream, (ulong)values.Count);
        foreach (Guid value in values)
            WriteGuid(stream, value);
    }

    private static void ValidateForeignKeyBindingsForSerialization(
        IReadOnlyList<ForeignKeyDefinition> foreignKeys)
    {
        foreach (ForeignKeyDefinition foreignKey in foreignKeys)
        {
            if (foreignKey.ColumnNames is null ||
                foreignKey.ReferencedColumnNames is null)
            {
                throw new InvalidDataException(
                    $"Foreign key '{foreignKey.ConstraintName}' ordered column lists cannot be null.");
            }

            int childArity = foreignKey.ColumnNames.Count > 0
                ? foreignKey.ColumnNames.Count
                : 1;
            int referencedArity = foreignKey.ReferencedColumnNames.Count > 0
                ? foreignKey.ReferencedColumnNames.Count
                : 1;
            if (childArity != referencedArity)
            {
                throw new InvalidDataException(
                    $"Foreign key '{foreignKey.ConstraintName}' child and referenced column counts must match.");
            }

            IReadOnlyList<Guid>? childBindings = foreignKey.ColumnSchemaIds;
            IReadOnlyList<Guid>? referencedBindings =
                foreignKey.ReferencedColumnSchemaIds;
            if (childBindings is null || referencedBindings is null)
            {
                throw new InvalidDataException(
                    $"Foreign key '{foreignKey.ConstraintName}' stable binding lists cannot be null.");
            }

            if (childBindings.Count != 0 && childBindings.Count != childArity)
            {
                throw new InvalidDataException(
                    $"Foreign key '{foreignKey.ConstraintName}' child identity count '{childBindings.Count}' does not match ordered foreign key arity '{childArity}'.");
            }
            if (referencedBindings.Count != 0 &&
                referencedBindings.Count != referencedArity)
            {
                throw new InvalidDataException(
                    $"Foreign key '{foreignKey.ConstraintName}' referenced identity count '{referencedBindings.Count}' does not match ordered foreign key arity '{referencedArity}'.");
            }

            bool hasAnyBinding =
                foreignKey.ReferencedTableSchemaId != Guid.Empty ||
                foreignKey.ReferencedKeySchemaId != Guid.Empty ||
                childBindings.Count != 0 ||
                referencedBindings.Count != 0;
            bool hasCompleteBinding =
                foreignKey.ReferencedTableSchemaId != Guid.Empty &&
                childBindings.Count == childArity &&
                referencedBindings.Count == referencedArity;
            if (hasAnyBinding && !hasCompleteBinding)
            {
                throw new InvalidDataException(
                    $"Foreign key '{foreignKey.ConstraintName}' stable bindings must be either complete or absent.");
            }

            if (childBindings.Any(identity => identity == Guid.Empty) ||
                referencedBindings.Any(identity => identity == Guid.Empty))
            {
                throw new InvalidDataException(
                    $"Foreign key '{foreignKey.ConstraintName}' stable column identities cannot be empty.");
            }
        }
    }

    private static bool ForeignKeyChildBindingMatchesColumns(
        ForeignKeyDefinition foreignKey,
        IReadOnlyList<string> columnNames,
        IReadOnlyList<Guid> columnIds,
        IReadOnlyList<Guid> childBindingIds)
    {
        if (foreignKey.ColumnNames.Count != childBindingIds.Count)
            return false;

        for (int bindingIndex = 0;
             bindingIndex < foreignKey.ColumnNames.Count;
             bindingIndex++)
        {
            string childColumnName = foreignKey.ColumnNames[bindingIndex];
            int matchingColumnIndex = -1;
            for (int columnIndex = 0;
                 columnIndex < columnNames.Count;
                 columnIndex++)
            {
                if (!string.Equals(
                        columnNames[columnIndex],
                        childColumnName,
                        StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                if (matchingColumnIndex >= 0)
                    return false;

                matchingColumnIndex = columnIndex;
            }

            if (matchingColumnIndex < 0 ||
                childBindingIds[bindingIndex] != columnIds[matchingColumnIndex])
            {
                return false;
            }
        }

        return true;
    }

    private static Guid ReadGuid(ReadOnlySpan<byte> data, ref int pos)
    {
        const int GuidLength = 16;
        if (data.Length - pos < GuidLength)
            throw new InvalidDataException("Truncated stable schema identity.");

        Guid value = new(data.Slice(pos, GuidLength));
        pos += GuidLength;
        if (value == Guid.Empty)
            throw new InvalidDataException("Persisted stable schema identity cannot be empty.");
        return value;
    }

    private static Guid ReadOptionalGuid(ReadOnlySpan<byte> data, ref int pos)
    {
        const int GuidLength = 16;
        if (data.Length - pos < GuidLength)
            throw new InvalidDataException("Truncated stable schema binding.");

        Guid value = new(data.Slice(pos, GuidLength));
        pos += GuidLength;
        return value;
    }

    private static Guid[] ReadGuidList(
        ReadOnlySpan<byte> data,
        ref int pos,
        int expectedArity,
        string bindingKind)
    {
        int count = ReadCount(data, ref pos, $"stable foreign key {bindingKind}");
        if (count != 0 && count != expectedArity)
        {
            throw new InvalidDataException(
                $"Stable foreign key {bindingKind} binding count '{count}' does not match ordered foreign key arity '{expectedArity}'.");
        }

        const int GuidLength = 16;
        if (count > (data.Length - pos) / GuidLength)
        {
            throw new InvalidDataException(
                $"Stable foreign key {bindingKind} bindings are truncated.");
        }

        var values = new Guid[count];
        for (int i = 0; i < count; i++)
            values[i] = ReadOptionalGuid(data, ref pos);
        return values;
    }

    private static Guid[] ReadIdentityList(
        ReadOnlySpan<byte> data,
        ref int pos,
        int expectedCount,
        string objectKind)
    {
        int count = ReadCount(data, ref pos, $"stable {objectKind} identity");
        if (count != expectedCount)
        {
            throw new InvalidDataException(
                $"Stable {objectKind} identity count '{count}' does not match metadata count '{expectedCount}'.");
        }

        const int GuidLength = 16;
        if (count > (data.Length - pos) / GuidLength)
            throw new InvalidDataException($"Stable {objectKind} identities are truncated.");

        var identities = new Guid[count];
        for (int i = 0; i < count; i++)
            identities[i] = ReadGuid(data, ref pos);
        return identities;
    }

    private static int ReadCount(
        ReadOnlySpan<byte> data,
        ref int pos,
        string valueKind,
        int minimumBytesPerItem = 0)
    {
        ulong rawCount = ReadVarint(data, ref pos, $"{valueKind} count");
        if (rawCount > int.MaxValue)
        {
            throw new InvalidDataException(
                $"{valueKind} count '{rawCount}' exceeds the supported maximum.");
        }

        int count = (int)rawCount;
        if (count > MaximumSchemaCollectionCount)
        {
            throw new InvalidDataException(
                $"{valueKind} count '{count}' exceeds the supported maximum of {MaximumSchemaCollectionCount}.");
        }
        if (minimumBytesPerItem > 0 &&
            count > (data.Length - pos) / minimumBytesPerItem)
        {
            throw new InvalidDataException(
                $"{valueKind} count '{count}' exceeds the remaining schema payload.");
        }

        return count;
    }

    private static ForeignKeyDefinition CloneWithSchemaIdAndBindings(
        ForeignKeyDefinition value,
        Guid schemaId,
        Guid referencedTableSchemaId,
        IReadOnlyList<Guid> columnSchemaIds,
        IReadOnlyList<Guid> referencedColumnSchemaIds,
        Guid referencedKeySchemaId) =>
        new()
        {
            SchemaId = schemaId,
            ColumnSchemaIds = columnSchemaIds,
            ReferencedTableSchemaId = referencedTableSchemaId,
            ReferencedColumnSchemaIds = referencedColumnSchemaIds,
            ReferencedKeySchemaId = referencedKeySchemaId,
            ConstraintName = value.ConstraintName,
            ColumnName = value.ColumnName,
            ReferencedTableName = value.ReferencedTableName,
            ReferencedColumnName = value.ReferencedColumnName,
            ColumnNames = value.ColumnNames,
            ReferencedColumnNames = value.ReferencedColumnNames,
            OnDelete = value.OnDelete,
            OnUpdate = value.OnUpdate,
            SupportingIndexName = value.SupportingIndexName,
        };

    private static ForeignKeyDefinition CloneWithOnUpdate(
        ForeignKeyDefinition value,
        ForeignKeyOnDeleteAction onUpdate) =>
        new()
        {
            SchemaId = value.SchemaId,
            ColumnSchemaIds = value.ColumnSchemaIds,
            ReferencedTableSchemaId = value.ReferencedTableSchemaId,
            ReferencedColumnSchemaIds = value.ReferencedColumnSchemaIds,
            ReferencedKeySchemaId = value.ReferencedKeySchemaId,
            ConstraintName = value.ConstraintName,
            ColumnName = value.ColumnName,
            ReferencedTableName = value.ReferencedTableName,
            ReferencedColumnName = value.ReferencedColumnName,
            ColumnNames = value.ColumnNames,
            ReferencedColumnNames = value.ReferencedColumnNames,
            OnDelete = value.OnDelete,
            OnUpdate = onUpdate,
            SupportingIndexName = value.SupportingIndexName,
        };

    private static CheckConstraintDefinition CloneWithSchemaId(
        CheckConstraintDefinition value,
        Guid schemaId) =>
        new()
        {
            SchemaId = schemaId,
            ConstraintName = value.ConstraintName,
            ExpressionSql = value.ExpressionSql,
            ColumnName = value.ColumnName,
        };

    private static KeyConstraintDefinition CloneWithSchemaId(
        KeyConstraintDefinition value,
        Guid schemaId) =>
        new()
        {
            SchemaId = schemaId,
            ConstraintName = value.ConstraintName,
            Kind = value.Kind,
            Columns = value.Columns,
            BackingIndexName = value.BackingIndexName,
        };

    public static byte[] SerializeIndex(IndexSchema index)
    {
        var ms = new MemoryStream();
        string?[] columnCollations = NormalizeColumnCollations(index.Columns.Count, index.ColumnCollations);
        var nameBytes = Encoding.UTF8.GetBytes(index.IndexName);
        WriteVarint(ms, (ulong)nameBytes.Length);
        ms.Write(nameBytes);

        var tableNameBytes = Encoding.UTF8.GetBytes(index.TableName);
        WriteVarint(ms, (ulong)tableNameBytes.Length);
        ms.Write(tableNameBytes);

        WriteVarint(ms, (ulong)index.Columns.Count);
        foreach (var col in index.Columns)
        {
            var colBytes = Encoding.UTF8.GetBytes(col);
            WriteVarint(ms, (ulong)colBytes.Length);
            ms.Write(colBytes);
        }

        ms.WriteByte(index.IsUnique ? (byte)1 : (byte)0);
        ms.WriteByte((byte)index.Kind);
        ms.WriteByte((byte)index.State);
        WriteNullableString(ms, index.OwnerIndexName);
        WriteNullableString(ms, index.OptionsJson);
        WriteVarint(ms, IndexMetadataVersion);
        WriteVarint(ms, (ulong)columnCollations.Length);
        foreach (string? columnCollation in columnCollations)
            WriteNullableString(ms, columnCollation);
        return ms.ToArray();
    }

    public static IndexSchema DeserializeIndex(ReadOnlySpan<byte> data)
    {
        int pos = 0;
        int nameLen = (int)Varint.Read(data[pos..], out int nb);
        pos += nb;
        string indexName = Encoding.UTF8.GetString(data.Slice(pos, nameLen));
        pos += nameLen;

        int tableNameLen = (int)Varint.Read(data[pos..], out int tnb);
        pos += tnb;
        string tableName = Encoding.UTF8.GetString(data.Slice(pos, tableNameLen));
        pos += tableNameLen;

        int colCount = (int)Varint.Read(data[pos..], out int cb);
        pos += cb;

        var columns = new string[colCount];
        for (int i = 0; i < colCount; i++)
        {
            int colLen = (int)Varint.Read(data[pos..], out int clb);
            pos += clb;
            columns[i] = Encoding.UTF8.GetString(data.Slice(pos, colLen));
            pos += colLen;
        }

        bool isUnique = data[pos] != 0;
        pos++;

        IndexKind kind = IndexKind.Sql;
        IndexState state = IndexState.Ready;
        string? ownerIndexName = null;
        string? optionsJson = null;
        string?[] columnCollations = Array.Empty<string?>();

        if (pos < data.Length)
            kind = (IndexKind)data[pos++];

        if (pos < data.Length)
            state = (IndexState)data[pos++];

        if (pos < data.Length)
            ownerIndexName = ReadNullableString(data, ref pos);

        if (pos < data.Length)
            optionsJson = ReadNullableString(data, ref pos);

        if (pos < data.Length)
        {
            ulong metadataVersion = Varint.Read(data[pos..], out int metadataBytesRead);
            pos += metadataBytesRead;

            if (metadataVersion != IndexMetadataVersion)
                throw new InvalidDataException($"Unsupported index schema metadata version '{metadataVersion}'.");

            int metadataColumnCount = (int)Varint.Read(data[pos..], out int metadataCountBytesRead);
            pos += metadataCountBytesRead;
            if (metadataColumnCount != columns.Length)
                throw new InvalidDataException($"Index schema metadata column count '{metadataColumnCount}' does not match index column count '{columns.Length}'.");

            columnCollations = new string?[metadataColumnCount];
            for (int i = 0; i < metadataColumnCount; i++)
                columnCollations[i] = ReadNullableString(data, ref pos);
        }

        return new IndexSchema
        {
            IndexName = indexName,
            TableName = tableName,
            Columns = columns,
            ColumnCollations = columnCollations,
            IsUnique = isUnique,
            Kind = kind,
            State = state,
            OwnerIndexName = ownerIndexName,
            OptionsJson = optionsJson,
        };
    }

    /// <summary>
    /// Serialize a table name as a B+tree key (simple UTF-8 encoding mapped to a long hash for rowid-keyed tree).
    /// For the catalog, we use a simple sequential ID approach instead.
    /// </summary>
    public static long TableNameToKey(string tableName)
    {
        // Use a stable hash. For MVP, a simple deterministic hash.
        long hash = 0;
        foreach (char c in tableName.ToLowerInvariant())
            hash = hash * 31 + c;
        return hash & 0x7FFFFFFFFFFFFFFF; // ensure positive
    }

    /// <summary>
    /// Hash index name to a B+tree key. Uses a different multiplier than table names to reduce collisions.
    /// </summary>
    public static long IndexNameToKey(string indexName)
    {
        long hash = 0;
        foreach (char c in indexName.ToLowerInvariant())
            hash = hash * 37 + c;
        return hash & 0x7FFFFFFFFFFFFFFF;
    }

    /// <summary>
    /// Hash view name to a B+tree key. Uses a different multiplier to reduce collisions.
    /// </summary>
    public static long ViewNameToKey(string viewName)
    {
        long hash = 0;
        foreach (char c in viewName.ToLowerInvariant())
            hash = hash * 41 + c;
        return hash & 0x7FFFFFFFFFFFFFFF;
    }

    /// <summary>
    /// Hash trigger name to a B+tree key.
    /// </summary>
    public static long TriggerNameToKey(string triggerName)
    {
        long hash = 0;
        foreach (char c in triggerName.ToLowerInvariant())
            hash = hash * 43 + c;
        return hash & 0x7FFFFFFFFFFFFFFF;
    }

    /// <summary>
    /// Serialize a TriggerSchema.
    /// Format: [nameLen:4][name][tableLen:4][table][timing:1][event:1][bodyLen:4][body]
    /// </summary>
    public static byte[] SerializeTrigger(TriggerSchema trigger)
    {
        var nameBytes = Encoding.UTF8.GetBytes(trigger.TriggerName);
        var tableBytes = Encoding.UTF8.GetBytes(trigger.TableName);
        var bodyBytes = Encoding.UTF8.GetBytes(trigger.BodySql);

        var result = new byte[4 + nameBytes.Length + 4 + tableBytes.Length + 1 + 1 + 4 + bodyBytes.Length];
        int pos = 0;

        BitConverter.TryWriteBytes(result.AsSpan(pos), nameBytes.Length); pos += 4;
        nameBytes.CopyTo(result.AsSpan(pos)); pos += nameBytes.Length;

        BitConverter.TryWriteBytes(result.AsSpan(pos), tableBytes.Length); pos += 4;
        tableBytes.CopyTo(result.AsSpan(pos)); pos += tableBytes.Length;

        result[pos++] = (byte)trigger.Timing;
        result[pos++] = (byte)trigger.Event;

        BitConverter.TryWriteBytes(result.AsSpan(pos), bodyBytes.Length); pos += 4;
        bodyBytes.CopyTo(result.AsSpan(pos));

        return result;
    }

    public static TriggerSchema DeserializeTrigger(ReadOnlySpan<byte> data)
    {
        int pos = 0;

        int nameLen = BitConverter.ToInt32(data.Slice(pos, 4)); pos += 4;
        string triggerName = Encoding.UTF8.GetString(data.Slice(pos, nameLen)); pos += nameLen;

        int tableLen = BitConverter.ToInt32(data.Slice(pos, 4)); pos += 4;
        string tableName = Encoding.UTF8.GetString(data.Slice(pos, tableLen)); pos += tableLen;

        var timing = (TriggerTiming)data[pos++];
        var evt = (TriggerEvent)data[pos++];

        int bodyLen = BitConverter.ToInt32(data.Slice(pos, 4)); pos += 4;
        string bodySql = Encoding.UTF8.GetString(data.Slice(pos, bodyLen));

        return new TriggerSchema
        {
            TriggerName = triggerName,
            TableName = tableName,
            Timing = timing,
            Event = evt,
            BodySql = bodySql,
        };
    }

    private static void WriteVarint(MemoryStream ms, ulong value)
    {
        Span<byte> buf = stackalloc byte[10];
        int len = Varint.Write(buf, value);
        ms.Write(buf[..len]);
    }

    private static string?[] NormalizeColumnCollations(int columnCount, IReadOnlyList<string?> columnCollations)
    {
        if (columnCount == 0)
            return Array.Empty<string?>();

        if (columnCollations.Count == 0)
            return new string?[columnCount];

        if (columnCollations.Count > columnCount)
            throw new InvalidDataException($"Index collation metadata count '{columnCollations.Count}' exceeds index column count '{columnCount}'.");

        var normalized = new string?[columnCount];
        for (int i = 0; i < columnCollations.Count; i++)
            normalized[i] = columnCollations[i];

        return normalized;
    }

    private static void WriteNullableString(MemoryStream ms, string? value)
    {
        if (value == null)
        {
            WriteVarint(ms, 0);
            return;
        }

        int byteCount = Encoding.UTF8.GetByteCount(value);
        ulong encodedLength = checked((ulong)byteCount + 1);
        EnsureSchemaPayloadCapacity(
            ms,
            checked(GetVarintLength(encodedLength) + byteCount));
        byte[] bytes = Encoding.UTF8.GetBytes(value);
        WriteVarint(ms, encodedLength);
        ms.Write(bytes);
    }

    private static void WriteString(MemoryStream ms, string value)
    {
        int byteCount = Encoding.UTF8.GetByteCount(value);
        EnsureSchemaPayloadCapacity(
            ms,
            checked(GetVarintLength((ulong)byteCount) + byteCount));
        byte[] bytes = Encoding.UTF8.GetBytes(value);
        WriteVarint(ms, (ulong)bytes.Length);
        ms.Write(bytes);
    }

    private static int GetVarintLength(ulong value)
    {
        int length = 1;
        while (value >= 0x80)
        {
            value >>= 7;
            length++;
        }

        return length;
    }

    private static void EnsureSchemaPayloadCapacity(
        MemoryStream stream,
        int additionalLength)
    {
        if (stream is SchemaPayloadStream schemaStream)
            schemaStream.EnsureAdditionalCapacity(additionalLength);
    }

    private static string? ReadNullableString(ReadOnlySpan<byte> data, ref int pos)
    {
        ulong encodedLength = ReadVarint(data, ref pos, "nullable string length");

        if (encodedLength == 0)
            return null;

        if (encodedLength - 1 > int.MaxValue)
            throw new InvalidDataException($"String length '{encodedLength - 1}' exceeds the supported maximum.");

        int length = (int)(encodedLength - 1);
        if (length > data.Length - pos)
            throw new InvalidDataException("Nullable string value is truncated.");

        string value = Encoding.UTF8.GetString(data.Slice(pos, length));
        pos += length;
        return value;
    }

    private static string ReadString(ReadOnlySpan<byte> data, ref int pos)
    {
        ulong rawLength = ReadVarint(data, ref pos, "string length");
        if (rawLength > int.MaxValue)
            throw new InvalidDataException($"String length '{rawLength}' exceeds the supported maximum.");

        int length = (int)rawLength;
        if (length > data.Length - pos)
            throw new InvalidDataException("String value is truncated.");

        string value = Encoding.UTF8.GetString(data.Slice(pos, length));
        pos += length;
        return value;
    }

    private static ulong ReadVarint(
        ReadOnlySpan<byte> data,
        ref int pos,
        string valueKind)
    {
        ulong result = 0;
        for (int byteIndex = 0; byteIndex < 10; byteIndex++)
        {
            if ((uint)pos >= (uint)data.Length)
                throw new InvalidDataException($"Truncated {valueKind}.");

            byte current = data[pos++];
            if (byteIndex == 9 && (current & 0xFE) != 0)
                throw new InvalidDataException($"Malformed {valueKind}.");

            result |= (ulong)(current & 0x7F) << (byteIndex * 7);
            if ((current & 0x80) == 0)
                return result;
        }

        throw new InvalidDataException($"Malformed {valueKind}.");
    }

    private sealed class SchemaPayloadStream(int maximumLength) : MemoryStream
    {
        public override void Write(
            byte[] buffer,
            int offset,
            int count)
        {
            EnsureAdditionalCapacity(count);
            base.Write(buffer, offset, count);
        }

        public override void Write(ReadOnlySpan<byte> buffer)
        {
            EnsureAdditionalCapacity(buffer.Length);
            base.Write(buffer);
        }

        public override void WriteByte(byte value)
        {
            EnsureAdditionalCapacity(1);
            base.WriteByte(value);
        }

        internal void EnsureAdditionalCapacity(int additionalLength)
        {
            if (additionalLength < 0 ||
                additionalLength > maximumLength - Length)
            {
                long attemptedLength = Length + additionalLength;
                throw new InvalidDataException(
                    $"Table schema payload length '{attemptedLength}' exceeds the supported maximum of {maximumLength} bytes.");
            }
        }
    }
}
