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
    private const ulong TableMetadataVersion = 8;
    private const ulong TableMetadataVersionWithCollations = 1;
    private const ulong TableMetadataVersionWithForeignKeys = 2;
    private const ulong TableMetadataVersionWithDefaultsAndChecks = 3;
    private const ulong TableMetadataVersionWithLogicalKeys = 4;
    private const ulong TableMetadataVersionWithOrderedForeignKeys = 5;
    private const ulong TableMetadataVersionWithRowVersion = 6;
    private const ulong TableMetadataVersionWithStableIdentities = 7;
    private const ulong TableMetadataVersionWithStableForeignKeyBindings = 8;
    private const ulong IndexMetadataVersion = 1;

    public static byte[] Serialize(TableSchema schema)
    {
        var ms = new MemoryStream();
        var nameBytes = Encoding.UTF8.GetBytes(schema.TableName);
        WriteVarint(ms, (ulong)nameBytes.Length);
        ms.Write(nameBytes);
        WriteVarint(ms, (ulong)schema.Columns.Count);

        foreach (var col in schema.Columns)
        {
            var colNameBytes = Encoding.UTF8.GetBytes(col.Name);
            WriteVarint(ms, (ulong)colNameBytes.Length);
            ms.Write(colNameBytes);
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

        return ms.ToArray();
    }

    public static TableSchema Deserialize(ReadOnlySpan<byte> data)
    {
        int pos = 0;
        int nameLen = (int)Varint.Read(data[pos..], out int nb);
        pos += nb;
        string tableName = Encoding.UTF8.GetString(data.Slice(pos, nameLen));
        pos += nameLen;

        int colCount = (int)Varint.Read(data[pos..], out int cb);
        pos += cb;

        var columnNames = new string[colCount];
        var columnTypes = new DbType[colCount];
        var columnFlags = new byte[colCount];
        for (int i = 0; i < colCount; i++)
        {
            int colNameLen = (int)Varint.Read(data[pos..], out int cnb);
            pos += cnb;
            columnNames[i] = Encoding.UTF8.GetString(data.Slice(pos, colNameLen));
            pos += colNameLen;
            columnTypes[i] = (DbType)data[pos++];
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
            ulong storedNextRowId = Varint.Read(data[pos..], out int nextRowIdBytesRead);
            pos += nextRowIdBytesRead;
            if (storedNextRowId <= long.MaxValue)
                nextRowId = (long)storedNextRowId;
        }

        if (pos < data.Length)
        {
            metadataVersion = Varint.Read(data[pos..], out int metadataBytesRead);
            pos += metadataBytesRead;

            if (metadataVersion is not (
                    TableMetadataVersionWithCollations or
                    TableMetadataVersionWithForeignKeys or
                    TableMetadataVersionWithDefaultsAndChecks or
                    TableMetadataVersionWithLogicalKeys or
                    TableMetadataVersionWithOrderedForeignKeys or
                    TableMetadataVersionWithRowVersion or
                    TableMetadataVersionWithStableIdentities or
                    TableMetadataVersion))
                throw new InvalidDataException($"Unsupported table schema metadata version '{metadataVersion}'.");

            int metadataColumnCount = (int)Varint.Read(data[pos..], out int metadataCountBytesRead);
            pos += metadataCountBytesRead;
            if (metadataColumnCount != colCount)
                throw new InvalidDataException($"Table schema metadata column count '{metadataColumnCount}' does not match schema column count '{colCount}'.");

            for (int i = 0; i < metadataColumnCount; i++)
                columnCollations[i] = ReadNullableString(data, ref pos);

            if (metadataVersion >= TableMetadataVersionWithForeignKeys && pos < data.Length)
            {
                int foreignKeyCount = (int)Varint.Read(data[pos..], out int foreignKeyCountBytesRead);
                pos += foreignKeyCountBytesRead;

                foreignKeys = new ForeignKeyDefinition[foreignKeyCount];
                for (int i = 0; i < foreignKeyCount; i++)
                {
                    string constraintName = ReadString(data, ref pos);
                    string columnName = ReadString(data, ref pos);
                    string referencedTableName = ReadString(data, ref pos);
                    string referencedColumnName = ReadString(data, ref pos);
                    ulong onDeleteRaw = Varint.Read(data[pos..], out int onDeleteBytesRead);
                    pos += onDeleteBytesRead;
                    string supportingIndexName = ReadString(data, ref pos);

                    if (!Enum.IsDefined(typeof(ForeignKeyOnDeleteAction), (int)onDeleteRaw))
                        throw new InvalidDataException($"Unsupported foreign key ON DELETE action '{onDeleteRaw}'.");

                    foreignKeys[i] = new ForeignKeyDefinition
                    {
                        ConstraintName = constraintName,
                        ColumnName = columnName,
                        ReferencedTableName = referencedTableName,
                        ReferencedColumnName = referencedColumnName,
                        ColumnNames = [columnName],
                        ReferencedColumnNames = [referencedColumnName],
                        OnDelete = (ForeignKeyOnDeleteAction)onDeleteRaw,
                        SupportingIndexName = supportingIndexName,
                    };
                }
            }

            if (metadataVersion >= TableMetadataVersionWithDefaultsAndChecks && pos < data.Length)
            {
                int defaultColumnCount = checked((int)Varint.Read(data[pos..], out int defaultCountBytesRead));
                pos += defaultCountBytesRead;
                if (defaultColumnCount != colCount)
                {
                    throw new InvalidDataException(
                        $"Table schema default metadata column count '{defaultColumnCount}' does not match schema column count '{colCount}'.");
                }

                for (int i = 0; i < defaultColumnCount; i++)
                    columnDefaults[i] = ReadNullableString(data, ref pos);

                int checkConstraintCount = checked((int)Varint.Read(data[pos..], out int checkCountBytesRead));
                pos += checkCountBytesRead;
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

            if (metadataVersion >= TableMetadataVersionWithLogicalKeys && pos < data.Length)
            {
                int keyConstraintCount = checked((int)Varint.Read(data[pos..], out int keyCountBytesRead));
                pos += keyCountBytesRead;
                keyConstraints = new KeyConstraintDefinition[keyConstraintCount];
                for (int i = 0; i < keyConstraintCount; i++)
                {
                    string? constraintName = ReadNullableString(data, ref pos);
                    ulong kindRaw = Varint.Read(data[pos..], out int kindBytesRead);
                    pos += kindBytesRead;
                    if (!Enum.IsDefined(typeof(KeyConstraintKind), (int)kindRaw))
                        throw new InvalidDataException($"Unsupported key constraint kind '{kindRaw}'.");

                    int keyColumnCount = checked((int)Varint.Read(data[pos..], out int keyColumnCountBytesRead));
                    pos += keyColumnCountBytesRead;
                    if (keyColumnCount == 0)
                        throw new InvalidDataException("Persisted key constraint must contain at least one column.");

                    var keyColumns = new string[keyColumnCount];
                    for (int columnIndex = 0; columnIndex < keyColumnCount; columnIndex++)
                        keyColumns[columnIndex] = ReadString(data, ref pos);

                    keyConstraints[i] = new KeyConstraintDefinition
                    {
                        ConstraintName = constraintName,
                        Kind = (KeyConstraintKind)kindRaw,
                        Columns = keyColumns,
                        BackingIndexName = ReadNullableString(data, ref pos),
                    };
                }
            }

            if (metadataVersion >= TableMetadataVersionWithOrderedForeignKeys && pos < data.Length)
            {
                int orderedForeignKeyCount = checked((int)Varint.Read(
                    data[pos..],
                    out int orderedForeignKeyCountBytesRead));
                pos += orderedForeignKeyCountBytesRead;
                if (orderedForeignKeyCount != foreignKeys.Length)
                {
                    throw new InvalidDataException(
                        $"Ordered foreign key metadata count '{orderedForeignKeyCount}' does not match foreign key count '{foreignKeys.Length}'.");
                }

                for (int i = 0; i < orderedForeignKeyCount; i++)
                {
                    int columnCount = checked((int)Varint.Read(data[pos..], out int columnCountBytesRead));
                    pos += columnCountBytesRead;
                    if (columnCount == 0)
                        throw new InvalidDataException("Persisted foreign key must contain at least one child column.");
                    var orderedChildColumnNames = new string[columnCount];
                    for (int columnIndex = 0; columnIndex < columnCount; columnIndex++)
                        orderedChildColumnNames[columnIndex] = ReadString(data, ref pos);

                    int referencedColumnCount = checked((int)Varint.Read(
                        data[pos..],
                        out int referencedColumnCountBytesRead));
                    pos += referencedColumnCountBytesRead;
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
                        SupportingIndexName = scalar.SupportingIndexName,
                    };
                }
            }

            if (metadataVersion >= TableMetadataVersionWithStableIdentities && pos < data.Length)
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

            if (metadataVersion >= TableMetadataVersionWithStableForeignKeyBindings &&
                pos < data.Length)
            {
                int bindingCount = checked((int)Varint.Read(
                    data[pos..],
                    out int bindingCountBytesRead));
                pos += bindingCountBytesRead;
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
                    foreignKeyColumnIds[i] = ReadGuidList(data, ref pos);
                    referencedColumnIds[i] = ReadGuidList(data, ref pos);
                    referencedKeyIds[i] = ReadOptionalGuid(data, ref pos);
                }
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

    private static void WriteGuid(Stream stream, Guid value) =>
        stream.Write(value.ToByteArray());

    private static void WriteGuidList(
        MemoryStream stream,
        IReadOnlyList<Guid> values)
    {
        WriteVarint(stream, (ulong)values.Count);
        foreach (Guid value in values)
            WriteGuid(stream, value);
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

    private static Guid[] ReadGuidList(ReadOnlySpan<byte> data, ref int pos)
    {
        int count = checked((int)Varint.Read(data[pos..], out int countBytesRead));
        pos += countBytesRead;
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
        int count = checked((int)Varint.Read(data[pos..], out int countBytesRead));
        pos += countBytesRead;
        if (count != expectedCount)
        {
            throw new InvalidDataException(
                $"Stable {objectKind} identity count '{count}' does not match metadata count '{expectedCount}'.");
        }

        var identities = new Guid[count];
        for (int i = 0; i < count; i++)
            identities[i] = ReadGuid(data, ref pos);
        return identities;
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

        byte[] bytes = Encoding.UTF8.GetBytes(value);
        WriteVarint(ms, checked((ulong)bytes.Length + 1));
        ms.Write(bytes);
    }

    private static void WriteString(MemoryStream ms, string value)
    {
        byte[] bytes = Encoding.UTF8.GetBytes(value);
        WriteVarint(ms, (ulong)bytes.Length);
        ms.Write(bytes);
    }

    private static string? ReadNullableString(ReadOnlySpan<byte> data, ref int pos)
    {
        ulong encodedLength = Varint.Read(data[pos..], out int bytesRead);
        pos += bytesRead;

        if (encodedLength == 0)
            return null;

        int length = checked((int)encodedLength - 1);
        string value = Encoding.UTF8.GetString(data.Slice(pos, length));
        pos += length;
        return value;
    }

    private static string ReadString(ReadOnlySpan<byte> data, ref int pos)
    {
        int length = checked((int)Varint.Read(data[pos..], out int bytesRead));
        pos += bytesRead;
        string value = Encoding.UTF8.GetString(data.Slice(pos, length));
        pos += length;
        return value;
    }
}
