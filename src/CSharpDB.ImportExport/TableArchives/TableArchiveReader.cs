using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text.Json;
using CSharpDB.ImportExport.Models;
using CSharpDB.ImportExport.Serialization;
using CSharpDB.Primitives;
using CSharpDB.Storage.Serialization;

namespace CSharpDB.ImportExport.TableArchives;

public static class TableArchiveReader
{
    private const int MaxNativeRowBytes = 256 * 1024 * 1024;
    private const int MaxNativeSchemaBytes = 16 * 1024 * 1024;
    private const int MaxNativeManifestBytes = 16 * 1024 * 1024;

    public static async ValueTask<TableArchiveManifest> ReadManifestAsync(
        string path,
        CancellationToken ct = default)
    {
        await using var stream = OpenRead(path);
        NativeTableArchiveHeader header = await ReadNativeHeaderAsync(stream, ct);
        TableArchiveSchema schema = await ReadNativeSchemaAsync(stream, header, ct);
        TableArchiveManifest manifest = await ReadNativeManifestAsync(stream, header, ct);
        await ValidateMetadataAsync(stream, header, schema, manifest, ct);
        return manifest;
    }

    public static async ValueTask<(TableArchiveSchema Schema, TableArchiveManifest Manifest)> ReadMetadataAsync(
        string path,
        CancellationToken ct = default)
    {
        await using var stream = OpenRead(path);
        return await ReadMetadataAsync(stream, ct);
    }

    /// <summary>
    /// Reads archive metadata from a readable, seekable stream without closing
    /// it. The stream position is not preserved.
    /// </summary>
    public static async ValueTask<(TableArchiveSchema Schema, TableArchiveManifest Manifest)> ReadMetadataAsync(
        Stream stream,
        CancellationToken ct = default)
    {
        ValidateInputStream(stream);
        NativeTableArchiveHeader header = await ReadNativeHeaderAsync(stream, ct);
        TableArchiveSchema schema = await ReadNativeSchemaAsync(stream, header, ct);
        TableArchiveManifest manifest = await ReadNativeManifestAsync(stream, header, ct);
        await ValidateMetadataAsync(stream, header, schema, manifest, ct);
        return (schema, manifest);
    }

    public static async ValueTask<TableArchiveSchema> ReadArchiveSchemaAsync(
        string path,
        CancellationToken ct = default)
    {
        await using var stream = OpenRead(path);
        NativeTableArchiveHeader header = await ReadNativeHeaderAsync(stream, ct);
        TableArchiveSchema schema = await ReadNativeSchemaAsync(stream, header, ct);
        TableArchiveManifest manifest = await ReadNativeManifestAsync(stream, header, ct);
        await ValidateMetadataAsync(stream, header, schema, manifest, ct);
        return schema;
    }

    public static async ValueTask<TableSchema> ReadTableSchemaAsync(
        string path,
        string? tableNameOverride = null,
        CancellationToken ct = default)
    {
        TableArchiveSchema schema = await ReadArchiveSchemaAsync(path, ct);
        return schema.ToTableSchema(tableNameOverride);
    }

    public static async IAsyncEnumerable<DbValue[]> ReadRowsAsync(
        string path,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        await using var stream = OpenRead(path);
        await foreach (DbValue[] row in ReadRowsAsync(stream, ct))
            yield return row;
    }

    /// <summary>
    /// Streams archive rows from a readable, seekable stream without closing
    /// it. The stream must not be used concurrently while enumeration is live.
    /// </summary>
    public static async IAsyncEnumerable<DbValue[]> ReadRowsAsync(
        Stream stream,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        ValidateInputStream(stream);
        NativeTableArchiveHeader header = await ReadNativeHeaderAsync(stream, ct);
        TableArchiveSchema archiveSchema = await ReadNativeSchemaAsync(stream, header, ct);
        TableArchiveManifest manifest = await ReadNativeManifestAsync(stream, header, ct);
        await ValidateMetadataAsync(stream, header, archiveSchema, manifest, ct);
        TableSchema schema = archiveSchema.ToTableSchema();
        await foreach (DbValue[] row in ReadNativeRowsAsync(stream, header, schema, ct))
            yield return row;
    }

    public static async ValueTask<bool> HasIntegerPrimaryKeyIndexAsync(
        string path,
        CancellationToken ct = default)
    {
        await using var stream = OpenRead(path);
        NativeTableArchiveHeader header = await ReadNativeHeaderAsync(stream, ct);
        TableArchiveSchema schema = await ReadNativeSchemaAsync(stream, header, ct);
        TableArchiveManifest manifest = await ReadNativeManifestAsync(stream, header, ct);
        NativeTableArchiveIndexHeader? indexHeader =
            await ValidateMetadataAsync(stream, header, schema, manifest, ct);
        return indexHeader is not null;
    }

    public static async ValueTask<TableArchiveRowLookupResult> LookupIntegerPrimaryKeyAsync(
        string path,
        long key,
        CancellationToken ct = default)
    {
        await using var stream = OpenRead(path);
        NativeTableArchiveHeader header = await ReadNativeHeaderAsync(stream, ct);
        TableArchiveSchema schema = await ReadNativeSchemaAsync(stream, header, ct);
        TableArchiveManifest manifest = await ReadNativeManifestAsync(stream, header, ct);
        NativeTableArchiveIndexHeader? validatedIndexHeader =
            await ValidateMetadataAsync(stream, header, schema, manifest, ct);
        if (validatedIndexHeader is null)
            return new TableArchiveRowLookupResult(IsIndexed: false, Row: null);

        NativeTableArchiveIndexHeader indexHeader = validatedIndexHeader.Value;
        if (indexHeader.EntryCount == 0)
            return new TableArchiveRowLookupResult(IsIndexed: true, Row: null);

        long pageOffset = indexHeader.RootPageOffset;
        var page = new byte[TableArchiveNativeFormat.IndexPageSize];

        while (true)
        {
            ValidateIndexPageOffset(header, pageOffset);
            stream.Position = header.IndexOffset + pageOffset;
            await stream.ReadExactlyAsync(page, ct);

            var pageHeader = TableArchiveNativeFormat.ReadIndexPageHeader(page);
            if (pageHeader.EntryCount == 0)
                return new TableArchiveRowLookupResult(IsIndexed: true, Row: null);

            if (pageHeader.PageType == TableArchiveNativeFormat.IndexLeafPageType)
            {
                int entryIndex = BinarySearchLeafEntry(page, pageHeader.EntryCount, key);
                if (entryIndex < 0)
                    return new TableArchiveRowLookupResult(IsIndexed: true, Row: null);

                long rowOffset = ReadIndexEntryValue(page, entryIndex);
                DbValue[] row = await ReadNativeRowAtOffsetAsync(stream, header, rowOffset, ct);
                ValidateRow(schema.ToTableSchema(), row, rowIndex: -1);
                DbValue indexedValue = row[indexHeader.KeyColumnIndex];
                if (indexedValue.Type != DbType.Integer || indexedValue.AsInteger != key)
                {
                    throw new InvalidDataException(
                        "The native table archive index points to a row with a different primary key.");
                }

                return new TableArchiveRowLookupResult(IsIndexed: true, Row: row);
            }

            int childIndex = FindInteriorChildIndex(page, pageHeader.EntryCount, key);
            if (childIndex < 0)
                return new TableArchiveRowLookupResult(IsIndexed: true, Row: null);

            pageOffset = ReadIndexEntryValue(page, childIndex);
        }
    }

    internal static FileStream OpenRead(string path)
        => new(path, FileMode.Open, FileAccess.Read, FileShare.Read);

    private static void ValidateInputStream(Stream stream)
    {
        ArgumentNullException.ThrowIfNull(stream);
        if (!stream.CanRead || !stream.CanSeek)
        {
            throw new ArgumentException(
                "A table archive stream must be readable and seekable.",
                nameof(stream));
        }
    }

    private static async ValueTask<NativeTableArchiveHeader> ReadNativeHeaderAsync(
        Stream stream,
        CancellationToken ct)
    {
        NativeTableArchiveHeader header = await TryReadNativeHeaderAsync(stream, ct)
            ?? throw new InvalidDataException("The table archive is not a native CSharpDB table archive.");
        ValidateSectionLayout(stream.Length, header);
        return header;
    }

    internal static async ValueTask<NativeTableArchiveHeader?> TryReadNativeHeaderAsync(
        Stream stream,
        CancellationToken ct)
    {
        stream.Position = 0;
        var magic = new byte[8];
        int read = await stream.ReadAsync(magic, ct);
        if (read < magic.Length || !TableArchiveNativeFormat.IsMagic(magic))
        {
            stream.Position = 0;
            return null;
        }

        stream.Position = 0;
        return await TableArchiveNativeFormat.ReadHeaderAsync(stream, ct);
    }

    internal static async ValueTask<(
        NativeTableArchiveHeader Header,
        NativeTableArchiveIndexHeader? IndexHeader,
        TableSchema Schema)?> TryReadValidatedLookupMetadataAsync(
        Stream stream,
        CancellationToken ct)
    {
        NativeTableArchiveHeader? nativeHeader = await TryReadNativeHeaderAsync(stream, ct);
        if (nativeHeader is not { } header)
            return null;

        ValidateSectionLayout(stream.Length, header);
        TableArchiveSchema archiveSchema = await ReadNativeSchemaAsync(stream, header, ct);
        TableArchiveManifest manifest = await ReadNativeManifestAsync(stream, header, ct);
        NativeTableArchiveIndexHeader? indexHeader =
            await ValidateMetadataAsync(stream, header, archiveSchema, manifest, ct);
        return (header, indexHeader, archiveSchema.ToTableSchema());
    }

    private static async ValueTask<TableArchiveSchema> ReadNativeSchemaAsync(
        Stream stream,
        NativeTableArchiveHeader header,
        CancellationToken ct)
    {
        byte[] bytes = await ReadSectionAsync(stream, header.SchemaOffset, header.SchemaLength, ct);
        TableArchiveSchema schema =
            JsonSerializer.Deserialize<TableArchiveSchema>(bytes, TableArchiveJson.Options)
            ?? throw new InvalidDataException("The table archive schema is empty.");
        if (schema.Columns is null)
            throw new InvalidDataException("The table archive schema columns collection is null.");
        if (schema.Columns.Any(static column => column is not null && column.IsRowVersion) &&
            header.FormatVersion < TableArchiveManifest.RowVersionFormatVersion)
        {
            throw new InvalidDataException(
                "ROWVERSION table archives require native archive format version 4 or later.");
        }

        return schema;
    }

    private static async ValueTask<TableArchiveManifest> ReadNativeManifestAsync(
        Stream stream,
        NativeTableArchiveHeader header,
        CancellationToken ct)
    {
        byte[] bytes = await ReadSectionAsync(stream, header.ManifestOffset, header.ManifestLength, ct);
        var manifest = JsonSerializer.Deserialize<TableArchiveManifest>(bytes, TableArchiveJson.Options)
            ?? throw new InvalidDataException("The table archive manifest is empty.");
        if (manifest.FormatVersion is not (
                TableArchiveManifest.CurrentFormatVersion or
                TableArchiveManifest.RowVersionFormatVersion or
                TableArchiveManifest.SchemaFidelityFormatVersion or
                TableArchiveManifest.ReferentialActionsFormatVersion or
                TableArchiveManifest.LogicalTypesFormatVersion))
            throw new InvalidDataException($"Unsupported native table archive format version {manifest.FormatVersion}.");
        if (manifest.FormatVersion != header.FormatVersion)
            throw new InvalidDataException("The table archive header and manifest format versions do not match.");
        return manifest;
    }

    internal static async ValueTask<NativeTableArchiveIndexHeader> ReadNativeIndexHeaderAsync(
        Stream stream,
        NativeTableArchiveHeader header,
        CancellationToken ct)
    {
        if (header.IndexOffset <= 0 ||
            header.IndexLength < TableArchiveNativeFormat.IndexHeaderSize)
        {
            throw new InvalidDataException("The native table archive index section is invalid.");
        }

        byte[] bytes = await ReadSectionAsync(
            stream,
            header.IndexOffset,
            TableArchiveNativeFormat.IndexHeaderSize,
            ct);
        return TableArchiveNativeFormat.ReadIndexHeader(bytes);
    }

    private static async ValueTask<byte[]> ReadSectionAsync(
        Stream stream,
        long offset,
        int length,
        CancellationToken ct)
    {
        if (length <= 0)
            throw new InvalidDataException("The native table archive section length is invalid.");

        stream.Position = offset;
        byte[] bytes = GC.AllocateUninitializedArray<byte>(length);
        await stream.ReadExactlyAsync(bytes, ct);
        return bytes;
    }

    private static async IAsyncEnumerable<DbValue[]> ReadNativeRowsAsync(
        Stream stream,
        NativeTableArchiveHeader header,
        TableSchema schema,
        [EnumeratorCancellation] CancellationToken ct)
    {
        stream.Position = header.RowsOffset;
        var lengthBuffer = new byte[sizeof(int)];
        for (long rowIndex = 0; rowIndex < header.RowCount; rowIndex++)
        {
            ct.ThrowIfCancellationRequested();
            await stream.ReadExactlyAsync(lengthBuffer, ct);
            int length = BinaryPrimitives.ReadInt32LittleEndian(lengthBuffer);
            if (length <= 0 || length > MaxNativeRowBytes)
                throw new InvalidDataException("The native table archive row length is invalid.");

            byte[] record = GC.AllocateUninitializedArray<byte>(length);
            await stream.ReadExactlyAsync(record, ct);
            DbValue[] row = RecordEncoder.Decode(record);
            ValidateRow(schema, row, rowIndex);
            yield return row;
        }

        long expectedRowsEnd = checked(header.RowsOffset + header.RowsLength);
        if (stream.Position != expectedRowsEnd)
            throw new InvalidDataException("The native table archive rows section length does not match its row records.");
    }

    private static async ValueTask<NativeTableArchiveIndexHeader?> ValidateMetadataAsync(
        Stream stream,
        NativeTableArchiveHeader header,
        TableArchiveSchema schema,
        TableArchiveManifest manifest,
        CancellationToken ct)
    {
        ValidateSchema(schema, header.FormatVersion);
        if (manifest.Indexes is null)
            throw new InvalidDataException("The table archive physical index collection is null.");

        NativeTableArchiveIndexHeader? nativeIndexHeader = null;
        if (header.IndexLength == 0)
        {
            if (header.IndexOffset != 0 || manifest.Indexes.Count != 0)
                throw new InvalidDataException("The table archive index metadata is inconsistent.");
        }
        else
        {
            if (manifest.Indexes.Count != 1)
                throw new InvalidDataException("The table archive physical index manifest is inconsistent.");

            TableArchiveIndexManifest indexManifest = manifest.Indexes[0]
                ?? throw new InvalidDataException("The table archive physical index manifest contains a null entry.");
            NativeTableArchiveIndexHeader indexHeader =
                await ReadNativeIndexHeaderAsync(stream, header, ct);
            ValidatePhysicalIndexMetadata(header, schema, indexManifest, indexHeader);
            nativeIndexHeader = indexHeader;
        }

        if (manifest.RowCount != header.RowCount)
            throw new InvalidDataException("The table archive header and manifest row counts do not match.");
        if (!string.Equals(manifest.SourceTableName, schema.TableName, StringComparison.Ordinal))
            throw new InvalidDataException("The table archive schema and manifest table names do not match.");
        if (schema.SecondaryIndexes is { Count: > 0 } &&
            header.FormatVersion < TableArchiveManifest.SchemaFidelityFormatVersion)
        {
            throw new InvalidDataException(
                "Logical secondary-index metadata requires native archive format version 5 or later.");
        }
        if (!string.Equals(manifest.SchemaEntry, "native:schema", StringComparison.Ordinal) ||
            !string.Equals(manifest.RowsEntry, "native:rows", StringComparison.Ordinal))
        {
            throw new InvalidDataException("The table archive manifest contains unsupported section identifiers.");
        }

        await ValidateIntegrityAsync(stream, header, manifest, ct);

        return nativeIndexHeader;
    }

    private static async ValueTask ValidateIntegrityAsync(
        Stream stream,
        NativeTableArchiveHeader header,
        TableArchiveManifest manifest,
        CancellationToken ct)
    {
        if (header.FormatVersion < TableArchiveManifest.IntegrityFormatVersion)
            return;

        TableArchiveSectionDigests digests = manifest.Digests
            ?? throw new InvalidDataException("Native table archive format version 5 requires section digests.");
        if (!string.Equals(
                digests.Algorithm,
                TableArchiveSectionDigests.Sha256Algorithm,
                StringComparison.Ordinal))
        {
            throw new InvalidDataException("The table archive digest algorithm must be 'sha256'.");
        }

        if (!string.Equals(
                digests.Encoding,
                TableArchiveSectionDigests.LowercaseHexEncoding,
                StringComparison.Ordinal))
        {
            throw new InvalidDataException("The table archive digest encoding must be 'lowercase-hex'.");
        }

        ValidateDigestText(digests.Schema, "schema");
        ValidateDigestText(digests.Rows, "rows");
        ValidateDigestText(digests.PhysicalIndex, "physical index");

        await VerifySectionDigestAsync(
            stream,
            header.SchemaOffset,
            header.SchemaLength,
            digests.Schema,
            "schema",
            ct);
        await VerifySectionDigestAsync(
            stream,
            header.RowsOffset,
            header.RowsLength,
            digests.Rows,
            "rows",
            ct);
        await VerifySectionDigestAsync(
            stream,
            header.IndexOffset,
            header.IndexLength,
            digests.PhysicalIndex,
            "physical index",
            ct);
    }

    private static void ValidateDigestText(string? digest, string sectionName)
    {
        if (digest is null ||
            digest.Length != SHA256.HashSizeInBytes * 2 ||
            digest.Any(static character =>
                character is not (>= '0' and <= '9') and
                not (>= 'a' and <= 'f')))
        {
            throw new InvalidDataException(
                $"The table archive {sectionName} digest must be 64 lowercase hexadecimal characters.");
        }
    }

    private static async ValueTask VerifySectionDigestAsync(
        Stream stream,
        long offset,
        long length,
        string expectedDigest,
        string sectionName,
        CancellationToken ct)
    {
        const int bufferSize = 64 * 1024;
        byte[] buffer = ArrayPool<byte>.Shared.Rent(bufferSize);
        using var hasher = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        try
        {
            if (length > 0)
            {
                stream.Position = offset;
                long remaining = length;
                while (remaining > 0)
                {
                    int count = (int)Math.Min(buffer.Length, remaining);
                    await stream.ReadExactlyAsync(buffer.AsMemory(0, count), ct);
                    hasher.AppendData(buffer.AsSpan(0, count));
                    remaining -= count;
                }
            }

            string actualDigest = Convert.ToHexString(hasher.GetHashAndReset()).ToLowerInvariant();
            if (!string.Equals(actualDigest, expectedDigest, StringComparison.Ordinal))
            {
                throw new InvalidDataException(
                    $"The native table archive {sectionName} section digest does not match its manifest.");
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private static void ValidateSectionLayout(long streamLength, NativeTableArchiveHeader header)
    {
        if (header.SchemaLength > MaxNativeSchemaBytes)
            throw new InvalidDataException("The native table archive schema section exceeds the size limit.");
        if (header.ManifestLength > MaxNativeManifestBytes)
            throw new InvalidDataException("The native table archive manifest section exceeds the size limit.");

        ValidateSectionRange(streamLength, header.SchemaOffset, header.SchemaLength, "schema");
        ValidateSectionRange(streamLength, header.RowsOffset, header.RowsLength, "rows", allowEmpty: true);
        ValidateSectionRange(streamLength, header.ManifestOffset, header.ManifestLength, "manifest");
        if (header.IndexLength > 0)
            ValidateSectionRange(streamLength, header.IndexOffset, header.IndexLength, "index");

        long schemaEnd = header.SchemaOffset + header.SchemaLength;
        long rowsEnd = header.RowsOffset + header.RowsLength;
        long expectedManifestOffset;
        if (header.IndexLength == 0)
        {
            if (header.IndexOffset != 0)
                throw new InvalidDataException("The native table archive empty index section has a nonzero offset.");
            expectedManifestOffset = rowsEnd;
        }
        else
        {
            if (header.IndexOffset != rowsEnd)
                throw new InvalidDataException("The native table archive index section is not in canonical order.");
            expectedManifestOffset = header.IndexOffset + header.IndexLength;
        }

        if (header.SchemaOffset != TableArchiveNativeFormat.HeaderSize ||
            header.RowsOffset != schemaEnd ||
            header.ManifestOffset != expectedManifestOffset)
        {
            throw new InvalidDataException("The native table archive sections are not contiguous and in canonical order.");
        }

        if ((header.RowCount == 0) != (header.RowsLength == 0))
            throw new InvalidDataException("The native table archive row count and rows section length are inconsistent.");

        long manifestEnd = header.ManifestOffset + header.ManifestLength;
        if (manifestEnd != streamLength)
            throw new InvalidDataException("The native table archive length does not match its section metadata.");
    }

    private static void ValidatePhysicalIndexMetadata(
        NativeTableArchiveHeader archiveHeader,
        TableArchiveSchema schema,
        TableArchiveIndexManifest manifest,
        NativeTableArchiveIndexHeader indexHeader)
    {
        if (manifest.ColumnIndex < 0 || manifest.ColumnIndex >= schema.Columns.Count)
            throw new InvalidDataException("The table archive physical index column ordinal is invalid.");

        TableArchiveColumn column = schema.Columns[manifest.ColumnIndex]
            ?? throw new InvalidDataException("The table archive physical index references a null column.");
        if (!string.Equals(manifest.Kind, "primary-key", StringComparison.Ordinal) ||
            !string.Equals(manifest.SectionEntry, "native:index:primary-key", StringComparison.Ordinal) ||
            !string.Equals(manifest.Name, $"{schema.TableName}_pk", StringComparison.Ordinal) ||
            !string.Equals(manifest.ColumnName, column.Name, StringComparison.Ordinal) ||
            manifest.ColumnIndex != indexHeader.KeyColumnIndex ||
            manifest.EntryCount != indexHeader.EntryCount ||
            manifest.EntryCount != archiveHeader.RowCount ||
            column.Type != DbType.Integer ||
            !column.IsPrimaryKey ||
            column.Nullable)
        {
            throw new InvalidDataException("The table archive physical index metadata does not match its schema and native header.");
        }

        if (indexHeader.PageCount >
            (long.MaxValue - TableArchiveNativeFormat.IndexHeaderSize) /
            TableArchiveNativeFormat.IndexPageSize)
        {
            throw new InvalidDataException("The table archive physical index page count is invalid.");
        }

        long expectedIndexLength =
            TableArchiveNativeFormat.IndexHeaderSize +
            indexHeader.PageCount * TableArchiveNativeFormat.IndexPageSize;
        if (archiveHeader.IndexLength != expectedIndexLength ||
            (indexHeader.EntryCount == 0 &&
             (indexHeader.PageCount != 0 || indexHeader.RootPageOffset != 0)) ||
            (indexHeader.EntryCount > 0 &&
             (indexHeader.PageCount <= 0 ||
              indexHeader.RootPageOffset < TableArchiveNativeFormat.IndexHeaderSize ||
              indexHeader.RootPageOffset > archiveHeader.IndexLength - TableArchiveNativeFormat.IndexPageSize)))
        {
            throw new InvalidDataException("The table archive physical index section length is inconsistent.");
        }
    }

    private static void ValidateSectionRange(
        long streamLength,
        long offset,
        long length,
        string sectionName,
        bool allowEmpty = false)
    {
        if (offset < TableArchiveNativeFormat.HeaderSize ||
            length < (allowEmpty ? 0 : 1) ||
            offset > streamLength ||
            length > streamLength - offset)
        {
            throw new InvalidDataException($"The native table archive {sectionName} section range is invalid.");
        }
    }

    private static void ValidateSchema(
        TableArchiveSchema schema,
        int formatVersion)
    {
        SqlIdentifierRules.Validate(schema.TableName, "Archived table name");
        if (schema.Columns is null || schema.Columns.Count == 0)
            throw new InvalidDataException("The table archive schema has no columns.");

        var columnNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var columnsByName = new Dictionary<string, TableArchiveColumn>(StringComparer.OrdinalIgnoreCase);
        foreach (TableArchiveColumn column in schema.Columns)
        {
            if (column is null)
                throw new InvalidDataException("The table archive schema columns collection contains a null entry.");
            SqlIdentifierRules.Validate(column.Name, "Archived column name");
            if (!columnNames.Add(column.Name))
                throw new InvalidDataException($"The table archive contains duplicate column '{column.Name}'.");
            columnsByName[column.Name] = column;
            if (column.Type == DbType.Null)
                throw new InvalidDataException($"Archived column '{column.Name}' has an invalid persistent type.");
            if (column.DeclaredType is not null &&
                formatVersion < TableArchiveManifest.LogicalTypesFormatVersion)
            {
                throw new InvalidDataException(
                    $"Archived column '{column.Name}' declares a logical SQL type, which requires native archive format version {TableArchiveManifest.LogicalTypesFormatVersion}.");
            }
            if (column.DeclaredType is { } declaredType &&
                declaredType.StorageType != column.Type)
            {
                throw new InvalidDataException(
                    $"Archived column '{column.Name}' declares {declaredType.ToSql()} but stores values as {column.Type}.");
            }
            if (column.IsIdentity &&
                (column.Type != DbType.Integer ||
                 column.DeclaredType is { Kind: not (SqlTypeKind.Integer or SqlTypeKind.BigInt) }))
            {
                throw new InvalidDataException(
                    $"Archived identity column '{column.Name}' must be declared INTEGER or BIGINT.");
            }
            if (column.IsRowVersion && (column.Type != DbType.Blob || column.Nullable))
                throw new InvalidDataException($"Archived ROWVERSION column '{column.Name}' is invalid.");
            if (column.IsRowVersion &&
                column.DeclaredType is { Kind: not SqlTypeKind.Blob })
            {
                throw new InvalidDataException(
                    $"Archived ROWVERSION column '{column.Name}' must be declared BLOB.");
            }
        }

        if (schema.KeyConstraints is null)
            throw new InvalidDataException("The table archive key constraint collection is null.");
        foreach (TableArchiveKeyConstraint key in schema.KeyConstraints)
        {
            if (key is null)
                throw new InvalidDataException("The table archive key constraint collection contains a null entry.");
            if (key.ConstraintName is not null)
                SqlIdentifierRules.Validate(key.ConstraintName, "Archived key constraint name");
            ValidateColumnList(columnNames, key.Columns, "key constraint");
        }

        if (schema.CheckConstraints is null)
            throw new InvalidDataException("The table archive check constraint collection is null.");
        foreach (TableArchiveCheckConstraint check in schema.CheckConstraints)
        {
            if (check is null)
                throw new InvalidDataException("The table archive check constraint collection contains a null entry.");
            if (check.ConstraintName is not null)
                SqlIdentifierRules.Validate(check.ConstraintName, "Archived check constraint name");
            if (string.IsNullOrWhiteSpace(check.ExpressionSql))
                throw new InvalidDataException("An archived CHECK constraint has no expression.");
            if (check.ColumnName is not null && !columnNames.Contains(check.ColumnName))
                throw new InvalidDataException($"Archived CHECK constraint references missing column '{check.ColumnName}'.");
        }

        if (schema.ForeignKeys is null)
            throw new InvalidDataException("The table archive foreign key collection is null.");
        foreach (TableArchiveForeignKey foreignKey in schema.ForeignKeys)
        {
            if (foreignKey is null)
                throw new InvalidDataException("The table archive foreign key collection contains a null entry.");
            if (!Enum.IsDefined(foreignKey.OnDelete))
            {
                throw new InvalidDataException(
                    $"Archived foreign key '{foreignKey.ConstraintName}' has an unknown ON DELETE action.");
            }
            if (!Enum.IsDefined(foreignKey.OnUpdate))
            {
                throw new InvalidDataException(
                    $"Archived foreign key '{foreignKey.ConstraintName}' has an unknown ON UPDATE action.");
            }
            if (formatVersion <
                    TableArchiveManifest.ReferentialActionsFormatVersion &&
                foreignKey.OnDelete is not (
                    ForeignKeyOnDeleteAction.Restrict or
                    ForeignKeyOnDeleteAction.Cascade))
            {
                throw new InvalidDataException(
                    $"Archived foreign key '{foreignKey.ConstraintName}' requires native archive format version {TableArchiveManifest.ReferentialActionsFormatVersion} for ON DELETE action '{foreignKey.OnDelete}'.");
            }
            if (formatVersion <
                    TableArchiveManifest.ReferentialActionsFormatVersion &&
                foreignKey.OnUpdate != ForeignKeyOnDeleteAction.Restrict)
            {
                throw new InvalidDataException(
                    $"Archived foreign key '{foreignKey.ConstraintName}' requires native archive format version {TableArchiveManifest.ReferentialActionsFormatVersion} for ON UPDATE action '{foreignKey.OnUpdate}'.");
            }
            SqlIdentifierRules.Validate(foreignKey.ConstraintName, "Archived foreign key name");
            SqlIdentifierRules.Validate(foreignKey.ReferencedTableName, "Archived referenced table name");
            if (foreignKey.ColumnNames is null || foreignKey.ReferencedColumnNames is null)
                throw new InvalidDataException($"Archived foreign key '{foreignKey.ConstraintName}' has null column metadata.");
            if (foreignKey.ColumnSchemaIds is null ||
                foreignKey.ReferencedColumnSchemaIds is null)
            {
                throw new InvalidDataException(
                    $"Archived foreign key '{foreignKey.ConstraintName}' has null stable identity bindings.");
            }
            IReadOnlyList<string> sourceColumns = foreignKey.ColumnNames.Count > 0
                ? foreignKey.ColumnNames
                : [foreignKey.ColumnName];
            IReadOnlyList<string> referencedColumns = foreignKey.ReferencedColumnNames.Count > 0
                ? foreignKey.ReferencedColumnNames
                : [foreignKey.ReferencedColumnName];
            ValidateColumnList(columnNames, sourceColumns, "foreign key");
            if (sourceColumns.Count != referencedColumns.Count || referencedColumns.Count == 0)
                throw new InvalidDataException($"Archived foreign key '{foreignKey.ConstraintName}' has inconsistent column lists.");
            if (!string.Equals(
                    foreignKey.ColumnName,
                    sourceColumns[0],
                    StringComparison.OrdinalIgnoreCase) ||
                !string.Equals(
                    foreignKey.ReferencedColumnName,
                    referencedColumns[0],
                    StringComparison.OrdinalIgnoreCase))
            {
                throw new InvalidDataException(
                    $"Archived foreign key '{foreignKey.ConstraintName}' has scalar and ordered columns that disagree.");
            }

            var referencedColumnNames =
                new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (string referencedColumn in referencedColumns)
            {
                SqlIdentifierRules.Validate(referencedColumn, "Archived referenced column name");
                if (!referencedColumnNames.Add(referencedColumn))
                {
                    throw new InvalidDataException(
                        $"Archived foreign key '{foreignKey.ConstraintName}' repeats referenced column '{referencedColumn}'.");
                }
            }
            if (string.Equals(
                    foreignKey.ReferencedTableName,
                    schema.TableName,
                    StringComparison.OrdinalIgnoreCase))
            {
                ValidateColumnList(
                    columnNames,
                    referencedColumns,
                    "self-referencing foreign key");
            }
        }

        ValidateSchemaIdentities(schema, columnsByName);

        var indexNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (TableArchiveSecondaryIndex index in
                 schema.SecondaryIndexes ?? Array.Empty<TableArchiveSecondaryIndex>())
        {
            if (index is null)
                throw new InvalidDataException("The table archive secondary index collection contains a null entry.");
            if (index.Columns is null || index.ColumnCollations is null)
                throw new InvalidDataException($"Archived secondary index '{index.Name}' has null column metadata.");
            SqlIdentifierRules.Validate(index.Name, "Archived secondary index name");
            if (!indexNames.Add(index.Name))
                throw new InvalidDataException($"The table archive contains duplicate secondary index '{index.Name}'.");
            ValidateColumnList(columnNames, index.Columns, "secondary index");
            if (index.ColumnCollations.Count != 0 && index.ColumnCollations.Count != index.Columns.Count)
                throw new InvalidDataException($"Archived secondary index '{index.Name}' has inconsistent collation metadata.");
            foreach (string? collation in index.ColumnCollations)
            {
                if (collation is not null)
                    SqlIdentifierRules.Validate(collation, "Archived index collation");
            }
        }
    }

    private static void ValidateSchemaIdentities(
        TableArchiveSchema schema,
        IReadOnlyDictionary<string, TableArchiveColumn> columnsByName)
    {
        bool hasAnyIdentity =
            schema.SchemaId != Guid.Empty ||
            schema.Columns.Any(static column => column.SchemaId != Guid.Empty) ||
            schema.KeyConstraints.Any(static key => key.SchemaId != Guid.Empty) ||
            schema.CheckConstraints.Any(static check => check.SchemaId != Guid.Empty) ||
            schema.ForeignKeys.Any(foreignKey =>
                foreignKey.SchemaId != Guid.Empty ||
                foreignKey.ReferencedTableSchemaId != Guid.Empty ||
                foreignKey.ReferencedKeySchemaId != Guid.Empty ||
                foreignKey.ColumnSchemaIds is { Count: > 0 } ||
                foreignKey.ReferencedColumnSchemaIds is { Count: > 0 });

        if (!hasAnyIdentity)
            return;

        var ownedIdentities = new HashSet<Guid>();
        AddOwnedIdentity(ownedIdentities, schema.SchemaId, "table");
        foreach (TableArchiveColumn column in schema.Columns)
            AddOwnedIdentity(ownedIdentities, column.SchemaId, $"column '{column.Name}'");
        foreach (TableArchiveKeyConstraint key in schema.KeyConstraints)
            AddOwnedIdentity(
                ownedIdentities,
                key.SchemaId,
                $"key constraint '{key.ConstraintName ?? "<unnamed>"}'");
        foreach (TableArchiveCheckConstraint check in schema.CheckConstraints)
            AddOwnedIdentity(
                ownedIdentities,
                check.SchemaId,
                $"check constraint '{check.ConstraintName ?? "<unnamed>"}'");
        var externalIdentityRoles = new Dictionary<Guid, string>();
        foreach (TableArchiveForeignKey foreignKey in schema.ForeignKeys)
        {
            AddOwnedIdentity(
                ownedIdentities,
                foreignKey.SchemaId,
                $"foreign key '{foreignKey.ConstraintName}'");
        }

        foreach (TableArchiveForeignKey foreignKey in schema.ForeignKeys)
        {
            IReadOnlyList<string> sourceColumns = foreignKey.ColumnNames.Count > 0
                ? foreignKey.ColumnNames
                : [foreignKey.ColumnName];
            IReadOnlyList<string> referencedColumns = foreignKey.ReferencedColumnNames.Count > 0
                ? foreignKey.ReferencedColumnNames
                : [foreignKey.ReferencedColumnName];
            if (foreignKey.ColumnSchemaIds is null ||
                foreignKey.ColumnSchemaIds.Count != sourceColumns.Count)
            {
                throw new InvalidDataException(
                    $"Archived foreign key '{foreignKey.ConstraintName}' has inconsistent child-column identity bindings.");
            }
            if (foreignKey.ReferencedColumnSchemaIds is null ||
                foreignKey.ReferencedColumnSchemaIds.Count != referencedColumns.Count)
            {
                throw new InvalidDataException(
                    $"Archived foreign key '{foreignKey.ConstraintName}' has inconsistent referenced-column identity bindings.");
            }
            if (foreignKey.ReferencedTableSchemaId == Guid.Empty)
            {
                throw new InvalidDataException(
                    $"Archived foreign key '{foreignKey.ConstraintName}' has no referenced-table identity.");
            }

            var referencedColumnIdentities = new HashSet<Guid>();
            for (int i = 0; i < sourceColumns.Count; i++)
            {
                Guid childColumnId = foreignKey.ColumnSchemaIds[i];
                Guid expectedChildColumnId = columnsByName[sourceColumns[i]].SchemaId;
                if (childColumnId == Guid.Empty || childColumnId != expectedChildColumnId)
                {
                    throw new InvalidDataException(
                        $"Archived foreign key '{foreignKey.ConstraintName}' has an invalid child-column identity binding.");
                }

                Guid referencedColumnId = foreignKey.ReferencedColumnSchemaIds[i];
                if (referencedColumnId == Guid.Empty ||
                    !referencedColumnIdentities.Add(referencedColumnId))
                {
                    throw new InvalidDataException(
                        $"Archived foreign key '{foreignKey.ConstraintName}' has an invalid referenced-column identity binding.");
                }
            }

            if (string.Equals(
                    foreignKey.ReferencedTableName,
                    schema.TableName,
                    StringComparison.OrdinalIgnoreCase))
            {
                if (foreignKey.ReferencedTableSchemaId != schema.SchemaId)
                {
                    throw new InvalidDataException(
                        $"Archived self-referencing foreign key '{foreignKey.ConstraintName}' has an invalid referenced-table identity.");
                }

                for (int i = 0; i < referencedColumns.Count; i++)
                {
                    if (!columnsByName.TryGetValue(
                            referencedColumns[i],
                            out TableArchiveColumn? referencedColumn) ||
                        foreignKey.ReferencedColumnSchemaIds[i] !=
                        referencedColumn.SchemaId)
                    {
                        throw new InvalidDataException(
                            $"Archived self-referencing foreign key '{foreignKey.ConstraintName}' has an invalid referenced-column identity binding.");
                    }
                }

                TableArchiveKeyConstraint[] matchingKeys = schema.KeyConstraints
                    .Where(key => key.Columns.SequenceEqual(
                        referencedColumns,
                        StringComparer.OrdinalIgnoreCase))
                    .ToArray();
                if (matchingKeys.Length > 0)
                {
                    if (!matchingKeys.Any(key =>
                            key.SchemaId ==
                            foreignKey.ReferencedKeySchemaId))
                    {
                        throw new InvalidDataException(
                            $"Archived self-referencing foreign key '{foreignKey.ConstraintName}' has an invalid referenced-key identity binding.");
                    }
                }
                else if (foreignKey.ReferencedKeySchemaId != Guid.Empty)
                {
                    throw new InvalidDataException(
                        $"Archived self-referencing foreign key '{foreignKey.ConstraintName}' has a referenced-key identity without a matching logical key.");
                }
            }
            else
            {
                AddExternalIdentityRole(
                    externalIdentityRoles,
                    foreignKey.ReferencedTableSchemaId,
                    "table",
                    foreignKey.ConstraintName);
                foreach (Guid referencedColumnId in
                         foreignKey.ReferencedColumnSchemaIds)
                {
                    AddExternalIdentityRole(
                        externalIdentityRoles,
                        referencedColumnId,
                        "column",
                        foreignKey.ConstraintName);
                }
                if (foreignKey.ReferencedKeySchemaId != Guid.Empty)
                {
                    AddExternalIdentityRole(
                        externalIdentityRoles,
                        foreignKey.ReferencedKeySchemaId,
                        "key",
                        foreignKey.ConstraintName);
                }

                if (ownedIdentities.Contains(
                        foreignKey.ReferencedTableSchemaId) ||
                    foreignKey.ReferencedColumnSchemaIds.Any(
                        ownedIdentities.Contains) ||
                    foreignKey.ReferencedKeySchemaId != Guid.Empty &&
                    ownedIdentities.Contains(
                        foreignKey.ReferencedKeySchemaId))
                {
                    throw new InvalidDataException(
                        $"Archived foreign key '{foreignKey.ConstraintName}' reuses an identity owned by its child table for an external reference.");
                }
            }
        }
    }

    private static void AddExternalIdentityRole(
        IDictionary<Guid, string> identityRoles,
        Guid identity,
        string role,
        string constraintName)
    {
        if (identityRoles.TryGetValue(
                identity,
                out string? existingRole))
        {
            if (!string.Equals(
                    existingRole,
                    role,
                    StringComparison.Ordinal))
            {
                throw new InvalidDataException(
                    $"Archived foreign key '{constraintName}' reuses a stable identity across referenced object roles.");
            }

            return;
        }

        identityRoles.Add(identity, role);
    }

    private static void AddOwnedIdentity(
        ISet<Guid> identities,
        Guid identity,
        string description)
    {
        if (identity == Guid.Empty)
            throw new InvalidDataException($"The archived {description} has no stable identity.");
        if (!identities.Add(identity))
            throw new InvalidDataException($"The archived {description} repeats a stable identity.");
    }

    private static void ValidateColumnList(
        IReadOnlySet<string> tableColumns,
        IReadOnlyList<string> columns,
        string description)
    {
        if (columns is null || columns.Count == 0)
            throw new InvalidDataException($"An archived {description} has no columns.");

        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (string column in columns)
        {
            SqlIdentifierRules.Validate(column, $"Archived {description} column");
            if (!tableColumns.Contains(column))
                throw new InvalidDataException($"Archived {description} references missing column '{column}'.");
            if (!seen.Add(column))
                throw new InvalidDataException($"Archived {description} repeats column '{column}'.");
        }
    }

    internal static void ValidateRow(TableSchema schema, DbValue[] row, long rowIndex)
    {
        if (row.Length != schema.Columns.Count)
        {
            throw new InvalidDataException(
                $"Archived row {rowIndex} has {row.Length} values; expected {schema.Columns.Count}.");
        }

        for (int columnIndex = 0; columnIndex < row.Length; columnIndex++)
        {
            ColumnDefinition column = schema.Columns[columnIndex];
            DbValue value = row[columnIndex];
            if (value.IsNull)
            {
                if (!column.Nullable || column.IsPrimaryKey || column.IsRowVersion)
                {
                    throw new InvalidDataException(
                        $"Archived row {rowIndex}, column '{column.Name}' cannot be NULL.");
                }
            }
            else if (value.Type != column.Type)
            {
                throw new InvalidDataException(
                    $"Archived row {rowIndex}, column '{column.Name}' has value tag {value.Type}; expected {column.Type}.");
            }
        }
    }

    internal static async ValueTask<DbValue[]> ReadNativeRowAtOffsetAsync(
        Stream stream,
        NativeTableArchiveHeader header,
        long rowOffset,
        CancellationToken ct)
    {
        long rowsEnd = checked(header.RowsOffset + header.RowsLength);
        if (rowOffset < header.RowsOffset || rowOffset + sizeof(int) > rowsEnd)
            throw new InvalidDataException("The native table archive index points outside the rows section.");

        stream.Position = rowOffset;
        var lengthBuffer = new byte[sizeof(int)];
        await stream.ReadExactlyAsync(lengthBuffer, ct);
        int length = BinaryPrimitives.ReadInt32LittleEndian(lengthBuffer);
        if (length <= 0 || length > MaxNativeRowBytes || rowOffset + sizeof(int) + length > rowsEnd)
            throw new InvalidDataException("The native table archive indexed row length is invalid.");

        byte[] record = GC.AllocateUninitializedArray<byte>(length);
        await stream.ReadExactlyAsync(record, ct);
        return RecordEncoder.Decode(record);
    }

    internal static void ValidateIndexPageOffset(NativeTableArchiveHeader header, long pageOffset)
    {
        if (pageOffset < TableArchiveNativeFormat.IndexHeaderSize ||
            pageOffset + TableArchiveNativeFormat.IndexPageSize > header.IndexLength)
        {
            throw new InvalidDataException("The native table archive index page offset is invalid.");
        }
    }

    internal static int BinarySearchLeafEntry(byte[] page, int entryCount, long key)
    {
        int lo = 0;
        int hi = entryCount - 1;
        while (lo <= hi)
        {
            int mid = lo + ((hi - lo) >> 1);
            long candidate = ReadIndexEntryKey(page, mid);
            if (candidate == key)
                return mid;
            if (candidate < key)
                lo = mid + 1;
            else
                hi = mid - 1;
        }

        return -1;
    }

    internal static int FindInteriorChildIndex(byte[] page, int entryCount, long key)
    {
        int lo = 0;
        int hi = entryCount;
        while (lo < hi)
        {
            int mid = lo + ((hi - lo) >> 1);
            long maxKey = ReadIndexEntryKey(page, mid);
            if (key <= maxKey)
                hi = mid;
            else
                lo = mid + 1;
        }

        return lo < entryCount ? lo : -1;
    }

    private static long ReadIndexEntryKey(byte[] page, int index)
    {
        int offset = TableArchiveNativeFormat.IndexPageHeaderSize + index * TableArchiveNativeFormat.IndexEntrySize;
        return BinaryPrimitives.ReadInt64LittleEndian(page.AsSpan(offset, sizeof(long)));
    }

    internal static long ReadIndexEntryValue(byte[] page, int index)
    {
        int offset = TableArchiveNativeFormat.IndexPageHeaderSize + index * TableArchiveNativeFormat.IndexEntrySize + sizeof(long);
        return BinaryPrimitives.ReadInt64LittleEndian(page.AsSpan(offset, sizeof(long)));
    }

}
