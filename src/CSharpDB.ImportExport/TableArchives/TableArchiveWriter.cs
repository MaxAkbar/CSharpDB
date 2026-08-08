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

public static class TableArchiveWriter
{
    /// <summary>
    /// Maximum number of integer primary-key entries retained while building the
    /// optional physical lookup index. Larger archives are written without that
    /// acceleration structure and remain readable through the scan fallback.
    /// </summary>
    public const int MaximumInMemoryPrimaryKeyIndexEntries = 65_536;

    private const int CooperativeYieldIntervalRows = 4096;
    private const int CooperativeYieldIntervalPages = 64;
    private const int FileBufferSize = 1024 * 1024;

    public static async ValueTask<TableArchiveManifest> WriteAsync(
        string path,
        TableSchema schema,
        IAsyncEnumerable<DbValue[]> rows,
        CancellationToken ct = default)
        => await WriteAsync(path, schema, Array.Empty<IndexSchema>(), rows, ct);

    public static async ValueTask<TableArchiveManifest> WriteAsync(
        string path,
        TableSchema schema,
        IReadOnlyList<IndexSchema> secondaryIndexes,
        IAsyncEnumerable<DbValue[]> rows,
        CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);
        ArgumentNullException.ThrowIfNull(secondaryIndexes);

        string fullPath = Path.GetFullPath(path);
        string? directory = Path.GetDirectoryName(fullPath);
        if (string.IsNullOrWhiteSpace(directory))
            throw new ArgumentException("The archive path must have a parent directory.", nameof(path));
        Directory.CreateDirectory(directory);

        string temporaryPath = Path.Combine(
            directory,
            $".{Path.GetFileName(fullPath)}.{Guid.NewGuid():N}.tmp");
        bool published = false;
        try
        {
            TableArchiveManifest manifest;
            await using (var stream = new FileStream(
                temporaryPath,
                FileMode.CreateNew,
                FileAccess.ReadWrite,
                FileShare.None,
                FileBufferSize,
                FileOptions.Asynchronous | FileOptions.SequentialScan))
            {
                manifest = await WriteAsync(stream, schema, secondaryIndexes, rows, ct);
                await stream.FlushAsync(ct);
                stream.Flush(flushToDisk: true);
            }

            ct.ThrowIfCancellationRequested();
            File.Move(temporaryPath, fullPath, overwrite: true);
            published = true;
            return manifest;
        }
        finally
        {
            if (!published)
                TryDelete(temporaryPath);
        }
    }

    public static async ValueTask<TableArchiveManifest> WriteAsync(
        Stream destination,
        TableSchema schema,
        IAsyncEnumerable<DbValue[]> rows,
        CancellationToken ct = default)
        => await WriteAsync(destination, schema, Array.Empty<IndexSchema>(), rows, ct);

    public static async ValueTask<TableArchiveManifest> WriteAsync(
        Stream destination,
        TableSchema schema,
        IReadOnlyList<IndexSchema> secondaryIndexes,
        IAsyncEnumerable<DbValue[]> rows,
        CancellationToken ct = default)
        => await WriteNativeAsync(destination, schema, secondaryIndexes, rows, ct);

    private static async ValueTask<TableArchiveManifest> WriteNativeAsync(
        Stream destination,
        TableSchema schema,
        IReadOnlyList<IndexSchema> secondaryIndexes,
        IAsyncEnumerable<DbValue[]> rows,
        CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(destination);
        ArgumentNullException.ThrowIfNull(schema);
        ArgumentNullException.ThrowIfNull(secondaryIndexes);
        ArgumentNullException.ThrowIfNull(rows);
        if (!destination.CanSeek)
            throw new ArgumentException("Native table archives require a seekable destination stream.", nameof(destination));
        if (!destination.CanWrite)
            throw new ArgumentException("Native table archives require a writable destination stream.", nameof(destination));

        ValidateSchemaTypes(schema);

        int primaryKeyColumnIndex = FindIntegerPrimaryKeyColumnIndex(schema);
        if (primaryKeyColumnIndex >= 0 && !destination.CanRead)
        {
            throw new ArgumentException(
                "Integrity-protected native table archives with a physical index require a readable destination stream.",
                nameof(destination));
        }

        destination.Position = 0;
        destination.SetLength(0);

        byte[] emptyHeader = new byte[TableArchiveNativeFormat.HeaderSize];
        await destination.WriteAsync(emptyHeader, ct);

        long schemaOffset = destination.Position;
        TableArchiveSchema archiveSchema = TableArchiveSchema.FromTableSchema(schema, secondaryIndexes);
        byte[] schemaBytes = JsonSerializer.SerializeToUtf8Bytes(
            archiveSchema,
            TableArchiveJson.Options);
        await destination.WriteAsync(schemaBytes, ct);
        string schemaDigest = EncodeDigest(SHA256.HashData(schemaBytes));

        long rowsOffset = destination.Position;
        long rowCount = 0;
        NativePrimaryKeyIndexBuilder? primaryKeyIndexBuilder = primaryKeyColumnIndex >= 0
            ? new NativePrimaryKeyIndexBuilder(primaryKeyColumnIndex)
            : null;
        var lengthBuffer = new byte[sizeof(int)];
        using var rowsHasher = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        await foreach (DbValue[] row in rows.WithCancellation(ct).ConfigureAwait(false))
        {
            ct.ThrowIfCancellationRequested();
            DbValue[] validatedRow = ValidateRow(schema, row, rowCount);
            int encodedLength = RecordEncoder.GetEncodedLength(validatedRow);
            byte[] recordBuffer = ArrayPool<byte>.Shared.Rent(encodedLength);
            try
            {
                RecordEncoder.EncodeInto(validatedRow, recordBuffer.AsSpan(0, encodedLength), encodedLength);
                BinaryPrimitives.WriteInt32LittleEndian(lengthBuffer, encodedLength);
                long rowOffset = destination.Position;
                await destination.WriteAsync(lengthBuffer, ct);
                await destination.WriteAsync(recordBuffer.AsMemory(0, encodedLength), ct);
                rowsHasher.AppendData(lengthBuffer);
                rowsHasher.AppendData(recordBuffer.AsSpan(0, encodedLength));
                primaryKeyIndexBuilder?.Add(validatedRow, rowOffset);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(recordBuffer);
            }

            rowCount++;
            if (rowCount % CooperativeYieldIntervalRows == 0)
            {
                ct.ThrowIfCancellationRequested();
                await Task.Yield();
            }
        }

        long rowsLength = destination.Position - rowsOffset;
        string rowsDigest = EncodeDigest(rowsHasher.GetHashAndReset());
        long indexOffset = 0;
        long indexLength = 0;
        TableArchiveIndexManifest[] indexes = Array.Empty<TableArchiveIndexManifest>();
        if (primaryKeyIndexBuilder?.TryBuildEntries(ct, out List<NativePrimaryKeyIndexEntry>? primaryKeyEntries) == true)
        {
            indexOffset = destination.Position;
            NativePrimaryKeyIndexWriteResult index = await WriteNativePrimaryKeyIndexAsync(
                destination,
                primaryKeyColumnIndex,
                primaryKeyEntries,
                ct);
            indexLength = destination.Position - indexOffset;
            indexes =
            [
                new TableArchiveIndexManifest
                {
                    Name = $"{schema.TableName}_pk",
                    Kind = "primary-key",
                    ColumnName = schema.Columns[primaryKeyColumnIndex].Name,
                    ColumnIndex = primaryKeyColumnIndex,
                    EntryCount = index.EntryCount,
                },
            ];
        }

        string physicalIndexDigest = indexLength == 0
            ? EncodeDigest(SHA256.HashData(ReadOnlySpan<byte>.Empty))
            : await ComputeSectionDigestAsync(destination, indexOffset, indexLength, ct);
        const int archiveFormatVersion = TableArchiveManifest.LatestFormatVersion;
        var manifest = new TableArchiveManifest
        {
            FormatVersion = archiveFormatVersion,
            SourceTableName = schema.TableName,
            CreatedUtc = DateTimeOffset.UtcNow,
            RowCount = rowCount,
            SchemaEntry = "native:schema",
            RowsEntry = "native:rows",
            Indexes = indexes,
            Digests = new TableArchiveSectionDigests
            {
                Algorithm = TableArchiveSectionDigests.Sha256Algorithm,
                Encoding = TableArchiveSectionDigests.LowercaseHexEncoding,
                Schema = schemaDigest,
                Rows = rowsDigest,
                PhysicalIndex = physicalIndexDigest,
            },
        };
        long manifestOffset = destination.Position;
        byte[] manifestBytes = JsonSerializer.SerializeToUtf8Bytes(manifest, TableArchiveJson.Options);
        await destination.WriteAsync(manifestBytes, ct);

        destination.Position = 0;
        await TableArchiveNativeFormat.WriteHeaderAsync(
            destination,
            new NativeTableArchiveHeader(
                archiveFormatVersion,
                schemaOffset,
                schemaBytes.Length,
                manifestOffset,
                manifestBytes.Length,
                rowsOffset,
                rowsLength,
                rowCount,
                indexOffset,
                indexLength),
            ct);
        destination.Position = destination.Length;
        return manifest;
    }

    private static async ValueTask<string> ComputeSectionDigestAsync(
        Stream stream,
        long offset,
        long length,
        CancellationToken ct)
    {
        const int bufferSize = 64 * 1024;
        byte[] buffer = ArrayPool<byte>.Shared.Rent(bufferSize);
        long originalPosition = stream.Position;
        using var hasher = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        try
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

            return EncodeDigest(hasher.GetHashAndReset());
        }
        finally
        {
            stream.Position = originalPosition;
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private static string EncodeDigest(ReadOnlySpan<byte> digest)
        => Convert.ToHexString(digest).ToLowerInvariant();

    private static void TryDelete(string path)
    {
        try
        {
            File.Delete(path);
        }
        catch (IOException)
        {
        }
        catch (UnauthorizedAccessException)
        {
        }
    }

    private static int FindIntegerPrimaryKeyColumnIndex(TableSchema schema)
    {
        int index = schema.PrimaryKeyColumnIndex;
        return index >= 0 &&
               index < schema.Columns.Count &&
               schema.Columns[index].Type == DbType.Integer
            ? index
            : -1;
    }

    private static async ValueTask<NativePrimaryKeyIndexWriteResult> WriteNativePrimaryKeyIndexAsync(
        Stream destination,
        int keyColumnIndex,
        IReadOnlyList<NativePrimaryKeyIndexEntry> entries,
        CancellationToken ct)
    {
        long indexStart = destination.Position;
        await destination.WriteAsync(new byte[TableArchiveNativeFormat.IndexHeaderSize], ct);

        long rootPageOffset = 0;
        long pageCount = 0;
        if (entries.Count > 0)
        {
            List<NativePrimaryKeyIndexNode> level = await WriteNativePrimaryKeyLeafPagesAsync(
                destination,
                indexStart,
                entries,
                ct);
            pageCount += level.Count;

            while (level.Count > 1)
            {
                level = await WriteNativePrimaryKeyInteriorPagesAsync(destination, indexStart, level, ct);
                pageCount += level.Count;
            }

            rootPageOffset = level[0].PageOffset;
        }

        long end = destination.Position;
        var headerBuffer = new byte[TableArchiveNativeFormat.IndexHeaderSize];
        TableArchiveNativeFormat.WriteIndexHeader(
            headerBuffer,
            new NativeTableArchiveIndexHeader(
                TableArchiveNativeFormat.PrimaryKeyIndexVersion,
                keyColumnIndex,
                TableArchiveNativeFormat.IndexPageSize,
                rootPageOffset,
                pageCount,
                entries.Count));

        destination.Position = indexStart;
        await destination.WriteAsync(headerBuffer, ct);
        destination.Position = end;
        return new NativePrimaryKeyIndexWriteResult(entries.Count);
    }

    private static async ValueTask<List<NativePrimaryKeyIndexNode>> WriteNativePrimaryKeyLeafPagesAsync(
        Stream destination,
        long indexStart,
        IReadOnlyList<NativePrimaryKeyIndexEntry> entries,
        CancellationToken ct)
    {
        int pageCapacity = TableArchiveNativeFormat.MaxIndexEntriesPerPage;
        var nodes = new List<NativePrimaryKeyIndexNode>((entries.Count + pageCapacity - 1) / pageCapacity);
        var pageBuffer = new byte[TableArchiveNativeFormat.IndexPageSize];

        for (int start = 0; start < entries.Count; start += pageCapacity)
        {
            int count = Math.Min(pageCapacity, entries.Count - start);
            long pageOffset = destination.Position - indexStart;
            long nextLeafOffset = start + count < entries.Count
                ? pageOffset + TableArchiveNativeFormat.IndexPageSize
                : 0;

            pageBuffer.AsSpan().Clear();
            TableArchiveNativeFormat.WriteIndexPageHeader(
                pageBuffer,
                TableArchiveNativeFormat.IndexLeafPageType,
                count,
                nextLeafOffset);

            for (int i = 0; i < count; i++)
            {
                NativePrimaryKeyIndexEntry entry = entries[start + i];
                Span<byte> target = pageBuffer.AsSpan(
                    TableArchiveNativeFormat.IndexPageHeaderSize + i * TableArchiveNativeFormat.IndexEntrySize,
                    TableArchiveNativeFormat.IndexEntrySize);
                BinaryPrimitives.WriteInt64LittleEndian(target, entry.Key);
                BinaryPrimitives.WriteInt64LittleEndian(target[8..], entry.RowOffset);
            }

            await destination.WriteAsync(pageBuffer, ct);
            nodes.Add(new NativePrimaryKeyIndexNode(entries[start + count - 1].Key, pageOffset));
            if (nodes.Count % CooperativeYieldIntervalPages == 0)
            {
                ct.ThrowIfCancellationRequested();
                await Task.Yield();
            }
        }

        return nodes;
    }

    private static async ValueTask<List<NativePrimaryKeyIndexNode>> WriteNativePrimaryKeyInteriorPagesAsync(
        Stream destination,
        long indexStart,
        IReadOnlyList<NativePrimaryKeyIndexNode> children,
        CancellationToken ct)
    {
        int pageCapacity = TableArchiveNativeFormat.MaxIndexEntriesPerPage;
        var nodes = new List<NativePrimaryKeyIndexNode>((children.Count + pageCapacity - 1) / pageCapacity);
        var pageBuffer = new byte[TableArchiveNativeFormat.IndexPageSize];

        for (int start = 0; start < children.Count; start += pageCapacity)
        {
            int count = Math.Min(pageCapacity, children.Count - start);
            long pageOffset = destination.Position - indexStart;

            pageBuffer.AsSpan().Clear();
            TableArchiveNativeFormat.WriteIndexPageHeader(
                pageBuffer,
                TableArchiveNativeFormat.IndexInteriorPageType,
                count,
                nextLeafPageOffset: 0);

            for (int i = 0; i < count; i++)
            {
                NativePrimaryKeyIndexNode child = children[start + i];
                Span<byte> target = pageBuffer.AsSpan(
                    TableArchiveNativeFormat.IndexPageHeaderSize + i * TableArchiveNativeFormat.IndexEntrySize,
                    TableArchiveNativeFormat.IndexEntrySize);
                BinaryPrimitives.WriteInt64LittleEndian(target, child.MaxKey);
                BinaryPrimitives.WriteInt64LittleEndian(target[8..], child.PageOffset);
            }

            await destination.WriteAsync(pageBuffer, ct);
            nodes.Add(new NativePrimaryKeyIndexNode(children[start + count - 1].MaxKey, pageOffset));
            if (nodes.Count % CooperativeYieldIntervalPages == 0)
            {
                ct.ThrowIfCancellationRequested();
                await Task.Yield();
            }
        }

        return nodes;
    }

    public static async IAsyncEnumerable<DbValue[]> ToAsyncRows(
        IEnumerable<DbValue[]> rows,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        foreach (DbValue[] row in rows)
        {
            ct.ThrowIfCancellationRequested();
            yield return row;
            await Task.Yield();
        }
    }

    private static DbValue[] ValidateRow(TableSchema schema, DbValue[] row, long rowIndex)
    {
        if (row is null)
            throw new InvalidDataException($"Archive row {rowIndex} is null.");
        if (row.Length != schema.Columns.Count)
        {
            throw new InvalidDataException(
                $"Archive row {rowIndex} has {row.Length} values; expected {schema.Columns.Count}.");
        }

        for (int i = 0; i < row.Length; i++)
        {
            ColumnDefinition column = schema.Columns[i];
            DbValue value = row[i];
            if (value.IsNull)
            {
                if (!column.Nullable || column.IsPrimaryKey || column.IsRowVersion)
                {
                    throw new InvalidDataException(
                        $"Archive row {rowIndex}, column '{column.Name}' cannot be NULL.");
                }

                continue;
            }

            if (value.Type != column.Type)
            {
                throw new InvalidDataException(
                    $"Archive row {rowIndex}, column '{column.Name}' has value tag {value.Type}; expected {column.Type}.");
            }
        }

        return row;
    }

    private static void ValidateSchemaTypes(TableSchema schema)
    {
        if (schema.Columns.Count == 0)
            throw new InvalidDataException("The table archive schema has no columns.");

        foreach (ColumnDefinition column in schema.Columns)
        {
            if (column.Type == DbType.Null)
            {
                throw new InvalidDataException(
                    $"Archived column '{column.Name}' has an invalid persistent type.");
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
            if (column.IsRowVersion &&
                column.DeclaredType is { Kind: not SqlTypeKind.Blob })
            {
                throw new InvalidDataException(
                    $"Archived ROWVERSION column '{column.Name}' must be declared BLOB.");
            }
        }
    }

    private readonly record struct NativePrimaryKeyIndexEntry(long Key, long RowOffset);

    private readonly record struct NativePrimaryKeyIndexNode(long MaxKey, long PageOffset);

    private readonly record struct NativePrimaryKeyIndexWriteResult(long EntryCount);

    private sealed class NativePrimaryKeyIndexBuilder(int keyColumnIndex)
    {
        private List<NativePrimaryKeyIndexEntry>? _entries = [];
        private bool _isValid = true;
        private bool _isSorted = true;
        private bool _hasLastKey;
        private long _lastKey;

        public void Add(DbValue[] row, long rowOffset)
        {
            if (!_isValid)
                return;

            if (keyColumnIndex < 0 ||
                keyColumnIndex >= row.Length ||
                row[keyColumnIndex].Type != DbType.Integer)
            {
                _entries = null;
                _isValid = false;
                return;
            }

            if (_entries!.Count >= MaximumInMemoryPrimaryKeyIndexEntries)
            {
                _entries = null;
                _isValid = false;
                return;
            }

            long key = row[keyColumnIndex].AsInteger;
            if (_hasLastKey && key < _lastKey)
                _isSorted = false;

            _entries.Add(new NativePrimaryKeyIndexEntry(key, rowOffset));
            _lastKey = key;
            _hasLastKey = true;
        }

        public bool TryBuildEntries(CancellationToken ct, out List<NativePrimaryKeyIndexEntry> entries)
        {
            entries = _entries ?? [];
            ct.ThrowIfCancellationRequested();
            if (!_isValid || _entries is null)
                return false;

            if (!_isSorted)
                entries.Sort(static (left, right) => left.Key.CompareTo(right.Key));

            for (int i = 1; i < entries.Count; i++)
            {
                if (i % CooperativeYieldIntervalRows == 0)
                    ct.ThrowIfCancellationRequested();

                if (entries[i - 1].Key == entries[i].Key)
                    return false;
            }

            return true;
        }
    }
}
