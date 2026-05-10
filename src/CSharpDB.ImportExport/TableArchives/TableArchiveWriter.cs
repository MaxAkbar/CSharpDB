using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Text.Json;
using CSharpDB.ImportExport.Models;
using CSharpDB.ImportExport.Serialization;
using CSharpDB.Primitives;
using CSharpDB.Storage.Serialization;

namespace CSharpDB.ImportExport.TableArchives;

public static class TableArchiveWriter
{
    private const int CooperativeYieldIntervalRows = 4096;
    private const int CooperativeYieldIntervalPages = 64;
    private const int FileBufferSize = 1024 * 1024;

    public static async ValueTask<TableArchiveManifest> WriteAsync(
        string path,
        TableSchema schema,
        IAsyncEnumerable<DbValue[]> rows,
        CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);

        string? directory = Path.GetDirectoryName(Path.GetFullPath(path));
        if (!string.IsNullOrWhiteSpace(directory))
            Directory.CreateDirectory(directory);

        await using var stream = new FileStream(
            path,
            FileMode.Create,
            FileAccess.ReadWrite,
            FileShare.None,
            FileBufferSize,
            FileOptions.Asynchronous | FileOptions.SequentialScan);
        return await WriteAsync(stream, schema, rows, ct);
    }

    public static async ValueTask<TableArchiveManifest> WriteAsync(
        Stream destination,
        TableSchema schema,
        IAsyncEnumerable<DbValue[]> rows,
        CancellationToken ct = default)
        => await WriteNativeAsync(destination, schema, rows, ct);

    private static async ValueTask<TableArchiveManifest> WriteNativeAsync(
        Stream destination,
        TableSchema schema,
        IAsyncEnumerable<DbValue[]> rows,
        CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(destination);
        ArgumentNullException.ThrowIfNull(schema);
        ArgumentNullException.ThrowIfNull(rows);
        if (!destination.CanSeek)
            throw new ArgumentException("Native table archives require a seekable destination stream.", nameof(destination));

        byte[] emptyHeader = new byte[TableArchiveNativeFormat.HeaderSize];
        await destination.WriteAsync(emptyHeader, ct);

        long schemaOffset = destination.Position;
        byte[] schemaBytes = JsonSerializer.SerializeToUtf8Bytes(
            TableArchiveSchema.FromTableSchema(schema),
            TableArchiveJson.Options);
        await destination.WriteAsync(schemaBytes, ct);

        long rowsOffset = destination.Position;
        long rowCount = 0;
        int primaryKeyColumnIndex = FindIntegerPrimaryKeyColumnIndex(schema);
        NativePrimaryKeyIndexBuilder? primaryKeyIndexBuilder = primaryKeyColumnIndex >= 0
            ? new NativePrimaryKeyIndexBuilder(primaryKeyColumnIndex)
            : null;
        var lengthBuffer = new byte[sizeof(int)];
        await foreach (DbValue[] row in rows.WithCancellation(ct).ConfigureAwait(false))
        {
            ct.ThrowIfCancellationRequested();
            DbValue[] normalizedRow = NormalizeRow(schema, row);
            int encodedLength = RecordEncoder.GetEncodedLength(normalizedRow);
            byte[] recordBuffer = ArrayPool<byte>.Shared.Rent(encodedLength);
            try
            {
                RecordEncoder.EncodeInto(normalizedRow, recordBuffer.AsSpan(0, encodedLength), encodedLength);
                BinaryPrimitives.WriteInt32LittleEndian(lengthBuffer, encodedLength);
                long rowOffset = destination.Position;
                await destination.WriteAsync(lengthBuffer, ct);
                await destination.WriteAsync(recordBuffer.AsMemory(0, encodedLength), ct);
                primaryKeyIndexBuilder?.Add(normalizedRow, rowOffset);
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

        var manifest = new TableArchiveManifest
        {
            FormatVersion = TableArchiveManifest.CurrentFormatVersion,
            SourceTableName = schema.TableName,
            CreatedUtc = DateTimeOffset.UtcNow,
            RowCount = rowCount,
            SchemaEntry = "native:schema",
            RowsEntry = "native:rows",
            Indexes = indexes,
        };
        long manifestOffset = destination.Position;
        byte[] manifestBytes = JsonSerializer.SerializeToUtf8Bytes(manifest, TableArchiveJson.Options);
        await destination.WriteAsync(manifestBytes, ct);

        destination.Position = 0;
        await TableArchiveNativeFormat.WriteHeaderAsync(
            destination,
            new NativeTableArchiveHeader(
                TableArchiveNativeFormat.FormatVersion,
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

    private static DbValue[] NormalizeRow(TableSchema schema, DbValue[] row)
    {
        if (row.Length == schema.Columns.Count)
            return row;

        var normalized = new DbValue[schema.Columns.Count];
        int copied = Math.Min(row.Length, normalized.Length);
        Array.Copy(row, normalized, copied);
        for (int i = copied; i < normalized.Length; i++)
            normalized[i] = DbValue.Null;
        return normalized;
    }

    private readonly record struct NativePrimaryKeyIndexEntry(long Key, long RowOffset);

    private readonly record struct NativePrimaryKeyIndexNode(long MaxKey, long PageOffset);

    private readonly record struct NativePrimaryKeyIndexWriteResult(long EntryCount);

    private sealed class NativePrimaryKeyIndexBuilder(int keyColumnIndex)
    {
        private readonly List<NativePrimaryKeyIndexEntry> _entries = [];
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
            entries = _entries;
            ct.ThrowIfCancellationRequested();
            if (!_isValid)
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
