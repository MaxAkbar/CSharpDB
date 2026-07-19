using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Text.Json;
using CSharpDB.ImportExport.Models;
using CSharpDB.ImportExport.Serialization;
using CSharpDB.Primitives;
using CSharpDB.Storage.Serialization;

namespace CSharpDB.ImportExport.TableArchives;

public static class TableArchiveReader
{
    private const int MaxNativeRowBytes = 256 * 1024 * 1024;

    public static async ValueTask<TableArchiveManifest> ReadManifestAsync(
        string path,
        CancellationToken ct = default)
    {
        await using var stream = OpenRead(path);
        NativeTableArchiveHeader header = await ReadNativeHeaderAsync(stream, ct);
        return await ReadNativeManifestAsync(stream, header, ct);
    }

    public static async ValueTask<(TableArchiveSchema Schema, TableArchiveManifest Manifest)> ReadMetadataAsync(
        string path,
        CancellationToken ct = default)
    {
        await using var stream = OpenRead(path);
        NativeTableArchiveHeader header = await ReadNativeHeaderAsync(stream, ct);
        TableArchiveSchema schema = await ReadNativeSchemaAsync(stream, header, ct);
        TableArchiveManifest manifest = await ReadNativeManifestAsync(stream, header, ct);
        return (schema, manifest);
    }

    public static async ValueTask<TableArchiveSchema> ReadArchiveSchemaAsync(
        string path,
        CancellationToken ct = default)
    {
        await using var stream = OpenRead(path);
        NativeTableArchiveHeader header = await ReadNativeHeaderAsync(stream, ct);
        TableArchiveSchema schema = await ReadNativeSchemaAsync(stream, header, ct);
        _ = await ReadNativeManifestAsync(stream, header, ct);
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
        NativeTableArchiveHeader header = await ReadNativeHeaderAsync(stream, ct);
        _ = await ReadNativeSchemaAsync(stream, header, ct);
        _ = await ReadNativeManifestAsync(stream, header, ct);
        await foreach (DbValue[] row in ReadNativeRowsAsync(stream, header, ct))
            yield return row;
    }

    public static async ValueTask<bool> HasIntegerPrimaryKeyIndexAsync(
        string path,
        CancellationToken ct = default)
    {
        await using var stream = OpenRead(path);
        NativeTableArchiveHeader header = await ReadNativeHeaderAsync(stream, ct);
        _ = await ReadNativeSchemaAsync(stream, header, ct);
        _ = await ReadNativeManifestAsync(stream, header, ct);
        if (header.IndexLength <= 0)
            return false;

        NativeTableArchiveIndexHeader indexHeader = await ReadNativeIndexHeaderAsync(stream, header, ct);
        return indexHeader.KeyColumnIndex >= 0;
    }

    public static async ValueTask<TableArchiveRowLookupResult> LookupIntegerPrimaryKeyAsync(
        string path,
        long key,
        CancellationToken ct = default)
    {
        await using var stream = OpenRead(path);
        NativeTableArchiveHeader header = await ReadNativeHeaderAsync(stream, ct);
        _ = await ReadNativeSchemaAsync(stream, header, ct);
        _ = await ReadNativeManifestAsync(stream, header, ct);
        if (header.IndexLength <= 0)
            return new TableArchiveRowLookupResult(IsIndexed: false, Row: null);

        NativeTableArchiveIndexHeader indexHeader = await ReadNativeIndexHeaderAsync(stream, header, ct);
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

    private static async ValueTask<NativeTableArchiveHeader> ReadNativeHeaderAsync(
        Stream stream,
        CancellationToken ct)
        => await TryReadNativeHeaderAsync(stream, ct)
           ?? throw new InvalidDataException("The table archive is not a native CSharpDB table archive.");

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

    private static async ValueTask<TableArchiveSchema> ReadNativeSchemaAsync(
        Stream stream,
        NativeTableArchiveHeader header,
        CancellationToken ct)
    {
        byte[] bytes = await ReadSectionAsync(stream, header.SchemaOffset, header.SchemaLength, ct);
        TableArchiveSchema schema =
            JsonSerializer.Deserialize<TableArchiveSchema>(bytes, TableArchiveJson.Options)
            ?? throw new InvalidDataException("The table archive schema is empty.");
        if (schema.Columns.Any(static column => column.IsRowVersion) &&
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
                TableArchiveManifest.RowVersionFormatVersion))
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
            yield return RecordEncoder.Decode(record);
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
