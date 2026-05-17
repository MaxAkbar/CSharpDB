using System.Buffers.Binary;

namespace CSharpDB.ImportExport.Serialization;

internal readonly record struct NativeTableArchiveHeader(
    int FormatVersion,
    long SchemaOffset,
    int SchemaLength,
    long ManifestOffset,
    int ManifestLength,
    long RowsOffset,
    long RowsLength,
    long RowCount,
    long IndexOffset,
    long IndexLength);

internal readonly record struct NativeTableArchiveIndexHeader(
    int Version,
    int KeyColumnIndex,
    int PageSize,
    long RootPageOffset,
    long PageCount,
    long EntryCount);

internal static class TableArchiveNativeFormat
{
    public const int FormatVersion = 3;
    public const int HeaderSize = 76;

    public const int PrimaryKeyIndexVersion = 1;
    public const int IndexHeaderSize = 48;
    public const int IndexPageSize = 4096;
    public const int IndexPageHeaderSize = 24;
    public const int IndexEntrySize = 16;
    public const byte IndexLeafPageType = 1;
    public const byte IndexInteriorPageType = 2;

    private static ReadOnlySpan<byte> Magic => "CSDBTBL3"u8;
    private static ReadOnlySpan<byte> IndexMagic => "CSDBIDX1"u8;

    public static bool IsMagic(ReadOnlySpan<byte> value)
        => value.Length >= Magic.Length && value[..Magic.Length].SequenceEqual(Magic);

    public static async ValueTask WriteHeaderAsync(
        Stream stream,
        NativeTableArchiveHeader header,
        CancellationToken ct)
    {
        var buffer = new byte[HeaderSize];
        Magic.CopyTo(buffer);
        BinaryPrimitives.WriteInt32LittleEndian(buffer.AsSpan(8), header.FormatVersion);
        BinaryPrimitives.WriteInt64LittleEndian(buffer.AsSpan(12), header.SchemaOffset);
        BinaryPrimitives.WriteInt32LittleEndian(buffer.AsSpan(20), header.SchemaLength);
        BinaryPrimitives.WriteInt64LittleEndian(buffer.AsSpan(24), header.ManifestOffset);
        BinaryPrimitives.WriteInt32LittleEndian(buffer.AsSpan(32), header.ManifestLength);
        BinaryPrimitives.WriteInt64LittleEndian(buffer.AsSpan(36), header.RowsOffset);
        BinaryPrimitives.WriteInt64LittleEndian(buffer.AsSpan(44), header.RowsLength);
        BinaryPrimitives.WriteInt64LittleEndian(buffer.AsSpan(52), header.RowCount);
        BinaryPrimitives.WriteInt64LittleEndian(buffer.AsSpan(60), header.IndexOffset);
        BinaryPrimitives.WriteInt64LittleEndian(buffer.AsSpan(68), header.IndexLength);
        await stream.WriteAsync(buffer, ct);
    }

    public static async ValueTask<NativeTableArchiveHeader> ReadHeaderAsync(Stream stream, CancellationToken ct)
    {
        var buffer = new byte[HeaderSize];
        await stream.ReadExactlyAsync(buffer, ct);

        if (!IsMagic(buffer))
            throw new InvalidDataException("The table archive is not a native CSharpDB table archive.");

        int version = BinaryPrimitives.ReadInt32LittleEndian(buffer.AsSpan(8));
        if (version != FormatVersion)
            throw new InvalidDataException($"Unsupported native table archive format version {version}.");

        var header = new NativeTableArchiveHeader(
            version,
            BinaryPrimitives.ReadInt64LittleEndian(buffer.AsSpan(12)),
            BinaryPrimitives.ReadInt32LittleEndian(buffer.AsSpan(20)),
            BinaryPrimitives.ReadInt64LittleEndian(buffer.AsSpan(24)),
            BinaryPrimitives.ReadInt32LittleEndian(buffer.AsSpan(32)),
            BinaryPrimitives.ReadInt64LittleEndian(buffer.AsSpan(36)),
            BinaryPrimitives.ReadInt64LittleEndian(buffer.AsSpan(44)),
            BinaryPrimitives.ReadInt64LittleEndian(buffer.AsSpan(52)),
            BinaryPrimitives.ReadInt64LittleEndian(buffer.AsSpan(60)),
            BinaryPrimitives.ReadInt64LittleEndian(buffer.AsSpan(68)));

        Validate(header);
        return header;
    }

    public static void Validate(NativeTableArchiveHeader header)
    {
        if (header.FormatVersion != FormatVersion ||
            header.SchemaOffset < HeaderSize ||
            header.SchemaLength <= 0 ||
            header.ManifestOffset < HeaderSize ||
            header.ManifestLength <= 0 ||
            header.RowsOffset < HeaderSize ||
            header.RowsLength < 0 ||
            header.RowCount < 0 ||
            header.IndexOffset < 0 ||
            header.IndexLength < 0)
        {
            throw new InvalidDataException("The native table archive header is invalid.");
        }
    }

    public static void WriteIndexHeader(
        Span<byte> destination,
        NativeTableArchiveIndexHeader header)
    {
        if (destination.Length < IndexHeaderSize)
            throw new ArgumentException("The destination is too small for an archive index header.", nameof(destination));

        destination[..IndexHeaderSize].Clear();
        IndexMagic.CopyTo(destination);
        BinaryPrimitives.WriteInt32LittleEndian(destination[8..], header.Version);
        BinaryPrimitives.WriteInt32LittleEndian(destination[12..], header.KeyColumnIndex);
        BinaryPrimitives.WriteInt32LittleEndian(destination[16..], header.PageSize);
        BinaryPrimitives.WriteInt64LittleEndian(destination[24..], header.RootPageOffset);
        BinaryPrimitives.WriteInt64LittleEndian(destination[32..], header.PageCount);
        BinaryPrimitives.WriteInt64LittleEndian(destination[40..], header.EntryCount);
    }

    public static NativeTableArchiveIndexHeader ReadIndexHeader(ReadOnlySpan<byte> source)
    {
        if (source.Length < IndexHeaderSize || !source[..IndexMagic.Length].SequenceEqual(IndexMagic))
            throw new InvalidDataException("The native table archive index header is invalid.");

        var header = new NativeTableArchiveIndexHeader(
            BinaryPrimitives.ReadInt32LittleEndian(source[8..]),
            BinaryPrimitives.ReadInt32LittleEndian(source[12..]),
            BinaryPrimitives.ReadInt32LittleEndian(source[16..]),
            BinaryPrimitives.ReadInt64LittleEndian(source[24..]),
            BinaryPrimitives.ReadInt64LittleEndian(source[32..]),
            BinaryPrimitives.ReadInt64LittleEndian(source[40..]));

        if (header.Version != PrimaryKeyIndexVersion ||
            header.KeyColumnIndex < 0 ||
            header.PageSize != IndexPageSize ||
            header.RootPageOffset < 0 ||
            header.PageCount < 0 ||
            header.EntryCount < 0)
        {
            throw new InvalidDataException("The native table archive index header is invalid.");
        }

        return header;
    }

    public static void WriteIndexPageHeader(
        Span<byte> destination,
        byte pageType,
        int entryCount,
        long nextLeafPageOffset)
    {
        if (destination.Length < IndexPageHeaderSize)
            throw new ArgumentException("The destination is too small for an archive index page header.", nameof(destination));
        if (pageType is not (IndexLeafPageType or IndexInteriorPageType))
            throw new InvalidDataException("The native table archive index page type is invalid.");
        if (entryCount < 0 || entryCount > MaxIndexEntriesPerPage)
            throw new InvalidDataException("The native table archive index page entry count is invalid.");

        destination[..IndexPageHeaderSize].Clear();
        destination[0] = pageType;
        BinaryPrimitives.WriteInt32LittleEndian(destination[4..], entryCount);
        BinaryPrimitives.WriteInt64LittleEndian(destination[8..], nextLeafPageOffset);
    }

    public static (byte PageType, int EntryCount, long NextLeafPageOffset) ReadIndexPageHeader(ReadOnlySpan<byte> source)
    {
        if (source.Length < IndexPageHeaderSize)
            throw new InvalidDataException("The native table archive index page is truncated.");

        byte pageType = source[0];
        int entryCount = BinaryPrimitives.ReadInt32LittleEndian(source[4..]);
        long nextLeafPageOffset = BinaryPrimitives.ReadInt64LittleEndian(source[8..]);
        if (pageType is not (IndexLeafPageType or IndexInteriorPageType) ||
            entryCount < 0 ||
            entryCount > MaxIndexEntriesPerPage ||
            nextLeafPageOffset < 0)
        {
            throw new InvalidDataException("The native table archive index page header is invalid.");
        }

        return (pageType, entryCount, nextLeafPageOffset);
    }

    public static int MaxIndexEntriesPerPage => (IndexPageSize - IndexPageHeaderSize) / IndexEntrySize;
}
