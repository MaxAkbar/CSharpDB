using CSharpDB.ImportExport.Serialization;
using CSharpDB.Primitives;

namespace CSharpDB.ImportExport.TableArchives;

public sealed class TableArchivePrimaryKeyLookupReader : IAsyncDisposable
{
    private readonly FileStream _stream;
    private readonly NativeTableArchiveHeader _header;
    private readonly NativeTableArchiveIndexHeader _indexHeader;
    private readonly byte[] _page;

    private TableArchivePrimaryKeyLookupReader(
        FileStream stream,
        NativeTableArchiveHeader header,
        NativeTableArchiveIndexHeader indexHeader)
    {
        _stream = stream;
        _header = header;
        _indexHeader = indexHeader;
        _page = new byte[TableArchiveNativeFormat.IndexPageSize];
    }

    public int KeyColumnIndex => _indexHeader.KeyColumnIndex;

    public static async ValueTask<TableArchivePrimaryKeyLookupReader?> TryOpenAsync(
        string path,
        CancellationToken ct = default)
    {
        FileStream stream = TableArchiveReader.OpenRead(path);
        try
        {
            NativeTableArchiveHeader? nativeHeader = await TableArchiveReader.TryReadNativeHeaderAsync(stream, ct);
            if (nativeHeader is not { } header || header.IndexLength <= 0)
            {
                await stream.DisposeAsync();
                return null;
            }

            NativeTableArchiveIndexHeader indexHeader = await TableArchiveReader.ReadNativeIndexHeaderAsync(stream, header, ct);
            return new TableArchivePrimaryKeyLookupReader(stream, header, indexHeader);
        }
        catch
        {
            await stream.DisposeAsync();
            throw;
        }
    }

    public async ValueTask<DbValue[]?> LookupAsync(long key, CancellationToken ct = default)
    {
        if (_indexHeader.EntryCount == 0)
            return null;

        long pageOffset = _indexHeader.RootPageOffset;
        while (true)
        {
            TableArchiveReader.ValidateIndexPageOffset(_header, pageOffset);
            _stream.Position = _header.IndexOffset + pageOffset;
            await _stream.ReadExactlyAsync(_page, ct);

            var pageHeader = TableArchiveNativeFormat.ReadIndexPageHeader(_page);
            if (pageHeader.EntryCount == 0)
                return null;

            if (pageHeader.PageType == TableArchiveNativeFormat.IndexLeafPageType)
            {
                int entryIndex = TableArchiveReader.BinarySearchLeafEntry(_page, pageHeader.EntryCount, key);
                if (entryIndex < 0)
                    return null;

                long rowOffset = TableArchiveReader.ReadIndexEntryValue(_page, entryIndex);
                return await TableArchiveReader.ReadNativeRowAtOffsetAsync(_stream, _header, rowOffset, ct);
            }

            int childIndex = TableArchiveReader.FindInteriorChildIndex(_page, pageHeader.EntryCount, key);
            if (childIndex < 0)
                return null;

            pageOffset = TableArchiveReader.ReadIndexEntryValue(_page, childIndex);
        }
    }

    public ValueTask DisposeAsync() => _stream.DisposeAsync();
}
