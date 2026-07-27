using CSharpDB.ImportExport.Serialization;
using CSharpDB.Primitives;

namespace CSharpDB.ImportExport.TableArchives;

public sealed class TableArchivePrimaryKeyLookupReader : IAsyncDisposable
{
    private readonly FileStream _stream;
    private readonly NativeTableArchiveHeader _header;
    private readonly NativeTableArchiveIndexHeader _indexHeader;
    private readonly TableSchema _schema;
    private readonly byte[] _page;

    private TableArchivePrimaryKeyLookupReader(
        FileStream stream,
        NativeTableArchiveHeader header,
        NativeTableArchiveIndexHeader indexHeader,
        TableSchema schema)
    {
        _stream = stream;
        _header = header;
        _indexHeader = indexHeader;
        _schema = schema;
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
            var metadata = await TableArchiveReader.TryReadValidatedLookupMetadataAsync(stream, ct);
            if (metadata is not { } validated || validated.IndexHeader is not { } indexHeader)
            {
                await stream.DisposeAsync();
                return null;
            }

            return new TableArchivePrimaryKeyLookupReader(
                stream,
                validated.Header,
                indexHeader,
                validated.Schema);
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
                DbValue[] row = await TableArchiveReader.ReadNativeRowAtOffsetAsync(
                    _stream,
                    _header,
                    rowOffset,
                    ct);
                TableArchiveReader.ValidateRow(_schema, row, rowIndex: -1);
                DbValue indexedValue = row[_indexHeader.KeyColumnIndex];
                if (indexedValue.Type != DbType.Integer || indexedValue.AsInteger != key)
                {
                    throw new InvalidDataException(
                        "The native table archive index points to a row with a different primary key.");
                }

                return row;
            }

            int childIndex = TableArchiveReader.FindInteriorChildIndex(_page, pageHeader.EntryCount, key);
            if (childIndex < 0)
                return null;

            pageOffset = TableArchiveReader.ReadIndexEntryValue(_page, childIndex);
        }
    }

    public ValueTask DisposeAsync() => _stream.DisposeAsync();
}
