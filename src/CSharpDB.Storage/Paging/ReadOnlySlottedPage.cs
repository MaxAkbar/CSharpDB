using System.Buffers.Binary;

namespace CSharpDB.Storage.Paging;

/// <summary>
/// Read-only view over a slotted page layout.
/// </summary>
internal readonly struct ReadOnlySlottedPage
{
    private readonly ReadOnlyMemory<byte> _data;
    private readonly int _baseOffset;

    public ReadOnlySlottedPage(ReadOnlyMemory<byte> pageData, uint pageId)
    {
        _data = pageData;
        _baseOffset = PageConstants.ContentOffset(pageId);
    }

    private ReadOnlySpan<byte> Span => _data.Span;
    internal ReadOnlyMemory<byte> Buffer => _data;

    public byte PageType => _data.Span[_baseOffset + PageConstants.PageTypeOffset];

    public ushort CellCount =>
        BinaryPrimitives.ReadUInt16LittleEndian(Span[(_baseOffset + PageConstants.CellCountOffset)..]);

    public ushort CellContentStart =>
        BinaryPrimitives.ReadUInt16LittleEndian(Span[(_baseOffset + PageConstants.FreeSpaceStartOffset)..]);

    public uint RightChildOrNextLeaf =>
        BinaryPrimitives.ReadUInt32LittleEndian(Span[(_baseOffset + PageConstants.RightChildOffset)..]);

    private int CellPointerArrayStart => _baseOffset + PageConstants.SlottedPageHeaderSize;

    public ushort GetCellOffset(int index) =>
        BinaryPrimitives.ReadUInt16LittleEndian(Span[(CellPointerArrayStart + index * PageConstants.CellPointerSize)..]);

    public ReadOnlyMemory<byte> GetCellMemory(int index)
    {
        ushort offset = GetCellOffset(index);
        var cellData = _data.Slice(offset).Span;
        ulong cellSize = Varint.Read(cellData, out int headerBytes);
        int totalSize = headerBytes + (int)cellSize;
        return _data.Slice(offset, totalSize);
    }
}
