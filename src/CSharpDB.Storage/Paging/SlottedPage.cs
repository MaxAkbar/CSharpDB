using System.Buffers.Binary;

namespace CSharpDB.Storage.Paging;

/// <summary>
/// Provides structured access to a slotted page layout.
/// Layout: [PageHeader(9 bytes)] [CellPointers(2 bytes each)] [... free space ...] [cells growing backward from end]
/// </summary>
public struct SlottedPage
{
    private readonly byte[] _data;
    private readonly int _baseOffset; // offset where the slotted page header starts within the raw page

    public SlottedPage(byte[] pageData, uint pageId)
    {
        _data = pageData;
        _baseOffset = PageConstants.ContentOffset(pageId);
    }

    /// <summary>Helper to get a Span view over the underlying page data.</summary>
    private Span<byte> Span => _data.AsSpan();
    internal byte[] Buffer => _data;

    public byte PageType
    {
        get => _data[_baseOffset + PageConstants.PageTypeOffset];
        set => _data[_baseOffset + PageConstants.PageTypeOffset] = value;
    }

    public ushort CellCount
    {
        get => BinaryPrimitives.ReadUInt16LittleEndian(Span[(_baseOffset + PageConstants.CellCountOffset)..]);
        set => BinaryPrimitives.WriteUInt16LittleEndian(Span[(_baseOffset + PageConstants.CellCountOffset)..], value);
    }

    /// <summary>
    /// Start of the cell content area (cells grow backward from end of page toward this offset).
    /// </summary>
    public ushort CellContentStart
    {
        get => BinaryPrimitives.ReadUInt16LittleEndian(Span[(_baseOffset + PageConstants.FreeSpaceStartOffset)..]);
        set => BinaryPrimitives.WriteUInt16LittleEndian(Span[(_baseOffset + PageConstants.FreeSpaceStartOffset)..], value);
    }

    /// <summary>
    /// For interior pages: the rightmost child page ID.
    /// For leaf pages: the next leaf page ID (0 = no next).
    /// </summary>
    public uint RightChildOrNextLeaf
    {
        get => BinaryPrimitives.ReadUInt32LittleEndian(Span[(_baseOffset + PageConstants.RightChildOffset)..]);
        set => BinaryPrimitives.WriteUInt32LittleEndian(Span[(_baseOffset + PageConstants.RightChildOffset)..], value);
    }

    private int CellPointerArrayStart => _baseOffset + PageConstants.SlottedPageHeaderSize;

    public ushort GetCellOffset(int index) =>
        BinaryPrimitives.ReadUInt16LittleEndian(Span[(CellPointerArrayStart + index * PageConstants.CellPointerSize)..]);

    public void SetCellOffset(int index, ushort offset) =>
        BinaryPrimitives.WriteUInt16LittleEndian(Span[(CellPointerArrayStart + index * PageConstants.CellPointerSize)..], offset);

    public Span<byte> GetCell(int index)
    {
        ushort offset = GetCellOffset(index);
        // Read cell size from the cell header (first varint)
        var cellData = Span[offset..];
        ulong cellSize = Varint.Read(cellData, out int headerBytes);
        return Span[offset..(offset + headerBytes + (int)cellSize)];
    }

    public ReadOnlyMemory<byte> GetCellMemory(int index)
    {
        ushort offset = GetCellOffset(index);
        var cellData = _data.AsSpan(offset);
        ulong cellSize = Varint.Read(cellData, out int headerBytes);
        int totalSize = headerBytes + (int)cellSize;
        return _data.AsMemory(offset, totalSize);
    }

    /// <summary>
    /// Free space available for new cells (between end of pointer array and start of cell content).
    /// </summary>
    public int FreeSpace
    {
        get
        {
            int pointerEnd = CellPointerArrayStart + CellCount * PageConstants.CellPointerSize;
            return CellContentStart - pointerEnd;
        }
    }

    /// <summary>
    /// Insert a cell into the page. The cell data should NOT include a size prefix — we add it.
    /// Returns true if it fit, false if the page is full.
    /// </summary>
    public bool InsertCell(int index, ReadOnlySpan<byte> cellData)
    {
        int totalCellSize = cellData.Length;
        int needed = totalCellSize + PageConstants.CellPointerSize;

        if (FreeSpace < needed)
            return false;

        // Write cell content at end of content area
        ushort newOffset = (ushort)(CellContentStart - totalCellSize);
        cellData.CopyTo(Span[newOffset..]);
        CellContentStart = newOffset;

        // Shift cell pointers to make room at `index`
        int count = CellCount;
        for (int i = count; i > index; i--)
            SetCellOffset(i, GetCellOffset(i - 1));

        SetCellOffset(index, newOffset);
        CellCount = (ushort)(count + 1);
        return true;
    }

    /// <summary>
    /// Delete the cell at the given index (just removes the pointer; does not reclaim space until defragment).
    /// </summary>
    public void DeleteCell(int index)
    {
        int count = CellCount;
        for (int i = index; i < count - 1; i++)
            SetCellOffset(i, GetCellOffset(i + 1));
        CellCount = (ushort)(count - 1);
    }

    /// <summary>
    /// Initialize a fresh page (leaf or interior).
    /// </summary>
    public void Initialize(byte pageType)
    {
        Span[_baseOffset..].Clear();
        PageType = pageType;
        CellCount = 0;
        CellContentStart = (ushort)PageConstants.PageSize;
        RightChildOrNextLeaf = PageConstants.NullPageId;
    }

    /// <summary>
    /// Compact the page: rewrite all live cells contiguously at the end.
    /// </summary>
    public void Defragment()
    {
        int count = CellCount;
        if (count == 0)
        {
            CellContentStart = (ushort)PageConstants.PageSize;
            return;
        }

        // Collect all live cells
        var cells = new (int index, byte[] data)[count];
        for (int i = 0; i < count; i++)
        {
            ushort off = GetCellOffset(i);
            cells[i] = (i, GetCellRawBytes(off));
        }

        // Rewrite cells from end of page
        ushort writePos = (ushort)PageConstants.PageSize;
        for (int i = 0; i < count; i++)
        {
            writePos -= (ushort)cells[i].data.Length;
            cells[i].data.AsSpan().CopyTo(Span[writePos..]);
            SetCellOffset(i, writePos);
        }

        // Clear freed space
        int pointerEnd = CellPointerArrayStart + count * PageConstants.CellPointerSize;
        Span[pointerEnd..writePos].Clear();
        CellContentStart = writePos;
    }

    private byte[] GetCellRawBytes(ushort offset)
    {
        var cellData = Span[offset..];
        ulong payloadSize = Varint.Read(cellData, out int headerBytes);
        int totalSize = headerBytes + (int)payloadSize;
        return Span[offset..(offset + totalSize)].ToArray();
    }
}
