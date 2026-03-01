namespace CSharpDB.Storage;

public static class PageConstants
{
    public const int PageSize = 4096;
    public const int FileHeaderSize = 100;

    // Magic bytes: "CSDB"
    public static readonly byte[] MagicBytes = "CSDB"u8.ToArray();
    public const int FormatVersion = 1;

    // File header layout offsets (within page 0)
    public const int MagicOffset = 0;           // 4 bytes
    public const int VersionOffset = 4;          // 4 bytes (int32)
    public const int PageSizeOffset = 8;         // 4 bytes (int32)
    public const int PageCountOffset = 12;       // 4 bytes (uint32)
    public const int SchemaRootPageOffset = 16;  // 4 bytes (uint32) — root page of schema catalog B+tree
    public const int FreelistHeadOffset = 20;    // 4 bytes (uint32) — first page of freelist, 0 = none
    public const int ChangeCounterOffset = 24;   // 4 bytes (uint32)

    // Slotted page header layout (within each B+tree page)
    public const int PageTypeOffset = 0;         // 1 byte
    public const int CellCountOffset = 1;        // 2 bytes (ushort)
    public const int FreeSpaceStartOffset = 3;   // 2 bytes (ushort) — start of cell content area
    public const int RightChildOffset = 5;       // 4 bytes (uint32) — rightmost child for interior pages
    public const int NextLeafOffset = 5;         // 4 bytes (uint32) — next leaf for leaf pages (reuses offset since interior uses RightChild)
    public const int SlottedPageHeaderSize = 9;  // total header before cell pointer array
    public const int CellPointerSize = 2;        // each cell pointer is 2 bytes (ushort offset within page)

    // Page types
    public const byte PageTypeInterior = 0x05;
    public const byte PageTypeLeaf = 0x0D;
    public const byte PageTypeFreelist = 0x00;

    // Special page IDs
    public const uint NullPageId = 0;

    // ============ WAL file format constants ============

    // WAL file header (32 bytes)
    public static readonly byte[] WalMagic = "CWAL"u8.ToArray();
    public const int WalHeaderSize = 32;
    // Layout: [magic:4][version:4][pageSize:4][dbPageCount:4][salt1:4][salt2:4][cksumSeed:4][reserved:4]

    // WAL frame header (24 bytes, precedes each page image)
    public const int WalFrameHeaderSize = 24;
    // Layout: [pageId:4][dbPageCount:4][salt1:4][salt2:4][headerCksum:4][dataCksum:4]

    // Total frame size = WalFrameHeaderSize + PageSize
    public const int WalFrameSize = WalFrameHeaderSize + PageSize;

    // Default auto-checkpoint threshold (number of committed frames)
    public const int DefaultCheckpointThreshold = 1000;

    // Usable space within a page (after file header on page 0, or full page otherwise)
    public static int UsableSpace(uint pageId) =>
        pageId == 0 ? PageSize - FileHeaderSize : PageSize;

    // Content area offset for a given page (page 0 has the file header)
    public static int ContentOffset(uint pageId) =>
        pageId == 0 ? FileHeaderSize : 0;
}
