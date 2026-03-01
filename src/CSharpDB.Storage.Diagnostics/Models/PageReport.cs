namespace CSharpDB.Storage.Diagnostics;

public sealed class PageReport
{
    public required uint PageId { get; init; }
    public required byte PageTypeCode { get; init; }
    public required string PageTypeName { get; init; }
    public required int BaseOffset { get; init; }

    public required int CellCount { get; init; }
    public required int CellContentStart { get; init; }
    public required uint RightChildOrNextLeaf { get; init; }
    public required int FreeSpaceBytes { get; init; }

    public required List<int> CellOffsets { get; init; }
    public List<LeafCellReport>? LeafCells { get; init; }
    public List<InteriorCellReport>? InteriorCells { get; init; }
}
