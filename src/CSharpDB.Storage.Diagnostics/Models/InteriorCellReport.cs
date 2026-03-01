namespace CSharpDB.Storage.Diagnostics;

public sealed class InteriorCellReport
{
    public required int CellIndex { get; init; }
    public required int CellOffset { get; init; }
    public required int HeaderBytes { get; init; }
    public required int CellTotalBytes { get; init; }

    public uint? LeftChildPage { get; init; }
    public long? Key { get; init; }
}
