namespace CSharpDB.Storage.Diagnostics;

public sealed class LeafCellReport
{
    public required int CellIndex { get; init; }
    public required int CellOffset { get; init; }
    public required int HeaderBytes { get; init; }
    public required int CellTotalBytes { get; init; }

    public long? Key { get; init; }
    public int PayloadBytes { get; init; }
}
