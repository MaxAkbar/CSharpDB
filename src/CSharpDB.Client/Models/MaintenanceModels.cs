namespace CSharpDB.Client.Models;

public enum ReindexScope
{
    All,
    Table,
    Index,
}

public sealed class ReindexRequest
{
    public ReindexScope Scope { get; init; } = ReindexScope.All;
    public string? Name { get; init; }
}

public sealed class DatabaseMaintenanceReport
{
    public string SchemaVersion { get; init; } = "1.0";
    public required string DatabasePath { get; init; }
    public required SpaceUsageReport SpaceUsage { get; init; }
    public required FragmentationReport Fragmentation { get; init; }
    public required Dictionary<string, int> PageTypeHistogram { get; init; }
}

public sealed class SpaceUsageReport
{
    public long DatabaseFileBytes { get; init; }
    public long WalFileBytes { get; init; }
    public int PageSizeBytes { get; init; }
    public int PhysicalPageCount { get; init; }
    public uint DeclaredPageCount { get; init; }
    public int FreelistPageCount { get; init; }
    public long FreelistBytes { get; init; }
}

public sealed class FragmentationReport
{
    public long BTreeFreeBytes { get; init; }
    public int PagesWithFreeSpace { get; init; }
    public int TailFreelistPageCount { get; init; }
    public long TailFreelistBytes { get; init; }
}

public sealed class ReindexResult
{
    public required ReindexScope Scope { get; init; }
    public string? Name { get; init; }
    public int RebuiltIndexCount { get; init; }
}

public sealed class VacuumResult
{
    public long DatabaseFileBytesBefore { get; init; }
    public long DatabaseFileBytesAfter { get; init; }
    public int PhysicalPageCountBefore { get; init; }
    public int PhysicalPageCountAfter { get; init; }
}
