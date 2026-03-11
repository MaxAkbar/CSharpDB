namespace CSharpDB.Engine;

public enum DatabaseReindexScope
{
    All,
    Table,
    Index,
}

public sealed class DatabaseReindexRequest
{
    public DatabaseReindexScope Scope { get; init; } = DatabaseReindexScope.All;
    public string? Name { get; init; }
}

public sealed class DatabaseMaintenanceReport
{
    public string SchemaVersion { get; init; } = "1.0";
    public required string DatabasePath { get; init; }
    public required DatabaseSpaceUsageReport SpaceUsage { get; init; }
    public required DatabaseFragmentationReport Fragmentation { get; init; }
    public required Dictionary<string, int> PageTypeHistogram { get; init; }
}

public sealed class DatabaseSpaceUsageReport
{
    public long DatabaseFileBytes { get; init; }
    public long WalFileBytes { get; init; }
    public int PageSizeBytes { get; init; }
    public int PhysicalPageCount { get; init; }
    public uint DeclaredPageCount { get; init; }
    public int FreelistPageCount { get; init; }
    public long FreelistBytes { get; init; }
}

public sealed class DatabaseFragmentationReport
{
    public long BTreeFreeBytes { get; init; }
    public int PagesWithFreeSpace { get; init; }
    public int TailFreelistPageCount { get; init; }
    public long TailFreelistBytes { get; init; }
}

public sealed class DatabaseReindexResult
{
    public required DatabaseReindexScope Scope { get; init; }
    public string? Name { get; init; }
    public int RebuiltIndexCount { get; init; }
}

public sealed class DatabaseVacuumResult
{
    public long DatabaseFileBytesBefore { get; init; }
    public long DatabaseFileBytesAfter { get; init; }
    public int PhysicalPageCountBefore { get; init; }
    public int PhysicalPageCountAfter { get; init; }
}
