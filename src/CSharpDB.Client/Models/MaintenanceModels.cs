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
    public bool AllowCorruptIndexRecovery { get; init; }
}

public sealed class BackupRequest
{
    public required string DestinationPath { get; init; }
    public bool WithManifest { get; init; }
}

public sealed class BackupResult
{
    public required string SourcePath { get; init; }
    public required string DestinationPath { get; init; }
    public string? ManifestPath { get; init; }
    public long DatabaseFileBytes { get; init; }
    public int PhysicalPageCount { get; init; }
    public uint DeclaredPageCount { get; init; }
    public uint ChangeCounter { get; init; }
    public int WarningCount { get; init; }
    public int ErrorCount { get; init; }
    public required string Sha256 { get; init; }
}

public sealed class RestoreRequest
{
    public required string SourcePath { get; init; }
    public bool ValidateOnly { get; init; }
}

public sealed class RestoreResult
{
    public required string SourcePath { get; init; }
    public string? DestinationPath { get; init; }
    public bool ValidateOnly { get; init; }
    public long DatabaseFileBytes { get; init; }
    public int PhysicalPageCount { get; init; }
    public uint DeclaredPageCount { get; init; }
    public uint ChangeCounter { get; init; }
    public bool SourceWalExists { get; init; }
    public int WarningCount { get; init; }
    public int ErrorCount { get; init; }
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
    public int RecoveredCorruptIndexCount { get; init; }
}

public sealed class VacuumResult
{
    public long DatabaseFileBytesBefore { get; init; }
    public long DatabaseFileBytesAfter { get; init; }
    public int PhysicalPageCountBefore { get; init; }
    public int PhysicalPageCountAfter { get; init; }
}
