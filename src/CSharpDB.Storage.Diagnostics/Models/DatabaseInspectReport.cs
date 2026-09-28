namespace CSharpDB.Storage.Diagnostics;

public sealed class DatabaseInspectReport
{
    public string SchemaVersion { get; init; } = "1.0";
    public required string DatabasePath { get; init; }
    public required FileHeaderReport Header { get; init; }
    public required Dictionary<string, int> PageTypeHistogram { get; init; }
    public bool IsSummary { get; init; }
    public long WalFileLengthBytes { get; init; }
    public long BTreeFreeBytes { get; init; }
    public int PagesWithFreeSpace { get; init; }
    public int TailFreelistPageCount { get; init; }
    public IndexInspectReport? IndexChecks { get; init; }
    public int PageCountScanned { get; init; }
    public List<PageReport>? Pages { get; init; }
    public required List<IntegrityIssue> Issues { get; init; }
}
