namespace CSharpDB.Storage.Diagnostics;

public sealed class PageInspectReport
{
    public string SchemaVersion { get; init; } = "1.0";
    public required string DatabasePath { get; init; }
    public required uint PageId { get; init; }
    public required bool Exists { get; init; }
    public PageReport? Page { get; init; }
    public string? HexDump { get; init; }
    public required List<IntegrityIssue> Issues { get; init; }
}
