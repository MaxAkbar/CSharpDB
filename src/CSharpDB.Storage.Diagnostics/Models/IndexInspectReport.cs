namespace CSharpDB.Storage.Diagnostics;

public sealed class IndexInspectReport
{
    public string SchemaVersion { get; init; } = "1.0";
    public required string DatabasePath { get; init; }
    public string? RequestedIndexName { get; init; }
    public int SampleSize { get; init; }

    public required List<IndexCheckItem> Indexes { get; init; }
    public required List<IntegrityIssue> Issues { get; init; }
}

public sealed class IndexCheckItem
{
    public required string IndexName { get; init; }
    public required string TableName { get; init; }
    public required List<string> Columns { get; init; }
    public required uint RootPage { get; init; }

    public bool RootPageValid { get; init; }
    public bool TableExists { get; init; }
    public bool ColumnsExistInTable { get; init; }
    public bool RootTreeReachable { get; init; }
}
