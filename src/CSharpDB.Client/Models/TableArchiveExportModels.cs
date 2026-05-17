namespace CSharpDB.Client.Models;

public sealed class TableArchiveExportResult
{
    public required string TableName { get; init; }
    public required string Path { get; init; }
    public required string FileName { get; init; }
    public long RowCount { get; init; }
}

public sealed class TableArchiveExportProgress
{
    public required string TableName { get; init; }
    public required string Stage { get; init; }
    public string? Message { get; init; }
    public long RowsExported { get; init; }
    public long? TotalRows { get; init; }
    public string? Path { get; init; }
}
