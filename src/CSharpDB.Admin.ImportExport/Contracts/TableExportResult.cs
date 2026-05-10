namespace CSharpDB.Admin.ImportExport.Contracts;

public sealed class TableExportResult
{
    public required string TableName { get; init; }
    public required string FileName { get; init; }
    public required string Path { get; init; }
    public long RowCount { get; init; }
    public string? DownloadUrl { get; init; }
    public bool IsDownload { get; init; }
}
