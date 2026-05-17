namespace CSharpDB.Admin.ImportExport.Contracts;

public sealed class TableExportRequest
{
    public required string TableName { get; init; }
    public TableExportDestination Destination { get; init; }
    public string? ServerPath { get; init; }
}
