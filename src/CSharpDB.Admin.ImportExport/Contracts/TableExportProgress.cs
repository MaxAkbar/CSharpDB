namespace CSharpDB.Admin.ImportExport.Contracts;

public sealed class TableExportProgress
{
    public required string Operation { get; init; }
    public required string Stage { get; init; }
    public string? Message { get; init; }
    public string? TableName { get; init; }
    public string? Path { get; init; }
    public long RowsProcessed { get; init; }
    public long? TotalRows { get; init; }

    public int? PercentComplete =>
        TotalRows is > 0
            ? (int)Math.Clamp(Math.Round((double)RowsProcessed / TotalRows.Value * 100), 0, 100)
            : null;
}
