namespace CSharpDB.Admin.ImportExport.Contracts;

public sealed class RestoreTableResult
{
    public required string TableName { get; init; }
    public long RowsInserted { get; init; }
}
