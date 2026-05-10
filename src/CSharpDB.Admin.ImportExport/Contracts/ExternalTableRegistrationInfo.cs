namespace CSharpDB.Admin.ImportExport.Contracts;

public sealed class ExternalTableRegistrationInfo
{
    public required string TableName { get; init; }
    public required string Path { get; init; }
    public string? SourceTableName { get; init; }
    public long RowCount { get; init; }
    public DateTimeOffset? CreatedUtc { get; init; }
}
