namespace CSharpDB.Admin.ImportExport.Contracts;

public sealed class RestoreTableRequest
{
    public required string ArchivePath { get; init; }
    public string? TargetTableName { get; init; }
}
