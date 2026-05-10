namespace CSharpDB.Admin.ImportExport.Contracts;

public sealed class ExternalTableRegistrationRequest
{
    public required string TableName { get; init; }
    public required string ArchivePath { get; init; }
    public bool ReplaceExisting { get; init; } = true;
}
