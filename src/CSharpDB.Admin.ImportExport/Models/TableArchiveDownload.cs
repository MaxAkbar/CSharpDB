namespace CSharpDB.Admin.ImportExport.Models;

public sealed class TableArchiveDownload
{
    public required string Token { get; init; }
    public required string Path { get; init; }
    public required string FileName { get; init; }
    public DateTimeOffset CreatedUtc { get; init; } = DateTimeOffset.UtcNow;
}
