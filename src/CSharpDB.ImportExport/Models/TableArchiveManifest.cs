namespace CSharpDB.ImportExport.Models;

public sealed class TableArchiveManifest
{
    public const int CurrentFormatVersion = 3;
    public const int RowVersionFormatVersion = 4;

    public int FormatVersion { get; init; } = CurrentFormatVersion;
    public required string SourceTableName { get; init; }
    public DateTimeOffset CreatedUtc { get; init; } = DateTimeOffset.UtcNow;
    public long RowCount { get; init; }
    public string SchemaEntry { get; init; } = "native:schema";
    public string RowsEntry { get; init; } = "native:rows";
    public IReadOnlyList<TableArchiveIndexManifest> Indexes { get; init; } = Array.Empty<TableArchiveIndexManifest>();
}
