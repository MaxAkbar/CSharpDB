namespace CSharpDB.ImportExport.Models;

public sealed class TableArchiveIndexManifest
{
    public required string Name { get; init; }
    public required string Kind { get; init; }
    public required string ColumnName { get; init; }
    public int ColumnIndex { get; init; }
    public long EntryCount { get; init; }
    public string SectionEntry { get; init; } = "native:index:primary-key";
}
