namespace CSharpDB.ImportExport.Models;

public sealed class TableArchiveManifest
{
    public const int CurrentFormatVersion = 3;
    public const int RowVersionFormatVersion = 4;
    public const int SchemaFidelityFormatVersion = 5;
    public const int ReferentialActionsFormatVersion = 6;
    public const int IntegrityFormatVersion = SchemaFidelityFormatVersion;
    public const int LatestFormatVersion = ReferentialActionsFormatVersion;

    public int FormatVersion { get; init; } = LatestFormatVersion;
    public required string SourceTableName { get; init; }
    public DateTimeOffset CreatedUtc { get; init; } = DateTimeOffset.UtcNow;
    public long RowCount { get; init; }
    public string SchemaEntry { get; init; } = "native:schema";
    public string RowsEntry { get; init; } = "native:rows";
    /// <summary>
    /// Physical lookup sections stored inside the archive. These are not source-table
    /// secondary indexes; logical SQL indexes live in TableArchiveSchema.SecondaryIndexes.
    /// </summary>
    public IReadOnlyList<TableArchiveIndexManifest> Indexes { get; init; } = Array.Empty<TableArchiveIndexManifest>();

    /// <summary>
    /// Required for format v5 and later. Legacy v3/v4 archives do not require
    /// digest metadata.
    /// </summary>
    public TableArchiveSectionDigests? Digests { get; init; }
}
