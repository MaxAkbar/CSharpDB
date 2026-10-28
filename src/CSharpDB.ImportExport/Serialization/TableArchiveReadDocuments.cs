using System.Text.Json.Serialization;
using CSharpDB.ImportExport.Models;
using CSharpDB.Primitives;

namespace CSharpDB.ImportExport.Serialization;

// Generated constructors for required/init-only models can overwrite property
// initializers when JSON omits additive fields. Mutable read documents preserve
// those legacy defaults, while explicit null still reaches normal validation.
internal sealed class TableArchiveSchemaDocument
{
    public Guid SchemaId { get; set; }
    [JsonRequired] public string TableName { get; set; } = null!;
    [JsonRequired] public IReadOnlyList<TableArchiveColumn> Columns { get; set; } = null!;
    public IReadOnlyList<TableArchiveForeignKeyDocument> ForeignKeys { get; set; } = [];
    public IReadOnlyList<TableArchiveCheckConstraint> CheckConstraints { get; set; } = [];
    public IReadOnlyList<TableArchiveKeyConstraint> KeyConstraints { get; set; } = [];
    public IReadOnlyList<TableArchiveSecondaryIndexDocument>? SecondaryIndexes { get; set; }
    public long NextRowId { get; set; }

    public TableArchiveSchema ToModel() => new()
    {
        SchemaId = SchemaId, TableName = TableName, Columns = Columns,
        ForeignKeys = TableArchiveReadDocuments.Map(ForeignKeys, static item => item.ToModel()),
        CheckConstraints = CheckConstraints, KeyConstraints = KeyConstraints,
        SecondaryIndexes = TableArchiveReadDocuments.Map(SecondaryIndexes, static item => item.ToModel()),
        NextRowId = NextRowId,
    };
}

internal sealed class TableArchiveForeignKeyDocument
{
    public Guid SchemaId { get; set; }
    public IReadOnlyList<Guid> ColumnSchemaIds { get; set; } = [];
    public Guid ReferencedTableSchemaId { get; set; }
    public IReadOnlyList<Guid> ReferencedColumnSchemaIds { get; set; } = [];
    public Guid ReferencedKeySchemaId { get; set; }
    [JsonRequired] public string ConstraintName { get; set; } = null!;
    [JsonRequired] public string ColumnName { get; set; } = null!;
    [JsonRequired] public string ReferencedTableName { get; set; } = null!;
    [JsonRequired] public string ReferencedColumnName { get; set; } = null!;
    public IReadOnlyList<string> ColumnNames { get; set; } = [];
    public IReadOnlyList<string> ReferencedColumnNames { get; set; } = [];
    public ForeignKeyOnDeleteAction OnDelete { get; set; }
    public ForeignKeyOnDeleteAction OnUpdate { get; set; }
    [JsonRequired] public string SupportingIndexName { get; set; } = null!;

    public TableArchiveForeignKey ToModel() => new()
    {
        SchemaId = SchemaId, ColumnSchemaIds = ColumnSchemaIds,
        ReferencedTableSchemaId = ReferencedTableSchemaId, ReferencedColumnSchemaIds = ReferencedColumnSchemaIds,
        ReferencedKeySchemaId = ReferencedKeySchemaId, ConstraintName = ConstraintName,
        ColumnName = ColumnName, ReferencedTableName = ReferencedTableName,
        ReferencedColumnName = ReferencedColumnName, ColumnNames = ColumnNames,
        ReferencedColumnNames = ReferencedColumnNames, OnDelete = OnDelete, OnUpdate = OnUpdate,
        SupportingIndexName = SupportingIndexName,
    };
}

internal sealed class TableArchiveSecondaryIndexDocument
{
    [JsonRequired] public string Name { get; set; } = null!;
    [JsonRequired] public IReadOnlyList<string> Columns { get; set; } = null!;
    public IReadOnlyList<string?> ColumnCollations { get; set; } = [];
    public bool IsUnique { get; set; }

    public TableArchiveSecondaryIndex ToModel() => new()
    {
        Name = Name, Columns = Columns, ColumnCollations = ColumnCollations, IsUnique = IsUnique,
    };
}

internal sealed class TableArchiveManifestDocument
{
    public int FormatVersion { get; set; } = TableArchiveManifest.LatestFormatVersion;
    [JsonRequired] public string SourceTableName { get; set; } = null!;
    public DateTimeOffset CreatedUtc { get; set; } = DateTimeOffset.UtcNow;
    public long RowCount { get; set; }
    public string SchemaEntry { get; set; } = "native:schema";
    public string RowsEntry { get; set; } = "native:rows";
    public IReadOnlyList<TableArchiveIndexDocument> Indexes { get; set; } = [];
    public TableArchiveDigestsDocument? Digests { get; set; }

    public TableArchiveManifest ToModel() => new()
    {
        FormatVersion = FormatVersion, SourceTableName = SourceTableName, CreatedUtc = CreatedUtc,
        RowCount = RowCount, SchemaEntry = SchemaEntry, RowsEntry = RowsEntry,
        Indexes = TableArchiveReadDocuments.Map(Indexes, static item => item.ToModel()),
        Digests = Digests?.ToModel(),
    };
}

internal sealed class TableArchiveIndexDocument
{
    [JsonRequired] public string Name { get; set; } = null!;
    [JsonRequired] public string Kind { get; set; } = null!;
    [JsonRequired] public string ColumnName { get; set; } = null!;
    public int ColumnIndex { get; set; }
    public long EntryCount { get; set; }
    public string SectionEntry { get; set; } = "native:index:primary-key";

    public TableArchiveIndexManifest ToModel() => new()
    {
        Name = Name, Kind = Kind, ColumnName = ColumnName, ColumnIndex = ColumnIndex,
        EntryCount = EntryCount, SectionEntry = SectionEntry,
    };
}

internal sealed class TableArchiveDigestsDocument
{
    public string Algorithm { get; set; } = TableArchiveSectionDigests.Sha256Algorithm;
    public string Encoding { get; set; } = TableArchiveSectionDigests.LowercaseHexEncoding;
    [JsonRequired] public string Schema { get; set; } = null!;
    [JsonRequired] public string Rows { get; set; } = null!;
    [JsonRequired] public string PhysicalIndex { get; set; } = null!;

    public TableArchiveSectionDigests ToModel() => new()
    {
        Algorithm = Algorithm, Encoding = Encoding, Schema = Schema, Rows = Rows, PhysicalIndex = PhysicalIndex,
    };
}

internal static class TableArchiveReadDocuments
{
    internal static IReadOnlyList<TModel> Map<TDocument, TModel>(
        IReadOnlyList<TDocument>? items, Func<TDocument, TModel> convert)
        where TDocument : class where TModel : class
        => items?.Select(item => item is null ? null! : convert(item)).ToArray()!;
}
