namespace CSharpDB.Admin.Models;

public sealed class DataModelState
{
    public List<DataModelNode> Nodes { get; set; } = [];
    public List<DataModelRelationship> Relationships { get; set; } = [];
    public List<string> Warnings { get; set; } = [];
    public string? SavedLayoutName { get; set; }
    public double ViewportX { get; set; }
    public double ViewportY { get; set; }
    public double Scale { get; set; } = 1;
}

public sealed class DataModelNode
{
    public string Name { get; set; } = "";
    public DataModelNodeKind Kind { get; set; } = DataModelNodeKind.Table;
    public double X { get; set; } = 20;
    public double Y { get; set; } = 20;
    public bool IsCollapsed { get; set; }
    public List<DataModelColumn> Columns { get; set; } = [];
    public string? SourceTableName { get; set; }
    public string? ArchivePath { get; set; }
    public string? ArchiveCreatedUtc { get; set; }
    public long? RowCount { get; set; }
    public int IndexCount { get; set; }
    public int TriggerCount { get; set; }
    public List<string> Warnings { get; set; } = [];
}

public enum DataModelNodeKind
{
    Table,
    ExternalTable,
}

public sealed class DataModelColumn
{
    public string Name { get; set; } = "";
    public string TypeLabel { get; set; } = "";
    public bool IsPrimaryKey { get; set; }
    public bool IsIdentity { get; set; }
    public bool Nullable { get; set; } = true;
    public string? Collation { get; set; }
    public bool IsForeignKey { get; set; }
    public bool IsIndexed { get; set; }
}

public sealed class DataModelRelationship
{
    public string Id { get; set; } = "";
    public string LeftTable { get; set; } = "";
    public string LeftColumn { get; set; } = "";
    public string RightTable { get; set; } = "";
    public string RightColumn { get; set; } = "";
    public DataModelRelationshipKind Kind { get; set; } = DataModelRelationshipKind.PhysicalForeignKey;
    public bool IsResolved { get; set; } = true;
    public string? ConstraintName { get; set; }
    public string? OnDelete { get; set; }
    public string? Warning { get; set; }
}

public enum DataModelRelationshipKind
{
    PhysicalForeignKey,
    ExternalArchiveForeignKey,
    Draft,
}

public sealed class DataModelSourceOption
{
    public required string Name { get; init; }
    public required DataModelNodeKind Kind { get; init; }
    public string? SourceTableName { get; init; }
}

public sealed record DataModelNodeMove(string NodeName, double X, double Y);

public sealed class DataModelSourceMetadata
{
    public required string TableName { get; init; }
    public DataModelNodeKind Kind { get; init; } = DataModelNodeKind.Table;
    public IReadOnlyList<DataModelColumnMetadata> Columns { get; init; } = [];
    public IReadOnlyList<DataModelForeignKeyMetadata> ForeignKeys { get; init; } = [];
    public IReadOnlyList<DataModelIndexMetadata> Indexes { get; init; } = [];
    public int TriggerCount { get; init; }
    public string? SourceTableName { get; init; }
    public string? ArchivePath { get; init; }
    public string? ArchiveCreatedUtc { get; init; }
    public long? RowCount { get; init; }
    public IReadOnlyList<string> Warnings { get; init; } = [];
}

public sealed class DataModelColumnMetadata
{
    public required string Name { get; init; }
    public required string TypeLabel { get; init; }
    public bool IsPrimaryKey { get; init; }
    public bool IsIdentity { get; init; }
    public bool Nullable { get; init; } = true;
    public string? Collation { get; init; }
}

public sealed class DataModelForeignKeyMetadata
{
    public required string ConstraintName { get; init; }
    public required string ColumnName { get; init; }
    public required string ReferencedTableName { get; init; }
    public required string ReferencedColumnName { get; init; }
    public string? OnDelete { get; init; }
}

public sealed class DataModelIndexMetadata
{
    public required string IndexName { get; init; }
    public IReadOnlyList<string> Columns { get; init; } = [];
    public bool IsUnique { get; init; }
}
