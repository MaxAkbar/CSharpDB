namespace CSharpDB.Admin.Models;

public sealed class DataModelState
{
    public int Version { get; set; } = 1;
    public string? DiagramName { get; set; }
    public List<DataModelNode> Nodes { get; set; } = [];
    public List<DataModelRelationship> Relationships { get; set; } = [];
    public List<DataModelPendingOperation> PendingOperations { get; set; } = [];
    public List<string> Warnings { get; set; } = [];
    public string? SavedLayoutName { get; set; }
    public string? SchemaSnapshotUtc { get; set; }
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
    public bool IsDraft { get; set; }
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
    public bool IsRowVersion { get; set; }
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

public sealed class DataModelPendingOperation
{
    public string Id { get; set; } = Guid.NewGuid().ToString("N");
    public DataModelPendingOperationKind Kind { get; set; }
    public string TableName { get; set; } = "";
    public string? NewTableName { get; set; }
    public string? ColumnName { get; set; }
    public string? NewColumnName { get; set; }
    public string ColumnType { get; set; } = "TEXT";
    public bool NotNull { get; set; }
    public List<DataModelColumn> Columns { get; set; } = [];
    public string? ReferencedTableName { get; set; }
    public string? ReferencedColumnName { get; set; }
    public string? ConstraintName { get; set; }
    public string OnDelete { get; set; } = "RESTRICT";
    public string Description { get; set; } = "";
}

public enum DataModelPendingOperationKind
{
    CreateTable,
    DropTable,
    RenameTable,
    AddColumn,
    DropColumn,
    RenameColumn,
    AddForeignKey,
    DropForeignKey,
}

public sealed class DataModelDiagramSummary
{
    public long Id { get; init; }
    public required string Name { get; init; }
    public required string CreatedUtc { get; init; }
    public required string UpdatedUtc { get; init; }
    public int SourceCount { get; init; }
    public int PendingOperationCount { get; init; }
}

public sealed class DataModelApplyResult
{
    public bool Succeeded { get; init; }
    public IReadOnlyList<string> Messages { get; init; } = [];
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
    public bool IsRowVersion { get; init; }
    public bool Nullable { get; init; } = true;
    public string? Collation { get; init; }
}

public sealed class DataModelForeignKeyMetadata
{
    public required string ConstraintName { get; init; }
    public required string ColumnName { get; init; }
    public IReadOnlyList<string> ColumnNames { get; init; } = [];
    public required string ReferencedTableName { get; init; }
    public required string ReferencedColumnName { get; init; }
    public IReadOnlyList<string> ReferencedColumnNames { get; init; } = [];
    public string? OnDelete { get; init; }
}

public sealed class DataModelIndexMetadata
{
    public required string IndexName { get; init; }
    public IReadOnlyList<string> Columns { get; init; } = [];
    public bool IsUnique { get; init; }
}
