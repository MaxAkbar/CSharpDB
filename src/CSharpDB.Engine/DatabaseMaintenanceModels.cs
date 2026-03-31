using CSharpDB.Primitives;

namespace CSharpDB.Engine;

public enum DatabaseReindexScope
{
    All,
    Table,
    Index,
}

public sealed class DatabaseReindexRequest
{
    public DatabaseReindexScope Scope { get; init; } = DatabaseReindexScope.All;
    public string? Name { get; init; }
    public bool AllowCorruptIndexRecovery { get; init; }
}

public sealed class DatabaseForeignKeyMigrationRequest
{
    public bool ValidateOnly { get; init; }
    public string? BackupDestinationPath { get; init; }
    public int ViolationSampleLimit { get; init; } = 100;
    public IReadOnlyList<DatabaseForeignKeyMigrationConstraintSpec> Constraints { get; init; } = Array.Empty<DatabaseForeignKeyMigrationConstraintSpec>();
}

public sealed class DatabaseForeignKeyMigrationConstraintSpec
{
    public required string TableName { get; init; }
    public required string ColumnName { get; init; }
    public required string ReferencedTableName { get; init; }
    public string? ReferencedColumnName { get; init; }
    public ForeignKeyOnDeleteAction OnDelete { get; init; } = ForeignKeyOnDeleteAction.Restrict;
}

public sealed class DatabaseForeignKeyMigrationResult
{
    public bool ValidateOnly { get; init; }
    public bool Succeeded { get; init; }
    public string? BackupDestinationPath { get; init; }
    public int AffectedTables { get; init; }
    public int AppliedForeignKeys { get; init; }
    public long CopiedRows { get; init; }
    public int ViolationCount { get; init; }
    public IReadOnlyList<DatabaseForeignKeyMigrationViolation> Violations { get; init; } = Array.Empty<DatabaseForeignKeyMigrationViolation>();
    public IReadOnlyList<DatabaseForeignKeyMigrationAppliedConstraint> AppliedConstraints { get; init; } = Array.Empty<DatabaseForeignKeyMigrationAppliedConstraint>();
}

public sealed class DatabaseForeignKeyMigrationViolation
{
    public required string TableName { get; init; }
    public required string ColumnName { get; init; }
    public required string ReferencedTableName { get; init; }
    public required string ReferencedColumnName { get; init; }
    public required string ChildKeyColumnName { get; init; }
    public DbValue ChildKeyValue { get; init; } = DbValue.Null;
    public DbValue ChildValue { get; init; } = DbValue.Null;
    public required string Reason { get; init; }
}

public sealed class DatabaseForeignKeyMigrationAppliedConstraint
{
    public required string TableName { get; init; }
    public required string ColumnName { get; init; }
    public required string ReferencedTableName { get; init; }
    public required string ReferencedColumnName { get; init; }
    public required string ConstraintName { get; init; }
    public required string SupportingIndexName { get; init; }
    public ForeignKeyOnDeleteAction OnDelete { get; init; } = ForeignKeyOnDeleteAction.Restrict;
}

public sealed class DatabaseMaintenanceReport
{
    public string SchemaVersion { get; init; } = "1.0";
    public required string DatabasePath { get; init; }
    public required DatabaseSpaceUsageReport SpaceUsage { get; init; }
    public required DatabaseFragmentationReport Fragmentation { get; init; }
    public required Dictionary<string, int> PageTypeHistogram { get; init; }
}

public sealed class DatabaseSpaceUsageReport
{
    public long DatabaseFileBytes { get; init; }
    public long WalFileBytes { get; init; }
    public int PageSizeBytes { get; init; }
    public int PhysicalPageCount { get; init; }
    public uint DeclaredPageCount { get; init; }
    public int FreelistPageCount { get; init; }
    public long FreelistBytes { get; init; }
}

public sealed class DatabaseFragmentationReport
{
    public long BTreeFreeBytes { get; init; }
    public int PagesWithFreeSpace { get; init; }
    public int TailFreelistPageCount { get; init; }
    public long TailFreelistBytes { get; init; }
}

public sealed class DatabaseReindexResult
{
    public required DatabaseReindexScope Scope { get; init; }
    public string? Name { get; init; }
    public int RebuiltIndexCount { get; init; }
    public int RecoveredCorruptIndexCount { get; init; }
}

public sealed class DatabaseVacuumResult
{
    public long DatabaseFileBytesBefore { get; init; }
    public long DatabaseFileBytesAfter { get; init; }
    public int PhysicalPageCountBefore { get; init; }
    public int PhysicalPageCountAfter { get; init; }
}
