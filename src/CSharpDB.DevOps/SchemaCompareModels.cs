using CSharpDB.Client.Models;

namespace CSharpDB.DevOps;

public enum DevOpsTargetKind
{
    Database,
    TableArchive,
}

public enum SchemaObjectKind
{
    Table,
    Column,
    ForeignKey,
    Index,
    View,
    Trigger,
    Procedure,
}

public enum SchemaChangeKind
{
    Added,
    Removed,
    Changed,
}

public enum SchemaScriptScope
{
    UserObjects,
    WholeDatabase,
}

public sealed class SchemaScriptOptions
{
    public SchemaScriptScope Scope { get; init; } = SchemaScriptScope.UserObjects;
    public SchemaObjectKind? ObjectKind { get; init; }
    public string? ObjectName { get; init; }
    public bool IncludeIndexes { get; init; } = true;
    public bool IncludeTriggers { get; init; } = true;
    public bool IncludeRelatedViews { get; init; }
    public bool IncludeRelatedProcedures { get; init; }
}

public sealed class SchemaTargetDescriptor
{
    public required DevOpsTargetKind Kind { get; init; }
    public required string DisplayName { get; init; }
    public string? Location { get; init; }
}

public sealed class SchemaSnapshot
{
    public required SchemaTargetDescriptor Target { get; init; }
    public required IReadOnlyList<TableSchema> Tables { get; init; }
    public IReadOnlyList<IndexSchema> Indexes { get; init; } = [];
    public IReadOnlyList<ViewDefinition> Views { get; init; } = [];
    public IReadOnlyList<TriggerSchema> Triggers { get; init; } = [];
    public IReadOnlyList<ProcedureDefinition> Procedures { get; init; } = [];
}

public sealed class SchemaDiffReport
{
    public required SchemaTargetDescriptor Source { get; init; }
    public required SchemaTargetDescriptor Target { get; init; }
    public required SchemaDiffSummary Summary { get; init; }
    public required IReadOnlyList<SchemaDiffChange> Changes { get; init; }
    public IReadOnlyList<string> Warnings { get; init; } = [];
    public DateTime GeneratedUtc { get; init; } = DateTime.UtcNow;
}

public sealed class SchemaDiffSummary
{
    public int TotalChanges { get; init; }
    public int DestructiveChanges { get; init; }
    public int TableChanges { get; init; }
    public int ColumnChanges { get; init; }
    public int ForeignKeyChanges { get; init; }
    public int IndexChanges { get; init; }
    public int ViewChanges { get; init; }
    public int TriggerChanges { get; init; }
    public int ProcedureChanges { get; init; }
}

public sealed class SchemaDiffChange
{
    public required SchemaObjectKind ObjectKind { get; init; }
    public required SchemaChangeKind ChangeKind { get; init; }
    public required string Name { get; init; }
    public string? ParentName { get; init; }
    public string? SourceDefinition { get; init; }
    public string? TargetDefinition { get; init; }
    public bool IsDestructive { get; init; }
    public string? Warning { get; init; }
    public IReadOnlyDictionary<string, string> Details { get; init; } = new Dictionary<string, string>();
}

public interface ISchemaCompareTarget
{
    SchemaTargetDescriptor Descriptor { get; }
    Task<SchemaSnapshot> LoadSchemaAsync(CancellationToken ct = default);
}
