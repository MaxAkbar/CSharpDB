namespace CSharpDB.DevOps;

public sealed class DriftReportOptions
{
    public IReadOnlyList<DataCompareOptions> DataTables { get; init; } = [];
}

public sealed class DriftReport
{
    public required SchemaTargetDescriptor Baseline { get; init; }
    public required SchemaTargetDescriptor Current { get; init; }
    public required SchemaDiffReport Schema { get; init; }
    public IReadOnlyList<DataDiffReport> Data { get; init; } = [];
    public required DriftSummary Summary { get; init; }
    public DateTime GeneratedUtc { get; init; } = DateTime.UtcNow;
}

public sealed class DriftSummary
{
    public bool HasDrift { get; init; }
    public int SchemaChanges { get; init; }
    public int DestructiveSchemaChanges { get; init; }
    public int DataTablesCompared { get; init; }
    public int DataTablesWithDifferences { get; init; }
    public int DataRowsDifferent { get; init; }
}
