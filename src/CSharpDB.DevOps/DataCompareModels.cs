namespace CSharpDB.DevOps;

public enum DataChangeKind
{
    SourceOnly,
    TargetOnly,
    Changed,
    Unmatched,
}

public sealed class DataCompareOptions
{
    public required string TableName { get; init; }
    public IReadOnlyList<string> KeyColumns { get; init; } = [];
    public int MaxPreviewRows { get; init; } = 100;
}

public sealed class DataDiffReport
{
    public required SchemaTargetDescriptor Source { get; init; }
    public required SchemaTargetDescriptor Target { get; init; }
    public required string TableName { get; init; }
    public required IReadOnlyList<string> KeyColumns { get; init; }
    public required DataDiffSummary Summary { get; init; }
    public required IReadOnlyList<DataDiffRow> Rows { get; init; }
    public IReadOnlyList<string> Warnings { get; init; } = [];
    public DateTime GeneratedUtc { get; init; } = DateTime.UtcNow;
}

public sealed class DataDiffSummary
{
    public int SourceRowCount { get; init; }
    public int TargetRowCount { get; init; }
    public int ComparedRowCount { get; init; }
    public int SourceOnlyRows { get; init; }
    public int TargetOnlyRows { get; init; }
    public int ChangedRows { get; init; }
    public int UnmatchedRows { get; init; }
    public int PreviewRowCount { get; init; }
    public bool HasDifferences => SourceOnlyRows > 0 || TargetOnlyRows > 0 || ChangedRows > 0 || UnmatchedRows > 0;
}

public sealed class DataDiffRow
{
    public required DataChangeKind ChangeKind { get; init; }
    public required IReadOnlyDictionary<string, object?> Key { get; init; }
    public IReadOnlyDictionary<string, object?>? SourceValues { get; init; }
    public IReadOnlyDictionary<string, object?>? TargetValues { get; init; }
    public IReadOnlyList<string> ChangedColumns { get; init; } = [];
    public string? Warning { get; init; }
}

public interface IDataCompareTarget
{
    SchemaTargetDescriptor Descriptor { get; }
    Task<CSharpDB.Client.Models.TableSchema?> GetTableSchemaAsync(string tableName, CancellationToken ct = default);
    IAsyncEnumerable<IReadOnlyDictionary<string, object?>> ReadRowsAsync(
        CSharpDB.Client.Models.TableSchema schema,
        CancellationToken ct = default);
}
