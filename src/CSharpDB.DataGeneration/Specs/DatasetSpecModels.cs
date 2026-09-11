using System.Text.Json;
using System.Text.Json.Serialization;

namespace CSharpDB.DataGen.Specs;

public sealed class DatasetSpec
{
    public string Dataset { get; set; } = string.Empty;
    public List<SqlTableSpec> Tables { get; set; } = [];
    public List<CollectionSpec> Collections { get; set; } = [];
}

public sealed class SqlTableSpec
{
    public string GeneratorKey { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public string OutputFileName { get; set; } = string.Empty;
    public JsonElement RowCount { get; set; }
    public List<RuleBindingSpec> Locals { get; set; } = [];
    public List<SqlColumnSpec> Columns { get; set; } = [];
    public List<SqlIndexSpec> Indexes { get; set; } = [];
}

public sealed class SqlColumnSpec
{
    public string Name { get; set; } = string.Empty;
    public string? SourceField { get; set; }
    public string Type { get; set; } = string.Empty;
    public bool Nullable { get; set; } = true;
    public bool PrimaryKey { get; set; }
    public JsonElement Generator { get; set; }
}

public sealed class SqlIndexSpec
{
    public string Name { get; set; } = string.Empty;
    public List<string> Columns { get; set; } = [];

    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
    public bool Unique { get; set; }
}

public sealed class CollectionSpec
{
    public string GeneratorKey { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public string OutputFileName { get; set; } = string.Empty;
    public JsonElement RowCount { get; set; }
    public List<RuleBindingSpec> Locals { get; set; } = [];
    public JsonElement Key { get; set; }
    public JsonElement Document { get; set; }
    public List<string> IndexPaths { get; set; } = [];
}

public sealed class RuleBindingSpec
{
    public string Name { get; set; } = string.Empty;
    public JsonElement Value { get; set; }
}

public sealed record LoadedDatasetSpec(string SourcePath, DatasetSpec Spec);

public sealed record GeneratedSqlTableSource(
    string GeneratorKey,
    long RowCount,
    Func<IEnumerable<IReadOnlyDictionary<string, object?>>> CreateRows);

public sealed record GeneratedCollectionDocument(string Key, JsonElement Document);

public sealed record GeneratedCollectionSource(
    string GeneratorKey,
    long RowCount,
    Func<IEnumerable<GeneratedCollectionDocument>> CreateDocuments);

public sealed record DatasetGenerationPlan(
    IReadOnlyDictionary<string, long> Counts,
    IReadOnlyDictionary<string, GeneratedSqlTableSource> SqlSources,
    IReadOnlyDictionary<string, GeneratedCollectionSource> CollectionSources);
