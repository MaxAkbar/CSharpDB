using System.Text.Json;
using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;
using CSharpDB.Client.Models;
using CSharpDB.Sql;

namespace CSharpDB.DevOps;

public sealed class DefinitionDependencyService(IDefinitionModuleAnalyzer? moduleAnalyzer = null)
{
    private readonly ConcurrentDictionary<string, SqlDependencyAnalysis> _sqlCache = new();
    private readonly ConcurrentDictionary<string, ModuleDefinitionInspection> _moduleCache = new();
    public DefinitionAnalysis Analyze(DefinitionCatalogSnapshot catalog, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        // Bindings are rebuilt per catalog/route. Syntax results include the complete relation fingerprint.
        if (_sqlCache.Count > 16384) _sqlCache.Clear();
        if (_moduleCache.Count > 512) _moduleCache.Clear();
        return new DefinitionAnalysisBuilder(catalog, moduleAnalyzer, _sqlCache, _moduleCache, ct).Build();
    }
}

internal sealed partial class DefinitionAnalysisBuilder(DefinitionCatalogSnapshot catalog, IDefinitionModuleAnalyzer? modules,
    ConcurrentDictionary<string, SqlDependencyAnalysis> sqlCache, ConcurrentDictionary<string, ModuleDefinitionInspection> moduleCache, CancellationToken ct)
{
    private string _schemaFingerprint = "";
    private readonly SqlDependencyAnalyzer _sql = new();
    private readonly Dictionary<string, IReadOnlyList<string>> _columns = new(StringComparer.Ordinal);
    private readonly Dictionary<string, DefinitionCatalogRecord> _relations = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, TableSchema> _tableSchemas = new(StringComparer.Ordinal);
    private readonly HashSet<string> _active = new(StringComparer.Ordinal);
    private readonly HashSet<string> _analyzed = new(StringComparer.Ordinal);
    private readonly List<DefinitionDependency> _edges = [];
    private readonly List<DefinitionAnalysisDiagnostic> _diagnostics = [];
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);
    private static bool Same(string? a, string? b) => string.Equals(a, b, StringComparison.OrdinalIgnoreCase);

    public DefinitionAnalysis Build()
    {
        _schemaFingerprint = Hash(string.Join("\n", catalog.Definitions.Where(d => d.Kind is "Table" or "View" or "External table").OrderBy(d => d.Id)
            .Select(d => d.Id + ":" + d.Name + ":" + Hash(d.Source))));
        foreach (var definition in catalog.Definitions.Where(d => d.Kind is "Table" or "View" or "External table")) _relations[definition.Name] = definition;
        foreach (var definition in catalog.Definitions.Where(d => d.Kind is "Table" or "External table"))
        {
            ct.ThrowIfCancellationRequested();
            try
            {
                if (JsonSerializer.Deserialize<TableSchema>(definition.Source, JsonOptions) is { } schema)
                {
                    _tableSchemas[definition.Id] = schema;
                    _columns[definition.Id] = schema.Columns.Select(c => c.Name).ToArray();
                }
            }
            catch (JsonException ex) { Diagnostic(definition, "schema", ex.Message); }
        }
        IndexDataModelDefinitions();
        foreach (var definition in catalog.Definitions) Analyze(definition);
        return new(catalog, _edges.Distinct().ToArray(), _diagnostics.Distinct().ToArray(), _columns);
    }

    private SqlDependencyRelation? Resolve(string name)
    {
        if (!_relations.TryGetValue(name, out var definition)) return null;
        if (!_columns.ContainsKey(definition.Id)) Analyze(definition);
        return new(definition.Name, _columns.GetValueOrDefault(definition.Id) ?? []);
    }

    private void Analyze(DefinitionCatalogRecord definition)
    {
        ct.ThrowIfCancellationRequested();
        if (_analyzed.Contains(definition.Id)) return;
        if (_active.Count >= 64 || !_active.Add(definition.Id)) { Diagnostic(definition, "definition", "Cyclic or deeply nested definition. Review the unresolved path."); return; }
        try
        {
            if (definition.Kind == "Default") Reference(definition, definition.OwnerName, definition.Name, "Default", "column");
            if (definition.Kind == "Check" && definition.MetadataJson is not null)
            {
                using var metadata = JsonDocument.Parse(definition.MetadataJson);
                if (Text(metadata.RootElement, "columnName") is { } column)
                    Reference(definition, definition.OwnerName, column, "Check", "columnName");
            }
            if (definition.Format is "sql" or "expression") AnalyzeSql(definition, definition.Source, "definition", definition.OwnerName, definition.Format == "expression");
            else if (definition.Format is not ("table" or "column" or "externalTable")) AnalyzeNative(definition);
        }
        catch (JsonException ex) { Diagnostic(definition, "definition", "Malformed stored definition: " + ex.Message); }
        catch (InvalidOperationException ex) { Diagnostic(definition, "definition", "Stored definition could not be analyzed: " + ex.Message); }
        finally { _active.Remove(definition.Id); _analyzed.Add(definition.Id); }
    }

    private void AnalyzeSql(DefinitionCatalogRecord definition, string sql, string location,
        string? owner = null, bool expression = false, DependencyConfidence confidence = DependencyConfidence.Confirmed)
    {
        if (string.IsNullOrWhiteSpace(sql)) { Diagnostic(definition, location, "The stored SQL definition is empty."); return; }
        string cacheKey = Hash(_schemaFingerprint + "\0" + owner + "\0" + expression + "\0" + sql);
        var result = sqlCache.GetOrAdd(cacheKey, _ => _sql.Analyze(sql, Resolve, owner, expression, ct));
        // Resolve cached references too: view output metadata and cycle diagnostics belong to this build.
        foreach (string relation in result.References.Select(r => r.Relation).Distinct(StringComparer.OrdinalIgnoreCase)) Resolve(relation);
        if (!expression && definition.Kind is "View" or "Saved query") _columns[definition.Id] = result.OutputColumns;
        foreach (var reference in result.References)
        {
            if (_relations.TryGetValue(reference.Relation, out var target))
                _edges.Add(new(definition.Id, target.Id, reference.Column, reference.OutputColumn, reference.Usage, location,
                    reference.Span, reference.Resolved ? confidence : DependencyConfidence.Possible));
        }
        foreach (var diagnostic in result.Diagnostics) Diagnostic(definition, $"{location}, line {diagnostic.Span.Line}", diagnostic.Message);
        if (definition.Kind == "Trigger" && owner is not null) Reference(definition, owner, null, "Trigger owner", location);
    }

    private void Reference(DefinitionCatalogRecord definition, string? name, string? column, string usage, string location,
        string? kind = null, DependencyConfidence confidence = DependencyConfidence.Confirmed, string? outputColumn = null,
        DependencyRelationshipKind relationship = DependencyRelationshipKind.Usage)
    {
        if (string.IsNullOrWhiteSpace(name)) return;
        var candidates = kind is null ? (_relations.TryGetValue(name, out var relation) ? [relation] : Array.Empty<DefinitionCatalogRecord>())
            : catalog.Definitions.Where(d => d.Kind == kind && (Same(d.Name, name) || d.Id.EndsWith(":" + name, StringComparison.Ordinal))).ToArray();
        if (candidates.Length != 1) { Diagnostic(definition, location, $"Unresolved {kind ?? "source"} '{name}'."); return; }
        var target = candidates[0];
        if (column is not null)
        {
            Analyze(target);
            if (!_columns.GetValueOrDefault(target.Id, []).Any(c => Same(c, column)))
            { confidence = DependencyConfidence.Possible; Diagnostic(definition, location, $"Unresolved field '{name}.{column}'."); }
        }
        _edges.Add(new(definition.Id, target.Id, column, outputColumn, usage, location, Confidence: confidence, Relationship: relationship));
    }
    private void Diagnostic(DefinitionCatalogRecord definition, string location, string message) => _diagnostics.Add(new(definition.Id, location, message));
    private static string Hash(string value) => Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(value)));
}
