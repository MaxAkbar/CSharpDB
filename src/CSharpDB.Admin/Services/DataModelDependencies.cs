using CSharpDB.Admin.Models;
using CSharpDB.CodeModules;
using CSharpDB.DevOps;

namespace CSharpDB.Admin.Services;

public sealed partial class DataModelService
{
    private readonly DefinitionDependencyService _dependencyAnalyzer = new(new ModuleDefinitionAnalyzer());

    private Task<DataModelDependencyInspection> ReadDependencyInspectionAsync(CancellationToken ct) => Task.Run(async () =>
    {
        // Embedded metadata reads may perform synchronous work before yielding.
        // Keep the entire inspection off the Blazor dispatcher, including page one.
        string? database = client.DataSource;
        try
        {
            var catalog = await DefinitionCatalogService.ReadAsync(client, ct: ct).ConfigureAwait(false);
            var analysis = _dependencyAnalyzer.Analyze(catalog, ct);
            if (!string.Equals(database, client.DataSource, StringComparison.Ordinal))
                throw new InvalidOperationException("The database changed during inspection. Refresh the model.");
            return DataModelDependencyInspection.Create(analysis, ct);
        }
        catch (OperationCanceledException) { throw; }
        catch (Exception ex)
        {
            return new(new Dictionary<string, IReadOnlyList<DataModelDependency>>(),
                [$"Dependency inspection is unavailable: {ex.Message} Refresh the model to retry."]);
        }
    }, ct);
}

internal sealed class DataModelDependencyInspection(
    IReadOnlyDictionary<string, IReadOnlyList<DataModelDependency>> sources, IReadOnlyList<string> warnings)
{
    public static readonly DataModelDependencyInspection Empty = new(new Dictionary<string, IReadOnlyList<DataModelDependency>>(), []);
    public IReadOnlyList<string> Warnings { get; } = warnings;
    public IReadOnlyList<DataModelDependency> ForSource(string kind, string name) => sources.GetValueOrDefault(Key(kind, name), []);
    private static string Key(string kind, string name) => kind + ":" + name;

    public static DataModelDependencyInspection Create(DefinitionAnalysis analysis, CancellationToken ct)
    {
        var definitions = analysis.Catalog.Definitions.ToDictionary(d => d.Id, StringComparer.Ordinal);
        var incoming = analysis.Dependencies.ToLookup(edge => edge.TargetId, StringComparer.Ordinal);
        var sources = new Dictionary<string, IReadOnlyList<DataModelDependency>>(StringComparer.OrdinalIgnoreCase);
        foreach (var target in analysis.Catalog.Definitions.Where(d => d.Kind is "Table" or "External table"))
        {
            ct.ThrowIfCancellationRequested();
            var found = new List<(DefinitionDependency Edge, int Depth, bool Possible)>();
            var queue = new Queue<(string Id, int Depth, bool Possible)>();
            var visited = new HashSet<(string, bool)>();
            queue.Enqueue((target.Id, 0, false));
            while (queue.TryDequeue(out var current))
            {
                ct.ThrowIfCancellationRequested();
                if (!visited.Add((current.Id, current.Possible))) continue;
                foreach (var edge in incoming[current.Id])
                {
                    if (edge.SourceId == target.Id || !definitions.ContainsKey(edge.SourceId)) continue;
                    bool possible = current.Possible || edge.Confidence == DependencyConfidence.Possible;
                    found.Add((edge, current.Depth, possible));
                    // Diagram membership and proposed/archive relationships are evidence,
                    // not executable dependency paths through which usage propagates.
                    if (edge.Relationship == DependencyRelationshipKind.Usage)
                        queue.Enqueue((edge.SourceId, current.Depth + 1, possible));
                }
            }

            sources[Key(target.Kind, target.Name)] = found.GroupBy(item => (item.Edge.SourceId, item.Edge.Relationship))
                .Select(group =>
                {
                    var definition = definitions[group.Key.SourceId];
                    return new DataModelDependency(definition.Kind, definition.Name,
                        definition.Source.Length <= 1200 ? definition.Source : definition.Source[..1200] + "…")
                    {
                        ObjectId = definition.Id,
                        Relationship = group.Key.Relationship switch
                        {
                            DependencyRelationshipKind.ModelMembership => "Model membership",
                            DependencyRelationshipKind.ProposedChange => "Proposed change",
                            DependencyRelationshipKind.ExternalArchive => "External archive",
                            _ => "Usage",
                        },
                        IsIndirect = group.All(item => item.Depth > 0),
                        NeedsReview = group.Any(item => item.Possible),
                        Columns = group.Where(item => item.Depth == 0 && item.Edge.TargetColumn is not null)
                            .Select(item => item.Edge.TargetColumn!).Distinct(StringComparer.OrdinalIgnoreCase).Order(StringComparer.OrdinalIgnoreCase).ToArray(),
                        Evidence = group.Select(item => $"{item.Edge.Usage} · {item.Edge.Location}" +
                            (item.Edge.Span is { } span ? $", line {span.Line}" : "") +
                            (item.Depth > 0 && definitions.TryGetValue(item.Edge.TargetId, out var via) ? $" · via {via.Name}" : ""))
                            .Distinct(StringComparer.Ordinal).ToArray(),
                    };
                }).OrderBy(item => item.Relationship, StringComparer.Ordinal).ThenBy(item => item.Kind, StringComparer.Ordinal)
                .ThenBy(item => item.Name, StringComparer.OrdinalIgnoreCase).ToArray();
        }

        var warnings = analysis.Catalog.Diagnostics.Select(d => $"{d.Source}: {d.Message}")
            .Concat(analysis.Diagnostics.Select(d => $"{(definitions.TryGetValue(d.ObjectId, out var definition) ? definition.Name : d.ObjectId)} · {d.Location}: {d.Message}"))
            .Distinct(StringComparer.Ordinal).ToArray();
        return new(sources, warnings);
    }
}
