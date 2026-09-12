using System.Text.Json;
using CSharpDB.Client.Models;

namespace CSharpDB.DevOps;

public static class ColumnImpactService
{
    public static ColumnImpactReport Assess(DefinitionAnalysis analysis, string table, string column, ColumnChangeKind change, CancellationToken ct = default)
    {
        var definitions = analysis.Catalog.Definitions.ToDictionary(d => d.Id, StringComparer.Ordinal);
        var root = definitions.Values.FirstOrDefault(d => d.Kind == "Table" && Same(d.Name, table))
            ?? throw new ArgumentException($"Table '{table}' was not found.");
        if (!analysis.OutputColumns.GetValueOrDefault(root.Id, []).Any(c => Same(c, column))) throw new ArgumentException($"Column '{table}.{column}' was not found.");
        var findings = new List<ColumnImpactFinding>();
        var incoming = analysis.Dependencies.Where(e => e.Relationship == DependencyRelationshipKind.Usage).ToLookup(e => e.TargetId, StringComparer.Ordinal);
        var annotations = analysis.Dependencies.Where(e => e.Relationship != DependencyRelationshipKind.Usage).ToLookup(e => e.TargetId, StringComparer.Ordinal);
        var visited = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var queue = new Queue<(string Id, string? Column, List<DefinitionDependency> Path)>();
        queue.Enqueue((root.Id, column, []));
        while (queue.TryDequeue(out var state))
        {
            ct.ThrowIfCancellationRequested();
            if (!visited.Add(state.Id + "\0" + state.Column)) continue;
            // Saved diagrams and draft/archive relationships are advisory annotations. They never
            // become executable lineage or participate in the engine's DDL restriction traversal.
            foreach (var edge in annotations[state.Id])
            {
                if (state.Column is not null && edge.TargetColumn is not null && !Same(edge.TargetColumn, state.Column)) continue;
                // Column edits carry table context for the Uses graph. That context alone does not
                // mean a proposal references every other column in the table.
                if (state.Column is not null && edge.TargetColumn is null && edge.Relationship == DependencyRelationshipKind.ProposedChange
                    && edge.Usage is not ("Proposed RenameTable" or "Proposed DropTable")) continue;
                var category = edge.Relationship switch
                {
                    DependencyRelationshipKind.ModelMembership => ColumnImpactCategory.ModelMembership,
                    DependencyRelationshipKind.ProposedChange => ColumnImpactCategory.ProposedChange,
                    _ => ColumnImpactCategory.ExternalArchive,
                };
                string reason = category switch
                {
                    ColumnImpactCategory.ModelMembership => "Saved model membership: review the diagram when this column changes.",
                    ColumnImpactCategory.ProposedChange => "Saved proposed change: review this draft against the changed schema; it has not been applied.",
                    _ => "External archive relationship: review its saved source metadata separately from the active schema.",
                };
                findings.Add(new(edge.SourceId, category, reason, [.. state.Path, edge], edge.Location));
            }
            foreach (var edge in incoming[state.Id])
            {
                bool shape = edge.Usage == "Wildcard" || (edge.Usage == "Positional insert" && change != ColumnChangeKind.Rename);
                bool wholeObject = edge.TargetColumn is null && edge.Usage is "Saved SQL action" or "Module query" or "Module call" or "Procedure action" or "Open form";
                if (state.Column is not null && !Same(edge.TargetColumn, state.Column) && !shape && !wholeObject) continue;
                var path = new List<DefinitionDependency>(state.Path) { edge };
                var category = path.Any(e => e.Confidence == DependencyConfidence.Possible) ? ColumnImpactCategory.NeedsReview
                    : path.Count == 1 ? ColumnImpactCategory.ConfirmedUsage : ColumnImpactCategory.IndirectEffects;
                findings.Add(new(edge.SourceId, category,
                    shape ? $"{edge.Usage} depends on the table's column shape." : $"{edge.Usage}: {table}.{column} affects this definition.", path, edge.Location));
                // Rename/drop can invalidate the whole referencing definition. Type/nullability follow output lineage.
                queue.Enqueue((edge.SourceId, change is ColumnChangeKind.Rename or ColumnChangeKind.Drop ? null : edge.OutputColumn, path));
            }
        }
        // The engine deliberately checks table-level view dependencies for column rewrites.
        var tableDependents = new Queue<string>(); tableDependents.Enqueue(root.Id);
        var seen = new HashSet<string>(StringComparer.Ordinal);
        while (tableDependents.TryDequeue(out string? id))
        {
            ct.ThrowIfCancellationRequested();
            if (!seen.Add(id)) continue;
            foreach (var edge in incoming[id].Where(e => definitions[e.SourceId].Kind == "View" && change != ColumnChangeKind.Nullability))
            {
                findings.Add(new(edge.SourceId, ColumnImpactCategory.EngineRestriction,
                    "The engine blocks column changes while this view depends on the table, even when the column is not explicitly selected.", [edge], edge.Location));
                tableDependents.Enqueue(edge.SourceId);
            }
        }
        foreach (var edge in incoming[root.Id])
        {
            var definition = definitions[edge.SourceId];
            bool ownerTrigger = definition.Kind == "Trigger" && Same(definition.OwnerName, table);
            bool columnDependency = Same(edge.TargetColumn, column);
            bool restricted = change != ColumnChangeKind.Nullability && (ownerTrigger
                || (definition.Kind == "Validation rule" && edge.TargetColumn is null)
                || (columnDependency && definition.Kind is "Trigger" or "Validation rule")
                || (definition.Kind == "Trigger" && edge.Usage == "Positional insert" && change == ColumnChangeKind.Drop)
                || (columnDependency && definition.Kind is "Foreign key" or "Key" && change is ColumnChangeKind.Drop or ColumnChangeKind.Type)
                || (columnDependency && definition.Kind is "Check" or "Index" && change == ColumnChangeKind.Drop));
            if (restricted) findings.Add(new(definition.Id, ColumnImpactCategory.EngineRestriction,
                $"{definition.Kind} participates in the engine's {change.ToString().ToLowerInvariant()} restrictions; review this dependency before changing the column.", [edge], edge.Location));
            if (columnDependency && definition.Kind is "Index" or "Check" && change == ColumnChangeKind.Type)
                findings.Add(new(definition.Id, ColumnImpactCategory.NeedsReview, "Compatibility depends on the proposed type. The engine validates checks and only rebuilds compatible ready SQL indexes.", [edge], edge.Location));
        }
        try
        {
            var schema = JsonSerializer.Deserialize<TableSchema>(root.Source, new JsonSerializerOptions(JsonSerializerDefaults.Web));
            var field = schema?.Columns.FirstOrDefault(c => Same(c.Name, column));
            if (field is not null)
            {
                bool primary = field.IsPrimaryKey || schema!.KeyConstraints.Any(k => k.Kind == KeyConstraintKind.PrimaryKey && k.Columns.Any(c => Same(c, column)));
                if ((change == ColumnChangeKind.Drop && (primary || schema!.Columns.Count == 1)) || (change == ColumnChangeKind.Type && (primary || field.IsIdentity)))
                    findings.Add(new(root.Id, ColumnImpactCategory.EngineRestriction, "Primary key/identity storage or the last remaining column prevents this change.", []));
                if (change == ColumnChangeKind.Nullability && (primary || field.IsIdentity))
                    findings.Add(new(root.Id, ColumnImpactCategory.EngineRestriction, "The engine does not allow dropping NOT NULL from a primary key or identity column.", []));
                if (change == ColumnChangeKind.Type && field.DefaultSql is not null)
                    findings.Add(new(root.Id, ColumnImpactCategory.NeedsReview, "The column default must remain compatible with the proposed type: " + field.DefaultSql, []));
            }
        }
        catch (JsonException) { /* Reported by the catalog analyzer. */ }
        foreach (var diagnostic in analysis.Diagnostics)
            findings.Add(new(diagnostic.ObjectId, ColumnImpactCategory.NeedsReview, diagnostic.Message, [], diagnostic.Location));
        foreach (var diagnostic in analysis.Catalog.Diagnostics)
            findings.Add(new("", ColumnImpactCategory.NeedsReview, diagnostic.Source + ": " + diagnostic.Message, []));
        if (change is ColumnChangeKind.Type or ColumnChangeKind.Nullability)
            findings.Add(new(root.Id, ColumnImpactCategory.NeedsReview, "Stored row values were not scanned. Conversion or nullability validation can still reject this change.", []));
        return new(table, column, change, findings.DistinctBy(f => (f.ObjectId, f.Category, f.Reason, f.Location)).ToArray(),
            analysis.Diagnostics.Count == 0 && analysis.Catalog.Diagnostics.Count == 0);
    }
    private static bool Same(string? a, string? b) => string.Equals(a, b, StringComparison.OrdinalIgnoreCase);
}
