using System.Text.Json;
using CSharpDB.Client.Models;
using CSharpDB.Sql;

namespace CSharpDB.DevOps;

internal sealed partial class DefinitionAnalysisBuilder
{
    private static JsonElement Property(JsonElement element, string name)
        => element.ValueKind == JsonValueKind.Object
            ? element.EnumerateObject().FirstOrDefault(p => Same(p.Name, name)).Value : default;
    private static string? Text(JsonElement element, string name) => String(Property(element, name));
    private static string? String(JsonElement element) => element.ValueKind is JsonValueKind.String ? element.GetString()
        : element.ValueKind is JsonValueKind.Number ? element.ToString() : null;
    private static IEnumerable<JsonElement> Items(JsonElement element) => element.ValueKind == JsonValueKind.Array ? element.EnumerateArray() : [];
    private static IEnumerable<string> Strings(JsonElement element) => Items(element).Select(String).OfType<string>();

    private void AnalyzeNative(DefinitionCatalogRecord definition)
    {
        if (definition.Format is "dataModel" or "modelChange" or "archiveForeignKey")
        {
            using var stored = JsonDocument.Parse(definition.Source);
            if (definition.Format == "dataModel") AnalyzeDataModel(definition, stored.RootElement);
            else if (definition.Format == "modelChange") AnalyzeModelChange(definition);
            else AnalyzeArchiveRelationship(definition, stored.RootElement);
            return;
        }
        using var document = JsonDocument.Parse(definition.MetadataJson ?? definition.Source);
        var root = document.RootElement;
        switch (definition.Format)
        {
            case "key": case "index":
                foreach (string column in Strings(Property(root, "columns"))) Reference(definition, definition.OwnerName, column, definition.Kind, "columns");
                break;
            case "foreignKey":
                foreach (string column in Strings(Property(root, "columnNames")).DefaultIfEmpty(Text(root, "columnName") ?? ""))
                    Reference(definition, definition.OwnerName, column, "Foreign key", "columnNames");
                foreach (string column in Strings(Property(root, "referencedColumnNames")).DefaultIfEmpty(Text(root, "referencedColumnName") ?? ""))
                    Reference(definition, Text(root, "referencedTableName"), column, "Referenced key", "referencedColumnNames");
                break;
            case "validation":
                Reference(definition, definition.OwnerName, Text(root, "column_name"), "Validation", "column_name");
                if ((Text(root, "expression_sql") ?? Text(root, "expression")) is { } expression)
                    AnalyzeSql(definition, expression, "expression_sql", definition.OwnerName, true);
                else Diagnostic(definition, "expression", "Validation expression is unavailable.");
                break;
            case "form":
                string? table = Text(root, "tableName") ?? definition.OwnerName;
                Reference(definition, table, null, "Source", "tableName");
                WalkForm(definition, root, table, "$", 0);
                break;
            case "report": AnalyzeReport(definition, root); break;
            case "pipeline": AnalyzePipeline(definition, root); break;
            case "module": AnalyzeModule(definition, root); break;
            default: Diagnostic(definition, "definition", $"Unsupported definition format '{definition.Format}'."); break;
        }
    }

    private void WalkForm(DefinitionCatalogRecord definition, JsonElement element, string? table, string path, int depth)
    {
        ct.ThrowIfCancellationRequested();
        if (depth > 64) { Diagnostic(definition, path, "Nested configuration exceeds the inspection limit."); return; }
        if (element.ValueKind == JsonValueKind.Array)
        {
            int i = 0; foreach (var item in element.EnumerateArray()) WalkForm(definition, item, table, $"{path}[{i++}]", depth + 1); return;
        }
        if (element.ValueKind != JsonValueKind.Object) return;
        string? local = Text(element, "childTable") ?? Text(element, "tableName") ?? table;
        if (Text(element, "childTable") is { } child)
        {
            Reference(definition, child, Text(element, "foreignKeyField"), "Child binding", path + ".foreignKeyField");
            Reference(definition, table, Text(element, "parentKeyField"), "Parent binding", path + ".parentKeyField");
        }
        if (Text(element, "lookupTable") is { } lookup)
        {
            foreach (string field in new[] { "valueField", "displayField" })
                Reference(definition, lookup, Text(element, field), "Lookup", path + "." + field);
            foreach (string field in Strings(Property(element, "displayFields"))) Reference(definition, lookup, field, "Lookup display", path + ".displayFields");
        }
        foreach (string field in new[] { "fieldName", "boundFieldName" })
            if (Text(element, field) is { } name) Reference(definition, local, name, "Binding", path + "." + field);
        foreach (var column in Strings(Property(element, "visibleColumns"))) Reference(definition, local, column, "Visible field", path + ".visibleColumns");
        if (Text(element, "moduleId") is { } module) Reference(definition, module, null, "Handler", path + ".moduleId", "Module");
        string? kind = Text(element, "kind");
        var args = Property(element, "arguments");
        if (Same(kind, "RunSql") || kind == "21")
        {
            string? sql = Text(element, "value") ?? Text(element, "target") ?? Text(args, "sql") ?? Text(args, "name");
            if (sql is not null)
            {
                if (catalog.Definitions.Any(d => d.Kind == "Saved query" && Same(d.Name, sql))) Reference(definition, sql, null, "Saved SQL action", path, "Saved query");
                else AnalyzeSql(definition, sql, path + ".sql", local);
            }
            else Diagnostic(definition, path, "SQL action target is dynamic or unavailable.");
        }
        if (Same(kind, "RunProcedure") || kind == "22")
            Reference(definition, Text(element, "target") ?? Text(element, "value") ?? Text(args, "procedureName") ?? Text(args, "procedure") ?? Text(args, "name"), null, "Procedure action", path, "Procedure");
        if (Same(kind, "RunCommand") || kind == "0") Diagnostic(definition, path, "Host command implementation is outside the stored definition catalog.");
        foreach (string field in new[] { "expression", "condition", "filterExpression" })
            if (Text(element, field) is { Length: > 0 } expression) AnalyzeSql(definition, expression.TrimStart('='), path + "." + field, local, true);
        foreach (var property in element.EnumerateObject())
            if (property.Value.ValueKind is JsonValueKind.Object or JsonValueKind.Array)
                WalkForm(definition, property.Value, local, path + "." + property.Name, depth + 1);
    }

    private void AnalyzeReport(DefinitionCatalogRecord definition, JsonElement root)
    {
        var source = Property(root, "source");
        string? name = Text(source, "name");
        if (string.IsNullOrWhiteSpace(name)) Diagnostic(definition, "source", "Report source is missing or unresolved.");
        string? kind = Text(source, "kind");
        string sourceKind = Same(kind, "savedQuery") || kind == "2" ? "Saved query" : Same(kind, "view") || kind == "1" ? "View" : "Table";
        Reference(definition, name, null, "Report source", "source", sourceKind);
        void Walk(JsonElement element, string path)
        {
            ct.ThrowIfCancellationRequested();
            if (element.ValueKind == JsonValueKind.Array) { int i = 0; foreach (var item in element.EnumerateArray()) Walk(item, $"{path}[{i++}]"); return; }
            if (element.ValueKind != JsonValueKind.Object) return;
            foreach (string field in new[] { "boundFieldName", "fieldName" })
                if (Text(element, field) is { } column) Reference(definition, name, column, "Report field", path + "." + field, sourceKind);
            if (Text(element, "expression") is { Length: > 0 } expression && name is not null)
            {
                var target = catalog.Definitions.FirstOrDefault(d => d.Kind == sourceKind && Same(d.Name, name));
                if (target is not null)
                {
                    Analyze(target);
                    var result = _sql.Analyze(expression.TrimStart('='), n => Same(n, name) ? new(name, _columns.GetValueOrDefault(target.Id, [])) : Resolve(n), name, true, ct);
                    foreach (var reference in result.References) Reference(definition, name, reference.Column, "Report expression", path + ".expression", sourceKind);
                    foreach (var diagnostic in result.Diagnostics) Diagnostic(definition, path + ".expression", diagnostic.Message);
                }
            }
            foreach (var property in element.EnumerateObject()) if (property.Value.ValueKind is JsonValueKind.Object or JsonValueKind.Array) Walk(property.Value, path + "." + property.Name);
        }
        Walk(root, "$");
        foreach (var binding in Items(Property(root, "eventBindings")))
            Diagnostic(definition, "eventBindings", "Report event handler implementation may have additional runtime dependencies.");
    }

    private void AnalyzePipeline(DefinitionCatalogRecord definition, JsonElement root)
    {
        var source = Property(root, "source");
        var lineage = new Dictionary<string, List<DefinitionDependency>>(StringComparer.OrdinalIgnoreCase);
        bool externalSource = !string.IsNullOrWhiteSpace(Text(source, "connectionString"));
        string? sourceKind = Text(source, "kind");
        bool fileSource = sourceKind is "0" or "1" || Same(sourceKind, "CsvFile") || Same(sourceKind, "JsonFile");
        if (externalSource || fileSource) Diagnostic(definition, "source", "External source: dependencies were not bound to the active database.");
        else if (Text(source, "queryText") is { Length: > 0 } query)
        {
            int start = _edges.Count;
            AnalyzeSql(definition, query, "source.queryText");
            foreach (var group in _edges.Skip(start).Where(e => e.OutputColumn is not null).GroupBy(e => e.OutputColumn!, StringComparer.OrdinalIgnoreCase)) lineage[group.Key] = group.ToList();
        }
        else if (Text(source, "tableName") is { } table && _relations.TryGetValue(table, out var relation))
        {
            Reference(definition, table, null, "Pipeline source", "source.tableName");
            foreach (string column in Resolve(table)?.Columns ?? [])
            {
                var edge = new DefinitionDependency(definition.Id, relation.Id, column, column, "Read", "source.tableName");
                lineage[column] = [edge]; _edges.Add(edge);
            }
        }
        else Diagnostic(definition, "source", "Pipeline source is missing or unresolved.");
        var watermark = Text(Property(root, "incremental"), "watermarkColumn");
        if (watermark is not null) UseField(watermark, "incremental.watermarkColumn", null);
        int ordinal = 0;
        foreach (var transform in Items(Property(root, "transforms")))
        {
            string path = $"transforms[{ordinal++}]";
            string? kind = Text(transform, "kind");
            bool Is(string label, string number) => Same(kind, label) || kind == number;
            if (Is("Select", "0")) foreach (var column in Strings(Property(transform, "selectColumns"))) UseField(column, path + ".selectColumns", column);
            if (Is("Select", "0") && Property(transform, "selectColumns").ValueKind == JsonValueKind.Array)
            {
                var selected = Strings(Property(transform, "selectColumns")).ToHashSet(StringComparer.OrdinalIgnoreCase);
                foreach (string key in lineage.Keys.Where(k => !selected.Contains(k)).ToArray()) lineage.Remove(key);
            }
            if (Is("Rename", "1")) foreach (var mapping in Items(Property(transform, "renameMappings")))
            {
                string? from = Text(mapping, "source"), to = Text(mapping, "target");
                if (from is null || to is null) continue;
                UseField(from, path + ".renameMappings", to);
                if (lineage.Remove(from, out var dependencies)) lineage[to] = dependencies;
            }
            if (Is("Cast", "2")) foreach (var mapping in Items(Property(transform, "castMappings"))) if (Text(mapping, "column") is { } column) UseField(column, path + ".castMappings", column);
            if (Is("Deduplicate", "5")) foreach (var column in Strings(Property(transform, "deduplicateKeys"))) UseField(column, path + ".deduplicateKeys", null);
            if (Is("Filter", "3") && Text(transform, "filterExpression") is { } filter) InspectExpression(filter, path + ".filterExpression", null);
            if (Is("Derive", "4")) foreach (var derived in Items(Property(transform, "derivedColumns")))
                if (Text(derived, "expression") is { } expression) InspectExpression(expression, path + ".derivedColumns", Text(derived, "name"));
            if (!new[] { "Select", "Rename", "Cast", "Filter", "Derive", "Deduplicate", "0", "1", "2", "3", "4", "5" }.Any(k => Same(k, kind))) Diagnostic(definition, path, "Unknown pipeline transformation requires review.");
        }
        var destination = Property(root, "destination");
        string? destinationKind = Text(destination, "kind");
        if (!string.IsNullOrWhiteSpace(Text(destination, "connectionString")) || destinationKind is "1" or "2" || Same(destinationKind, "CsvFile") || Same(destinationKind, "JsonFile")) Diagnostic(definition, "destination", "External destination: dependencies were not bound to the active database.");
        else if (Text(destination, "tableName") is { } target)
        {
            Reference(definition, target, null, "Pipeline destination", "destination.tableName");
            foreach (string column in lineage.Keys) Reference(definition, target, column, "Write", "destination." + column);
            if (lineage.Count == 0) Diagnostic(definition, "destination", "Destination column mapping depends on external or unresolved source fields.");
        }
        if (Items(Property(root, "hooks")).Any()) Diagnostic(definition, "hooks", "Pipeline host commands may contain additional runtime dependencies.");

        void UseField(string field, string location, string? output)
        {
            if (lineage.TryGetValue(field, out var dependencies)) _edges.AddRange(dependencies.Select(e => e with { Location = location, OutputColumn = output }));
            else Diagnostic(definition, location, $"Unresolved pipeline field '{field}'.");
        }
        void InspectExpression(string expression, string location, string? output)
        {
            var result = _sql.Analyze(expression, name => Same(name, "pipeline") ? new("pipeline", lineage.Keys.ToArray()) : null, "pipeline", true, ct);
            var refs = new List<DefinitionDependency>();
            foreach (var reference in result.References)
                if (reference.Column is { } field)
                {
                    UseField(field, location, output);
                    if (lineage.TryGetValue(field, out var dependencies)) refs.AddRange(dependencies);
                }
            if (output is not null) lineage[output] = refs;
            foreach (var diagnostic in result.Diagnostics) Diagnostic(definition, location, diagnostic.Message);
        }
    }

    private void AnalyzeModule(DefinitionCatalogRecord definition, JsonElement root)
    {
        string source = definition.MetadataJson is not null ? definition.Source : Text(root, "source") ?? "";
        if (modules is null) { Diagnostic(definition, "source", "C# static inspection is unavailable in this host."); return; }
        var inspection = moduleCache.GetOrAdd(Hash(source), _ => modules.Inspect(source, ct));
        string? ownerId = Text(root, "owner_id");
        var form = catalog.Definitions.FirstOrDefault(d => d.Kind == "Form" && (Same(d.Id, "Form:" + ownerId) || Same(d.Name, ownerId)));
        string? table = form?.OwnerName;
        foreach (var reference in inspection.References)
        {
            string location = $"source, line {reference.Line}";
            var confidence = reference.Confirmed ? DependencyConfidence.Confirmed : DependencyConfidence.Possible;
            switch (reference.Kind)
            {
                case "sql": AnalyzeSql(definition, reference.Text, location, table, confidence: confidence); break;
                case "field":
                    if (table is null) Diagnostic(definition, location, $"Cannot resolve the owning form table for field '{reference.Text}'.");
                    else Reference(definition, table, reference.Text, "Module field", location, confidence: confidence);
                    break;
                case "procedure": Reference(definition, reference.Text, null, "Module call", location, "Procedure", confidence); break;
                case "savedQuery": Reference(definition, reference.Text, null, "Module query", location, "Saved query", confidence); break;
                case "form": Reference(definition, reference.Text, null, "Open form", location, "Form", confidence); break;
            }
        }
        foreach (string diagnostic in inspection.Diagnostics) Diagnostic(definition, "source", diagnostic);
    }
}
