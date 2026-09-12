using System.Text.Json;
using CSharpDB.Client.Models;
using CSharpDB.Sql;

namespace CSharpDB.DevOps;

internal sealed partial class DefinitionAnalysisBuilder
{
    private readonly Dictionary<Guid, List<DefinitionCatalogRecord>> _modelSchemaIdentities = [];
    private readonly Dictionary<string, DefinitionCatalogRecord> _modelDefinitions = new(StringComparer.Ordinal);
    private readonly Dictionary<string, Dictionary<string, DefinitionCatalogRecord>> _modelChanges = new(StringComparer.Ordinal);
    private static readonly string[] ModelOperationKinds = ["CreateTable", "DropTable", "RenameTable", "AddColumn", "DropColumn", "RenameColumn",
        "AddForeignKey", "DropForeignKey", "AlterColumnType", "SetDefault", "DropDefault", "SetNotNull", "DropNotNull", "SetCollation", "DropCollation",
        "AddPrimaryKey", "AddUniqueKey", "AddCheck", "DropConstraint", "DropPrimaryKey", "CreateIndex", "DropIndex"];

    private void IndexDataModelDefinitions()
    {
        foreach (var definition in catalog.Definitions)
        {
            ct.ThrowIfCancellationRequested();
            if (definition.Format == "dataModel") _modelDefinitions[definition.Id] = definition;
            if (definition.Kind == "Table" && _tableSchemas.TryGetValue(definition.Id, out var schema) && schema.SchemaId != Guid.Empty)
            {
                if (!_modelSchemaIdentities.TryGetValue(schema.SchemaId, out var matches)) _modelSchemaIdentities[schema.SchemaId] = matches = [];
                matches.Add(definition);
            }
            if (definition.Format != "modelChange" || ModelOwnerId(definition) is not { } owner) continue;
            if (!_modelChanges.TryGetValue(owner, out var changes)) _modelChanges[owner] = changes = new(StringComparer.Ordinal);
            changes.TryAdd(OperationId(definition), definition);
        }
    }

    private sealed class ModelTable(DefinitionCatalogRecord target, bool proposed = false)
    {
        public DefinitionCatalogRecord Target { get; } = target;
        public bool Proposed { get; } = proposed;
        // A null value denotes a proposed column that does not exist in the active schema.
        public Dictionary<string, string?> Columns { get; } = new(StringComparer.OrdinalIgnoreCase);
        public HashSet<string> UnresolvedColumns { get; } = new(StringComparer.OrdinalIgnoreCase);
    }

    private static Guid Identity(JsonElement element) => Guid.TryParse(Text(element, "schemaId"), out var id) ? id : Guid.Empty;
    private static bool Flag(JsonElement element, string name) => Property(element, name).ValueKind == JsonValueKind.True;
    private static bool EnumIs(JsonElement element, string property, string label, int value)
        => Same(Text(element, property), label) || Text(element, property) == value.ToString(System.Globalization.CultureInfo.InvariantCulture);

    private void AnalyzeDataModel(DefinitionCatalogRecord definition, JsonElement root)
    {
        if (Property(root, "nodes").ValueKind != JsonValueKind.Array)
        { Diagnostic(definition, "$.nodes", "Saved data model has no valid node collection."); return; }
        var tables = new Dictionary<string, ModelTable?>(StringComparer.OrdinalIgnoreCase);
        var changes = _modelChanges.GetValueOrDefault(definition.Id) ?? new(StringComparer.Ordinal);
        ValidateModelArray(definition, root, "pendingOperations", "$");
        ValidateModelArray(definition, root, "relationships", "$");
        var operations = Items(Property(root, "pendingOperations")).ToArray();
        var operationKeys = new string[operations.Length];
        var seenOperationIds = new HashSet<string>(StringComparer.Ordinal);
        var createOperations = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        var proposedColumns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < operations.Length; i++)
        {
            ct.ThrowIfCancellationRequested();
            string? id = Text(operations[i], "id");
            string key = string.IsNullOrWhiteSpace(id) ? "index-" + i.ToString(System.Globalization.CultureInfo.InvariantCulture) : id;
            operationKeys[i] = seenOperationIds.Add(key) ? key : key + ":duplicate-" + i.ToString(System.Globalization.CultureInfo.InvariantCulture);
            string? tableName = Text(operations[i], "tableName");
            if (tableName is null) continue;
            if (EnumIs(operations[i], "kind", "CreateTable", 0)) createOperations.TryAdd(tableName, i);
            if (EnumIs(operations[i], "kind", "AddColumn", 3) && Text(operations[i], "columnName") is { } field)
                proposedColumns.Add(tableName + "\0" + field);
        }
        int nodeIndex = 0;
        foreach (var node in Items(Property(root, "nodes")))
        {
            ct.ThrowIfCancellationRequested();
            string path = $"$.nodes[{nodeIndex++}]";
            string? name = Text(node, "name");
            if (string.IsNullOrWhiteSpace(name)) { Diagnostic(definition, path, "Saved model member has no name."); continue; }
            ValidateModelArray(definition, node, "columns", path);
            if (Flag(node, "isDraft"))
            {
                // Create-table operations provide graph nodes for drafts without inventing live database objects.
                if (createOperations.TryGetValue(name, out int createIndex) && changes.TryGetValue(operationKeys[createIndex], out var draft))
                {
                    var binding = new ModelTable(draft, true);
                    foreach (var column in Items(Property(node, "columns")))
                        if (Text(column, "name") is { } field) binding.Columns[field] = null;
                    tables[name] = binding;
                    ModelEdge(definition, binding, null, "Proposed table in model", path, DependencyRelationshipKind.ProposedChange);
                }
                else { tables[name] = null; Diagnostic(definition, path, "Draft table has no saved create-table operation."); }
                continue;
            }
            bool external = EnumIs(node, "kind", "ExternalTable", 1);
            Guid schemaId = Identity(node);
            DefinitionCatalogRecord[] candidates = schemaId != Guid.Empty && !external ? [.. _modelSchemaIdentities.GetValueOrDefault(schemaId) ?? []]
                : _relations.TryGetValue(name, out var named) && named.Kind == (external ? "External table" : "Table") ? [named] : [];
            if (candidates.Length != 1)
            { tables[name] = null; Diagnostic(definition, path, $"Saved model member '{name}' is missing or ambiguous in the active catalog."); continue; }
            var target = candidates[0];
            var member = new ModelTable(target);
            tables[name] = member;
            // Stable identities keep old diagram labels attached to the renamed persisted object.
            tables.TryAdd(target.Name, member);
            var relationship = external ? DependencyRelationshipKind.ExternalArchive : DependencyRelationshipKind.ModelMembership;
            ModelEdge(definition, member, null, external ? "Archive model membership" : "Model membership", path + ".name", relationship);
            int columnIndex = 0;
            foreach (var column in Items(Property(node, "columns")))
            {
                ct.ThrowIfCancellationRequested();
                string columnPath = $"{path}.columns[{columnIndex++}].name";
                string? field = Text(column, "name");
                if (string.IsNullOrWhiteSpace(field)) continue;
                Guid columnId = Identity(column);
                var saved = _tableSchemas.GetValueOrDefault(target.Id)?.Columns.FirstOrDefault(c =>
                    columnId != Guid.Empty && !external ? c.SchemaId == columnId : Same(c.Name, field));
                if (saved is not null)
                {
                    member.Columns[field] = saved.Name;
                    member.Columns.TryAdd(saved.Name, saved.Name);
                    ModelEdge(definition, member, saved.Name, "Model column membership", columnPath, relationship);
                }
                else if (proposedColumns.Contains(name + "\0" + field))
                {
                    member.Columns[field] = null;
                    ModelEdge(definition, member, field, "Proposed model column", columnPath, DependencyRelationshipKind.ProposedChange);
                }
                else
                {
                    member.UnresolvedColumns.Add(field);
                    Diagnostic(definition, columnPath, $"Saved model column '{name}.{field}' is missing in the active catalog.");
                }
            }
            // Relationships can reference a column hidden by a saved diagram's detail settings.
            foreach (var column in _tableSchemas.GetValueOrDefault(target.Id)?.Columns ?? []) member.Columns.TryAdd(column.Name, column.Name);
        }

        int operationIndex = 0;
        foreach (var operation in operations)
        {
            ct.ThrowIfCancellationRequested();
            string operationId = operationKeys[operationIndex];
            string path = $"$.pendingOperations[{operationIndex++}]";
            if (!changes.TryGetValue(operationId, out var change))
            { Diagnostic(definition, path, "Saved operation has no corresponding catalog record."); continue; }
            _edges.Add(new(definition.Id, change.Id, null, null, "Saved proposed change", path, Relationship: DependencyRelationshipKind.ProposedChange));
            AnalyzeProposedOperation(change, operation, tables);
            _analyzed.Add(change.Id);
        }
        int relationshipIndex = 0;
        foreach (var relationship in Items(Property(root, "relationships")))
        {
            ct.ThrowIfCancellationRequested();
            string path = $"$.relationships[{relationshipIndex++}]";
            var category = EnumIs(relationship, "kind", "Draft", 2) ? DependencyRelationshipKind.ProposedChange
                : EnumIs(relationship, "kind", "ExternalArchiveForeignKey", 1) ? DependencyRelationshipKind.ExternalArchive
                : DependencyRelationshipKind.ModelMembership;
            string usage = category == DependencyRelationshipKind.ProposedChange ? "Proposed relationship" : "Model relationship";
            var pairs = Items(Property(relationship, "columnPairs")).ToArray();
            if (pairs.Length == 0)
            {
                ModelReference(definition, tables, Text(relationship, "leftTable"), Text(relationship, "leftColumn"), usage, path + ".leftColumn", category);
                ModelReference(definition, tables, Text(relationship, "rightTable"), Text(relationship, "rightColumn"), usage, path + ".rightColumn", category);
            }
            else for (int i = 0; i < pairs.Length; i++)
            {
                ModelReference(definition, tables, Text(relationship, "leftTable"), Text(pairs[i], "childColumn"), usage, $"{path}.columnPairs[{i}].childColumn", category);
                ModelReference(definition, tables, Text(relationship, "rightTable"), Text(pairs[i], "parentColumn"), usage, $"{path}.columnPairs[{i}].parentColumn", category);
            }
        }
    }

    private static string? ModelOwnerId(DefinitionCatalogRecord definition)
    {
        if (definition.MetadataJson is null) return null;
        try
        {
            using var metadata = JsonDocument.Parse(definition.MetadataJson);
            string? id = Text(metadata.RootElement, "modelId");
            return id is null || id.StartsWith("DataModel:", StringComparison.Ordinal) ? id : "DataModel:" + id;
        }
        catch (JsonException) { return null; }
    }

    private static string OperationId(DefinitionCatalogRecord definition)
    {
        // The catalog also supplies an ordinal when the saved operation has no ID.
        string owner = ModelOwnerId(definition)?.Replace("DataModel:", "ModelChange:", StringComparison.Ordinal) + ":";
        return definition.Id.StartsWith(owner, StringComparison.Ordinal) ? definition.Id[owner.Length..] : definition.Id;
    }

    private void AnalyzeModelChange(DefinitionCatalogRecord definition)
    {
        string? parentId = ModelOwnerId(definition);
        var parent = parentId is null ? null : _modelDefinitions.GetValueOrDefault(parentId);
        if (parent is null) Diagnostic(definition, "metadata.modelId", "The saved data model for this proposed change is unavailable.");
        else Analyze(parent);
    }

    private ModelTable? ModelBinding(Dictionary<string, ModelTable?> tables, string? name)
    {
        if (string.IsNullOrWhiteSpace(name)) return null;
        if (tables.TryGetValue(name, out var member)) return member;
        if (!_relations.TryGetValue(name, out var target) || target.Kind is not ("Table" or "External table")) return null;
        member = new ModelTable(target);
        foreach (string column in _columns.GetValueOrDefault(target.Id, [])) member.Columns[column] = column;
        return tables[name] = member;
    }

    private void ModelReference(DefinitionCatalogRecord source, Dictionary<string, ModelTable?> tables, string? table,
        string? column, string usage, string location, DependencyRelationshipKind relationship)
    {
        ct.ThrowIfCancellationRequested();
        if (relationship == DependencyRelationshipKind.ExternalArchive && (table is null || !tables.ContainsKey(table)))
        { Diagnostic(source, location, $"Archive endpoint '{table}' is not explicitly bound in the saved model."); return; }
        var binding = ModelBinding(tables, table);
        if (binding is null) { Diagnostic(source, location, $"Unresolved saved model source '{table}'."); return; }
        string? targetColumn = column;
        if (!string.IsNullOrWhiteSpace(column))
        {
            if (binding.UnresolvedColumns.Contains(column) || !binding.Columns.TryGetValue(column, out string? saved))
            { Diagnostic(source, location, $"Unresolved saved model field '{table}.{column}'."); return; }
            targetColumn = saved ?? column;
            if (saved is null) relationship = DependencyRelationshipKind.ProposedChange;
        }
        if (binding.Proposed) relationship = DependencyRelationshipKind.ProposedChange;
        else if (binding.Target.Kind == "External table" && relationship == DependencyRelationshipKind.ModelMembership) relationship = DependencyRelationshipKind.ExternalArchive;
        ModelEdge(source, binding, targetColumn, usage, location, relationship);
    }

    private void ModelEdge(DefinitionCatalogRecord source, ModelTable target, string? column, string usage, string location, DependencyRelationshipKind relationship)
        => _edges.Add(new(source.Id, target.Target.Id, column, null, usage, location, Relationship: relationship));

    private void AnalyzeProposedOperation(DefinitionCatalogRecord definition, JsonElement operation, Dictionary<string, ModelTable?> tables)
    {
        const DependencyRelationshipKind category = DependencyRelationshipKind.ProposedChange;
        string? table = Text(operation, "tableName"), column = Text(operation, "columnName");
        bool Is(string label, int value) => EnumIs(operation, "kind", label, value);
        string? kind = ModelOperationKinds.FirstOrDefault((label) => Is(label, Array.IndexOf(ModelOperationKinds, label)));
        if (kind is null) { Diagnostic(definition, "$.kind", "The proposed operation kind is missing or unsupported."); return; }
        foreach (string property in new[] { "columnNames", "referencedColumnNames", "columns" }) ValidateModelArray(definition, operation, property, "$");
        string usage = "Proposed " + kind;
        if (Is("CreateTable", 0))
        {
            if (string.IsNullOrWhiteSpace(table)) { Diagnostic(definition, "$.tableName", "Proposed table name is missing."); return; }
            var draft = new ModelTable(definition, true);
            foreach (var item in Items(Property(operation, "columns"))) if (Text(item, "name") is { } field) draft.Columns[field] = null;
            if (draft.Columns.Count == 0) draft.Columns["Id"] = null;
            tables[table] = draft;
            AnalyzeModelColumnExpressions(definition, operation, tables, table);
            return;
        }
        var binding = ModelBinding(tables, table);
        if (binding is null) { Diagnostic(definition, "$.tableName", $"Unresolved proposed table '{table}'."); return; }
        ModelReference(definition, tables, table, null, usage, "$.tableName", category);
        if (Is("AddColumn", 3) && column is not null)
        {
            binding.Columns.TryAdd(column, null);
            ModelEdge(definition, binding, column, "Proposed column", "$.columnName", category);
        }
        else if (!string.IsNullOrWhiteSpace(column)) ModelReference(definition, tables, table, column, usage, "$.columnName", category);
        int index = 0;
        foreach (string field in Strings(Property(operation, "columnNames")))
            ModelReference(definition, tables, table, field, usage, $"$.columnNames[{index++}]", category);
        string? referenced = Text(operation, "referencedTableName");
        if (!string.IsNullOrWhiteSpace(referenced))
        {
            ModelReference(definition, tables, referenced, null, "Proposed referenced table", "$.referencedTableName", category);
            var fields = Strings(Property(operation, "referencedColumnNames")).ToArray();
            if (fields.Length == 0 && Text(operation, "referencedColumnName") is { } single) fields = [single];
            for (int i = 0; i < fields.Length; i++) ModelReference(definition, tables, referenced, fields[i], "Proposed referenced column", $"$.referencedColumnNames[{i}]", category);
        }
        foreach (string property in new[] { "expressionSql", "checkExpressionSql" })
            if (Text(operation, property) is { Length: > 0 } expression) AnalyzeModelExpression(definition, expression, tables, table!, "$." + property);
        AnalyzeModelColumnExpressions(definition, operation, tables, table!);
        // Removal operations store constraint/index names instead of column lists.
        if (Is("DropForeignKey", 7) || Is("DropConstraint", 18) || Is("DropPrimaryKey", 19) || Is("DropIndex", 21))
        {
            string? name = Text(operation, "constraintName") ?? Text(operation, "indexName");
            foreach (var constraint in catalog.Definitions.Where(d => Same(d.OwnerName, binding.Target.Name)
                && d.Kind is "Foreign key" or "Key" or "Check" or "Index"
                && (Same(d.Name, name) || Is("DropPrimaryKey", 19) && d.Kind == "Key" && IsPrimaryKey(d))))
            {
                Analyze(constraint);
                foreach (var edge in _edges.Where(e => e.SourceId == constraint.Id && e.Relationship == DependencyRelationshipKind.Usage).ToArray())
                    _edges.Add(edge with { SourceId = definition.Id, Usage = usage, Location = "$.constraintName", Relationship = category });
            }
        }
        if (Is("RenameTable", 2) && Text(operation, "newTableName") is { Length: > 0 } renamedTable) tables[renamedTable] = binding;
        if (Is("RenameColumn", 5) && column is not null && Text(operation, "newColumnName") is { Length: > 0 } renamedColumn
            && binding.Columns.TryGetValue(column, out var original)) binding.Columns[renamedColumn] = original;
    }

    private static bool IsPrimaryKey(DefinitionCatalogRecord definition)
    {
        try { using var data = JsonDocument.Parse(definition.Source); return EnumIs(data.RootElement, "kind", "PrimaryKey", 0); }
        catch (JsonException) { return false; }
    }

    private void ValidateModelArray(DefinitionCatalogRecord definition, JsonElement value, string property, string path)
    {
        if (Property(value, property).ValueKind is not (JsonValueKind.Undefined or JsonValueKind.Array))
            Diagnostic(definition, path + "." + property, "Saved model collection is malformed; entries could not be inspected.");
    }

    private void AnalyzeModelColumnExpressions(DefinitionCatalogRecord definition, JsonElement operation, Dictionary<string, ModelTable?> tables, string table)
    {
        int index = 0;
        foreach (var column in Items(Property(operation, "columns")))
        {
            string path = $"$.columns[{index++}]";
            if (Text(column, "defaultSql") is { Length: > 0 } expression) AnalyzeModelExpression(definition, expression, tables, table, path + ".defaultSql");
            int checkIndex = 0;
            foreach (var check in Items(Property(column, "checks")))
                if (Text(check, "expressionSql") is { Length: > 0 } checkSql) AnalyzeModelExpression(definition, checkSql, tables, table, $"{path}.checks[{checkIndex++}].expressionSql");
        }
    }

    private void AnalyzeModelExpression(DefinitionCatalogRecord definition, string expression, Dictionary<string, ModelTable?> tables, string table, string path)
    {
        var result = _sql.Analyze(expression, name => ModelBinding(tables, name) is { } target ? new(name, target.Columns.Keys.ToArray()) : null, table, true, ct);
        foreach (var reference in result.References)
            ModelReference(definition, tables, reference.Relation, reference.Column, "Proposed expression", path, DependencyRelationshipKind.ProposedChange);
        foreach (var diagnostic in result.Diagnostics) Diagnostic(definition, path, diagnostic.Message);
    }

    private void AnalyzeArchiveRelationship(DefinitionCatalogRecord definition, JsonElement root)
    {
        const DependencyRelationshipKind category = DependencyRelationshipKind.ExternalArchive;
        foreach (string column in Strings(Property(root, "columnNames")).DefaultIfEmpty(Text(root, "columnName") ?? ""))
            Reference(definition, definition.OwnerName, column, "Archive foreign key", "$.columnNames", "External table", relationship: category);
        if (definition.MetadataJson is null)
        { Diagnostic(definition, "$.referencedTableName", "Archive relationship source metadata is unavailable."); return; }
        using var metadata = JsonDocument.Parse(definition.MetadataJson);
        var candidates = Strings(Property(metadata.RootElement, "referencedTableCandidates")).Distinct(StringComparer.OrdinalIgnoreCase).ToArray();
        if (candidates.Length != 1)
        { Diagnostic(definition, "$.referencedTableName", $"Archive target '{Text(root, "referencedTableName")}' has {candidates.Length} matching external registrations; its reference was not bound to a local table."); return; }
        foreach (string column in Strings(Property(root, "referencedColumnNames")).DefaultIfEmpty(Text(root, "referencedColumnName") ?? ""))
            Reference(definition, candidates[0], column, "Archive referenced key", "$.referencedColumnNames", "External table", relationship: category);
    }
}
