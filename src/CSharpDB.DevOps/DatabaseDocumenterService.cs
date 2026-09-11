using System.Globalization;
using System.Text.Json;
using CSharpDB.Client;
using CSharpDB.Client.Models;

namespace CSharpDB.DevOps;

/// <summary>Builds a data dictionary exclusively from saved catalog metadata.</summary>
public sealed class DatabaseDocumenterService
{
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);
    private static readonly string[] KindOrder = ["Table", "Column", "Foreign key", "Key", "Check", "Default", "Index", "View", "Trigger", "Procedure"];
    public static bool IsDocumentedKind(string kind) => kind is "Table" or "Column" or "Key" or "Check"
        or "Default" or "Foreign key" or "Index" or "View" or "Trigger" or "Procedure";

    public async Task<DatabaseDocument> GenerateAsync(ICSharpDbClient client, string displayName = "Database",
        IProgress<int>? progress = null, CancellationToken ct = default)
        => Build(await DefinitionCatalogService.ReadAsync(client, progress, ct), displayName, ct);

    public DatabaseDocument Build(DefinitionCatalogSnapshot catalog, string displayName = "Database", CancellationToken ct = default)
    {
        var diagnostics = new List<string>();
        if (catalog.DocumentationVersion < 1)
            diagnostics.Add("This server does not expose documentation metadata. Descriptions, procedure parameters, and trigger details may be unavailable. Upgrade the server.");
        // Catalog diagnostics can contain archive paths or connection details. Never export raw errors.
        if (catalog.Diagnostics.Any(d => d.Source != "Documentation"))
            diagnostics.Add("Some saved catalog metadata could not be read. Refresh after resolving the catalog error.");
        foreach (var diagnostic in catalog.Diagnostics.Where(d => d.Source == "Documentation"))
            diagnostics.Add(diagnostic.Message);
        var records = catalog.Definitions.Where(d => IsDocumentedKind(d.Kind)).ToArray();
        var tables = records.Where(d => d.Kind == "Table").ToDictionary(d => d.Name, StringComparer.OrdinalIgnoreCase);
        var columns = records.Where(d => d.Kind == "Column").ToArray();
        DatabaseDocumentLink TableLink(string name) => new(name, tables.GetValueOrDefault(name)?.Id);
        DatabaseDocumentLink ColumnLink(string table, string column) => new(table + "." + column,
            columns.FirstOrDefault(c => string.Equals(c.OwnerName, table, StringComparison.OrdinalIgnoreCase)
                && string.Equals(c.Name, column, StringComparison.OrdinalIgnoreCase))?.Id);
        var relationships = new List<DatabaseDocumentRelationship>();
        foreach (var record in records.Where(d => d.Kind == "Foreign key"))
        {
            ct.ThrowIfCancellationRequested();
            try
            {
                var key = Read<ForeignKeyDefinition>(record.Source);
                var child = key.ColumnNames.Count > 0 ? key.ColumnNames : [key.ColumnName];
                var parent = key.ReferencedColumnNames.Count > 0 ? key.ReferencedColumnNames : [key.ReferencedColumnName];
                if (child.Count != parent.Count || child.Count == 0) throw new JsonException();
                var pairs = child.Select((c, i) => new DatabaseDocumentColumnPair(ColumnLink(record.OwnerName!, c), ColumnLink(key.ReferencedTableName, parent[i]))).ToArray();
                var relationship = new DatabaseDocumentRelationship(record.Id, record.Name, TableLink(record.OwnerName!), TableLink(key.ReferencedTableName), Freeze(pairs), Action(key.OnDelete), Action(key.OnUpdate));
                relationships.Add(relationship);
                if (relationship.ChildTable.ObjectId is null || relationship.ParentTable.ObjectId is null || pairs.Any(p => p.Child.ObjectId is null || p.Parent.ObjectId is null))
                    diagnostics.Add($"Relationship {record.Name} has an unresolved table or column reference.");
            }
            catch (Exception ex) when (ex is JsonException or ArgumentException or InvalidOperationException)
            { diagnostics.Add($"Relationship {record.Name} could not be documented completely."); }
        }
        var entries = new List<DatabaseDocumentEntry>();
        foreach (var record in records.OrderBy(d => Array.IndexOf(KindOrder, d.Kind)).ThenBy(d => d.OwnerName, StringComparer.Ordinal).ThenBy(d => d.Name, StringComparer.Ordinal).ThenBy(d => d.Id, StringComparer.Ordinal))
        {
            ct.ThrowIfCancellationRequested();
            var facts = new List<DatabaseDocumentFact>();
            var documentColumns = new List<DatabaseDocumentColumn>();
            var parameters = new List<DatabaseDocumentParameter>();
            var related = new List<DatabaseDocumentLink>();
            string definition = record.Source, label = "Saved SQL definition";
            var docs = record.Documentation;
            if (docs?.NeedsReview == true) diagnostics.Add($"Description for {record.Kind} {record.Name} needs review because its definition changed.");
            try
            {
                switch (record.Kind)
                {
                    case "Table":
                        var table = Read<TableSchema>(record.Source);
                        definition = SchemaScriptRenderer.RenderCreateTable(table);
                        label = "Generated from current schema metadata";
                        foreach (var column in table.Columns)
                        {
                            var columnRecord = columns.FirstOrDefault(c => c.OwnerName == record.Name && c.Name == column.Name);
                            var attributes = new List<string>();
                            if (column.IsPrimaryKey || table.KeyConstraints.Any(k => k.Kind == KeyConstraintKind.PrimaryKey && k.Columns.Contains(column.Name, StringComparer.OrdinalIgnoreCase))) attributes.Add("Primary key");
                            if (column.IsIdentity) attributes.Add("Identity");
                            if (column.IsRowVersion) attributes.Add("Rowversion");
                            if (column.Collation is not null) attributes.Add("Collation: " + column.Collation);
                            documentColumns.Add(new(ColumnLink(record.Name, column.Name), column.EffectiveType.ToSql(), column.Nullable, string.Join("; ", attributes), column.DefaultSql, Description(columnRecord?.Documentation)));
                        }
                        related.AddRange(records.Where(d => d.OwnerName == record.Name && d.Kind is "Key" or "Check" or "Default" or "Index" or "Trigger" or "Foreign key").Select(d => new DatabaseDocumentLink(d.Kind + ": " + d.Name, d.Id)));
                        break;
                    case "Column":
                        var col = Read<ColumnDefinition>(record.Source);
                        definition = SchemaScriptRenderer.RenderColumn(col);
                        label = "Generated from current schema metadata";
                        facts.Add(new("Declared type", col.EffectiveType.ToSql()));
                        facts.Add(new("Nullable", col.Nullable ? "Yes" : "No"));
                        bool primary = col.IsPrimaryKey;
                        if (record.OwnerName is not null && tables.TryGetValue(record.OwnerName, out var owner))
                            primary |= Read<TableSchema>(owner.Source).KeyConstraints.Any(k => k.Kind == KeyConstraintKind.PrimaryKey && k.Columns.Contains(col.Name, StringComparer.OrdinalIgnoreCase));
                        facts.Add(new("Primary key", primary ? "Yes" : "No"));
                        facts.Add(new("Identity", col.IsIdentity ? "Yes" : "No"));
                        facts.Add(new("Rowversion", col.IsRowVersion ? "Yes" : "No"));
                        if (col.Collation is not null) facts.Add(new("Collation", col.Collation));
                        if (col.DefaultSql is not null) facts.Add(new("Default", col.DefaultSql));
                        break;
                    case "Index":
                        using (var raw = JsonDocument.Parse(record.Source))
                        {
                            if (raw.RootElement.TryGetProperty("kind", out var kind) && kind.TryGetInt32(out int value)
                                && (CSharpDB.Primitives.IndexKind)value is CSharpDB.Primitives.IndexKind.ForeignKeyInternal or CSharpDB.Primitives.IndexKind.ConstraintInternal or CSharpDB.Primitives.IndexKind.FullTextInternal) continue;
                            if (raw.RootElement.TryGetProperty("kind", out kind) && kind.TryGetInt32(out value) && value != 0)
                            {
                                label = "Saved index metadata";
                                facts.Add(new("Index kind", ((CSharpDB.Primitives.IndexKind)value).ToString()));
                                break;
                            }
                        }
                        var index = Read<IndexSchema>(record.Source);
                        definition = SchemaScriptRenderer.RenderCreateIndex(index);
                        label = "Generated from current schema metadata";
                        facts.Add(new("Unique", index.IsUnique ? "Yes" : "No"));
                        related.AddRange(index.Columns.Select(c => ColumnLink(index.TableName, c)));
                        break;
                    case "Key":
                        var key = Read<KeyConstraintDefinition>(record.Source);
                        definition = (key.ConstraintName is null ? "" : "CONSTRAINT " + CSharpDB.Primitives.SqlIdentifierRules.Quote(key.ConstraintName) + " ")
                            + (key.Kind == KeyConstraintKind.PrimaryKey ? "PRIMARY KEY" : "UNIQUE") + " (" + string.Join(", ", key.Columns.Select(CSharpDB.Primitives.SqlIdentifierRules.Quote)) + ")";
                        label = "Generated from current schema metadata";
                        related.AddRange(key.Columns.Select(c => ColumnLink(record.OwnerName!, c)));
                        break;
                    case "Foreign key":
                        var fk = Read<ForeignKeyDefinition>(record.Source);
                        definition = "FOREIGN KEY (" + string.Join(", ", (fk.ColumnNames.Count > 0 ? fk.ColumnNames : [fk.ColumnName]).Select(CSharpDB.Primitives.SqlIdentifierRules.Quote))
                            + ") REFERENCES " + CSharpDB.Primitives.SqlIdentifierRules.Quote(fk.ReferencedTableName) + " ("
                            + string.Join(", ", (fk.ReferencedColumnNames.Count > 0 ? fk.ReferencedColumnNames : [fk.ReferencedColumnName]).Select(CSharpDB.Primitives.SqlIdentifierRules.Quote))
                            + ") ON DELETE " + Action(fk.OnDelete) + " ON UPDATE " + Action(fk.OnUpdate);
                        label = "Generated from current schema metadata";
                        break;
                    case "View":
                        definition = SchemaScriptRenderer.RenderCreateView(new() { Name = record.Name, Sql = record.Source });
                        label = "Saved query with generated CREATE VIEW wrapper";
                        break;
                    case "Trigger" when record.MetadataJson is not null:
                        var trigger = Read<TriggerMetadata>(record.MetadataJson);
                        definition = SchemaScriptRenderer.RenderCreateTrigger(new() { TriggerName = record.Name, TableName = record.OwnerName!, BodySql = record.Source, Timing = trigger.Timing, Event = trigger.Event });
                        label = "Saved body with generated CREATE TRIGGER wrapper";
                        facts.Add(new("Timing", trigger.Timing.ToString()));
                        facts.Add(new("Event", trigger.Event.ToString()));
                        break;
                    case "Procedure":
                        facts.Add(new("Enabled", record.IsEnabled ? "Yes" : "No"));
                        if (record.MetadataJson is not null)
                            parameters.AddRange(Read<ProcedureMetadata>(record.MetadataJson).Parameters.Select(p => new DatabaseDocumentParameter(p.Name, SqlTypeDescriptor.FromLegacy(p.Type).ToSql(), p.Required, Convert.ToString(p.Default, CultureInfo.InvariantCulture), p.Description)));
                        break;
                }
            }
            catch (Exception ex) when (ex is JsonException or ArgumentException or InvalidOperationException or NullReferenceException)
            {
                definition = record.Source;
                label = "Saved definition; some details could not be decoded";
                diagnostics.Add($"Details for {record.Kind} {record.Name} could not be documented completely.");
            }
            if (record.OwnerName is not null) related.Insert(0, TableLink(record.OwnerName));
            var attached = relationships.Where(r => r.Id == record.Id || r.ChildTable.ObjectId == record.Id || r.ParentTable.ObjectId == record.Id
                || r.Columns.Any(p => p.Child.ObjectId == record.Id || p.Parent.ObjectId == record.Id));
            entries.Add(new(record.Id, record.Kind, record.Name, record.OwnerName, definition, label, Description(docs), docs?.Description,
                docs?.Revision ?? 0, docs?.DefinitionFingerprint ?? "", docs?.NeedsReview ?? false, catalog.DocumentationVersion >= 1 && docs is not null,
                Freeze(facts), Freeze(documentColumns), Freeze(parameters), Freeze(related), Freeze(attached)));
        }
        // Internal supporting indexes may have been filtered after their parent table was read.
        var visible = entries.Select(e => e.Id).ToHashSet(StringComparer.Ordinal);
        entries = entries.Select(e => e with { RelatedObjects = Freeze(e.RelatedObjects.Where(l => l.ObjectId is null || visible.Contains(l.ObjectId))) }).ToList();
        return new(SafeDisplayName(displayName), catalog.CapturedUtc, catalog.Version, catalog.DocumentationVersion, Freeze(entries), Freeze(diagnostics.Distinct(StringComparer.Ordinal)));
    }

    public static IReadOnlyList<DatabaseDocumentEntry> Search(DatabaseDocument document, string query = "", string kind = "", bool missingOnly = false, CancellationToken ct = default)
    {
        var result = new List<DatabaseDocumentEntry>();
        foreach (var entry in document.Entries)
        {
            ct.ThrowIfCancellationRequested();
            if (kind.Length > 0 && entry.Kind != kind || missingOnly && !string.IsNullOrWhiteSpace(entry.Description)) continue;
            if (query.Length == 0 || new[] { entry.Name, entry.OwnerName, entry.Description, entry.Definition }.Any(s => s?.Contains(query, StringComparison.OrdinalIgnoreCase) == true)) result.Add(entry);
        }
        return result.AsReadOnly();
    }

    private static string? Description(DefinitionDocumentation? docs) => docs is null ? null : docs.NeedsReview ? docs.NativeDescription : docs.Description ?? docs.NativeDescription;
    private static T Read<T>(string json) => JsonSerializer.Deserialize<T>(json, Json) ?? throw new JsonException();
    private static IReadOnlyList<T> Freeze<T>(IEnumerable<T> values) => Array.AsReadOnly(values.ToArray());
    private static string Action(ForeignKeyOnDeleteAction action) => action switch { ForeignKeyOnDeleteAction.NoAction => "NO ACTION", ForeignKeyOnDeleteAction.SetNull => "SET NULL", ForeignKeyOnDeleteAction.SetDefault => "SET DEFAULT", _ => action.ToString().ToUpperInvariant() };
    public static string SafeDisplayName(string name) => string.IsNullOrWhiteSpace(name) || name.IndexOfAny(['/', '\\', ':', '=', ';', '\r', '\n']) >= 0 ? "Database" : name;
    private sealed record TriggerMetadata(TriggerTiming Timing, TriggerEvent Event);
    private sealed record ProcedureMetadata(ProcedureParameterDefinition[] Parameters);
}
