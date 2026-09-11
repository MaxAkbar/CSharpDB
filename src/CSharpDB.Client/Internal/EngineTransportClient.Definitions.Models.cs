using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;
using CSharpDB.Client.Models;
using CSharpDB.ImportExport.TableArchives;

namespace CSharpDB.Client.Internal;

internal sealed partial class EngineTransportClient
{
    private delegate void DefinitionRecordWriter(string kind, string name, string source, string format = "sql",
        string? owner = null, string? identity = null, bool enabled = true, string? metadata = null);

    private static void AddDataModelDefinition(string id, string name, string source, DefinitionRecordWriter add,
        List<DefinitionCatalogDiagnostic> diagnostics, CancellationToken ct)
    {
        string modelId = "DataModel:" + id;
        string metadata = JsonSerializer.Serialize(new { modelId, modelName = name }, DefinitionJson);
        add("Data model", name, source, "dataModel", identity: modelId, metadata: metadata);
        try
        {
            using var document = JsonDocument.Parse(source);
            if (document.RootElement.ValueKind != JsonValueKind.Object)
                throw new JsonException("The saved data model must be a JSON object.");
            if (!TryModelProperty(document.RootElement, "pendingOperations", out var operations)) return;
            if (operations.ValueKind != JsonValueKind.Array)
                throw new JsonException("The saved data model pendingOperations must be an array.");
            var ids = new HashSet<string>(StringComparer.Ordinal);
            int index = 0;
            foreach (var operation in operations.EnumerateArray())
            {
                ct.ThrowIfCancellationRequested();
                int ordinal = index++;
                if (operation.ValueKind != JsonValueKind.Object)
                {
                    diagnostics.Add(new(name, $"Proposed change {ordinal + 1} is not a JSON object."));
                    continue;
                }
                string operationId = ModelText(operation, "id");
                if (string.IsNullOrWhiteSpace(operationId)) operationId = "index-" + ordinal.ToString(CultureInfo.InvariantCulture);
                if (!ids.Add(operationId))
                {
                    diagnostics.Add(new(name, $"Proposed change {ordinal + 1} has a duplicate identity."));
                    string originalId = operationId;
                    int suffix = ordinal;
                    do { operationId = originalId + ":duplicate-" + (suffix++).ToString(CultureInfo.InvariantCulture); }
                    while (!ids.Add(operationId));
                }
                string description = ModelText(operation, "description");
                if (string.IsNullOrWhiteSpace(description))
                {
                    string table = ModelText(operation, "tableName"), column = ModelText(operation, "columnName");
                    description = string.IsNullOrWhiteSpace(table) ? $"Change {ordinal + 1}" : table + (column.Length == 0 ? "" : "." + column);
                }
                add("Proposed change", description, operation.GetRawText(), "modelChange", name,
                    "ModelChange:" + id + ":" + operationId, metadata: metadata);
            }
        }
        catch (JsonException ex) { diagnostics.Add(new(name, "Could not inspect the saved data model: " + ex.Message)); }
    }

    private static bool TryModelProperty(JsonElement value, string name, out JsonElement property)
    {
        foreach (var candidate in value.EnumerateObject())
            if (string.Equals(candidate.Name, name, StringComparison.OrdinalIgnoreCase))
            { property = candidate.Value; return true; }
        property = default;
        return false;
    }

    private static string ModelText(JsonElement value, string name) =>
        TryModelProperty(value, name, out var property) && property.ValueKind == JsonValueKind.String ? property.GetString() ?? "" : "";

    private async Task AddExternalDefinitionsAsync(List<Dictionary<string, string?>> rows, DefinitionRecordWriter add,
        List<DefinitionCatalogDiagnostic> diagnostics, CancellationToken ct)
    {
        var registrations = rows.Select(row => new
        {
            Name = row.GetValueOrDefault("name") ?? "",
            Path = row.GetValueOrDefault("path") ?? "",
            SourceName = row.GetValueOrDefault("source_table_name") ?? "",
        }).ToArray();
        foreach (var registration in registrations)
        {
            ct.ThrowIfCancellationRequested();
            if (string.IsNullOrWhiteSpace(registration.Name))
            {
                diagnostics.Add(new("__external_tables", "An external table registration has no name."));
                continue;
            }
            string sourceName = registration.SourceName;
            string? schemaJson = null;
            var relationships = new List<(string Name, string Source, string ReferencedSource)>();
            try
            {
                ArgumentException.ThrowIfNullOrWhiteSpace(registration.Path);
                string databaseFolder = Path.GetDirectoryName(_databasePath) ?? "";
                string resolvedPath = Path.GetFullPath(Path.IsPathRooted(registration.Path)
                    ? registration.Path : Path.Combine(databaseFolder, registration.Path));
                var (archive, _) = await TableArchiveReader.ReadDefinitionMetadataAsync(resolvedPath, ct);
                // Archive schema IDs identify objects in the exporting database, never this registration.
                // Preserve source names on references and resolve them explicitly from registration metadata.
                var schema = JsonSerializer.SerializeToNode(MapTableSchema(archive.ToTableSchema()), DefinitionJson)!.AsObject();
                RemoveArchiveSchemaIdentities(schema);
                schema["tableName"] = registration.Name;
                schemaJson = schema.ToJsonString(DefinitionJson);
                if (!string.IsNullOrWhiteSpace(sourceName) && !string.Equals(sourceName, archive.TableName, StringComparison.OrdinalIgnoreCase))
                    diagnostics.Add(new(registration.Name, "The archive source table name differs from its saved registration. Review the external table registration."));
                sourceName = archive.TableName;
                if (schema["foreignKeys"] is JsonArray keys)
                    foreach (var key in keys.OfType<JsonObject>())
                    {
                        ct.ThrowIfCancellationRequested();
                        relationships.Add((key["constraintName"]?.GetValue<string>() ?? "Archive relationship", key.ToJsonString(DefinitionJson),
                            key["referencedTableName"]?.GetValue<string>() ?? ""));
                    }
            }
            catch (OperationCanceledException) { throw; }
            catch (Exception ex) when (ex is IOException or InvalidDataException or UnauthorizedAccessException or JsonException or FormatException or ArgumentException or NotSupportedException or InvalidOperationException)
            {
                diagnostics.Add(new(registration.Name, $"Could not inspect archive metadata from '{registration.Path}': {ex.Message}"));
            }
            string metadata = JsonSerializer.Serialize(new
            {
                archivePath = registration.Path, sourceTableName = sourceName,
                metadataAvailable = schemaJson is not null, dataPayloadIntegrityVerified = false,
            }, DefinitionJson);
            add("External table", registration.Name, schemaJson ?? JsonSerializer.Serialize(new TableSchema
            { TableName = registration.Name, Columns = [] }, DefinitionJson), "externalTable",
                identity: "ExternalTable:" + registration.Name, metadata: metadata);
            for (int i = 0; i < relationships.Count; i++)
            {
                var relationship = relationships[i];
                string[] candidates = string.Equals(relationship.ReferencedSource, sourceName, StringComparison.OrdinalIgnoreCase)
                    ? [registration.Name]
                    : registrations.Where(r => string.Equals(r.SourceName, relationship.ReferencedSource, StringComparison.OrdinalIgnoreCase))
                        .Select(r => r.Name).Distinct(StringComparer.OrdinalIgnoreCase).Order(StringComparer.OrdinalIgnoreCase).ToArray();
                add("Archive relationship", relationship.Name, relationship.Source, "archiveForeignKey", registration.Name,
                    "ArchiveForeignKey:" + registration.Name + ":" + i.ToString(CultureInfo.InvariantCulture) + ":" + relationship.Name,
                    metadata: JsonSerializer.Serialize(new
                    {
                        archivePath = registration.Path, sourceTableName = sourceName,
                        referencedSourceTableName = relationship.ReferencedSource, referencedTableCandidates = candidates,
                        dataPayloadIntegrityVerified = false,
                    }, DefinitionJson));
            }
        }
    }

    private static void RemoveArchiveSchemaIdentities(JsonNode? node)
    {
        if (node is JsonObject obj)
        {
            foreach (string key in new[] { "schemaId", "columnSchemaIds", "referencedTableSchemaId", "referencedColumnSchemaIds", "referencedKeySchemaId" })
                obj.Remove(key);
            foreach (var property in obj) RemoveArchiveSchemaIdentities(property.Value);
        }
        else if (node is JsonArray array)
            foreach (var value in array) RemoveArchiveSchemaIdentities(value);
    }
}
