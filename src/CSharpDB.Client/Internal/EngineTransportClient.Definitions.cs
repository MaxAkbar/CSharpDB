using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using CSharpDB.Client.Models;
using CSharpDB.Engine;
using CSharpDB.Primitives;

namespace CSharpDB.Client.Internal;

internal sealed partial class EngineTransportClient : ICSharpDbDefinitionCatalogReader
{
    private static readonly JsonSerializerOptions DefinitionJson = new(JsonSerializerDefaults.Web);
    private sealed record DefinitionReadSnapshot(string Version, DateTimeOffset CapturedUtc, DefinitionCatalogRecord[] Records, DefinitionCatalogDiagnostic[] Diagnostics);
    private readonly Dictionary<string, (DefinitionReadSnapshot Snapshot, DateTimeOffset LastRead)> _definitionReads = [];

    public async Task<DefinitionCatalogPage> ReadDefinitionCatalogAsync(
        string? continuationToken = null, int pageSize = 64, CancellationToken ct = default)
    {
        if (pageSize is < 1 or > 64) throw new ArgumentOutOfRangeException(nameof(pageSize));
        using ClientLockLease lease = await AcquireClientLockAsync(ct);
        var now = DateTimeOffset.UtcNow;
        foreach (string key in _definitionReads.Where(p => now - p.Value.LastRead > TimeSpan.FromMinutes(2)).Select(p => p.Key).ToArray()) _definitionReads.Remove(key);
        if (continuationToken is not null)
        {
            string[] parts = continuationToken.Split(':');
            if (parts.Length != 2 || !int.TryParse(parts[1], NumberStyles.None, CultureInfo.InvariantCulture, out int offset) || offset < 0)
                throw new ArgumentException("Invalid definition catalog continuation token.", nameof(continuationToken));
            if (!_definitionReads.TryGetValue(parts[0], out var read))
                throw new InvalidOperationException("The catalog snapshot expired or belongs to another database or route. Refresh the catalog.");
            if (offset >= read.Snapshot.Records.Length) throw new ArgumentException("Invalid definition catalog offset.", nameof(continuationToken));
            _definitionReads[parts[0]] = (read.Snapshot, now);
            return DefinitionPage(read.Snapshot, parts[0], offset, pageSize);
        }
        var db = await GetDatabaseAsync(ct);
        using var reader = db.CreateReaderSession();
        var records = new List<DefinitionCatalogRecord>();
        var stableIdentities = new HashSet<string>(StringComparer.Ordinal);
        var diagnostics = new List<DefinitionCatalogDiagnostic>();
        var names = db.GetTableNames().ToHashSet(StringComparer.OrdinalIgnoreCase);

        string? SchemaIdentity(string kind, Guid schemaId)
        {
            if (schemaId == Guid.Empty) return null;
            string identity = $"{kind}:{schemaId:N}";
            stableIdentities.Add(identity);
            return identity;
        }

        void Add(string kind, string name, string source, string format = "sql", string? owner = null,
            string? identity = null, bool enabled = true, string? metadata = null)
        {
            ct.ThrowIfCancellationRequested();
            string id = identity ?? $"{kind}:{IdentityPart(owner)}:{IdentityPart(name)}";
            string hash = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(source)));
            // Bound even escaped JSON pages below the standard gRPC receive limit.
            const int fragmentLength = 8192;
            var fragments = new List<string>();
            for (int start = 0; start < source.Length;)
            {
                int length = Math.Min(fragmentLength, source.Length - start);
                if (start + length < source.Length && char.IsHighSurrogate(source[start + length - 1])) length--;
                fragments.Add(source.Substring(start, length)); start += length;
            }
            if (fragments.Count == 0) fragments.Add("");
            for (int part = 0; part < fragments.Count; part++)
            {
                records.Add(new()
                {
                    Id = id, Kind = kind, Name = name, OwnerName = owner, Format = format,
                    Source = fragments[part], MetadataJson = metadata,
                    SourceHash = hash, IsEnabled = enabled, PartIndex = part, PartCount = fragments.Count,
                });
            }
        }

        foreach (string name in names.Order(StringComparer.OrdinalIgnoreCase))
        {
            ct.ThrowIfCancellationRequested();
            if (DbInternalTableRegistry.TryGet(name, out _)) continue;
            var schema = db.GetTableSchema(name);
            if (schema is null) continue;
            var mapped = MapTableSchema(schema);
            Add("Table", name, JsonSerializer.Serialize(mapped, DefinitionJson), "table", identity: SchemaIdentity("Table", schema.SchemaId));
            foreach (var column in mapped.Columns)
            {
                Add("Column", column.Name, JsonSerializer.Serialize(column, DefinitionJson), "column", name,
                    SchemaIdentity("Column", column.SchemaId));
                if (!string.IsNullOrWhiteSpace(column.DefaultSql)) Add("Default", column.Name, column.DefaultSql, "expression", name);
            }
            foreach (var check in mapped.CheckConstraints)
                Add("Check", check.ConstraintName ?? check.ColumnName ?? "CHECK", check.ExpressionSql, "expression", name,
                    SchemaIdentity("Check", check.SchemaId), metadata: JsonSerializer.Serialize(new { check.ColumnName }, DefinitionJson));
            foreach (var key in mapped.KeyConstraints)
                Add("Key", key.ConstraintName ?? key.Kind.ToString(), JsonSerializer.Serialize(key, DefinitionJson), "key", name,
                    SchemaIdentity("Key", key.SchemaId));
            foreach (var key in mapped.ForeignKeys)
                Add("Foreign key", key.ConstraintName, JsonSerializer.Serialize(key, DefinitionJson), "foreignKey", name,
                    SchemaIdentity("ForeignKey", key.SchemaId));
        }
        foreach (var index in db.GetIndexes())
            if (!DbInternalTableRegistry.TryGet(index.TableName, out _))
                Add("Index", index.IndexName, JsonSerializer.Serialize(index, DefinitionJson), "index", index.TableName);
        foreach (string name in db.GetViewNames()) Add("View", name, db.GetViewSql(name) ?? "");
        foreach (var trigger in db.GetTriggers())
            if (!DbInternalTableRegistry.IsInternalTable(trigger.TableName))
                Add("Trigger", trigger.TriggerName, trigger.BodySql, owner: trigger.TableName,
                    metadata: JsonSerializer.Serialize(new { trigger.Timing, trigger.Event }, DefinitionJson));

        async Task<List<Dictionary<string, string?>>> Rows(string table, string? query = null)
        {
            if (!names.Contains(table)) return [];
            await using var result = await reader.ExecuteReadAsync(query ?? $"SELECT * FROM {SqlIdentifierRules.Quote(table)}", ct);
            var rows = await result.ToListAsync(ct);
            return rows.Select(row => result.Schema.Select((column, i) => (column: column.Name, value: row[i].IsNull ? null : row[i].ToString()))
                .ToDictionary(x => x.column, x => x.value, StringComparer.OrdinalIgnoreCase)).ToList();
        }
        async Task Read(string table, Func<Task> action)
        {
            try { await action(); }
            catch (OperationCanceledException) { throw; }
            catch (Exception ex) when (ex is CSharpDbException or InvalidOperationException or FormatException or JsonException or ArgumentException)
            { diagnostics.Add(new(table, ex.Message)); }
        }
        static string Value(Dictionary<string, string?> row, string key) => row.GetValueOrDefault(key) ?? "";

        await Read(ProcedureTableName, async () =>
        {
            foreach (var row in await Rows(ProcedureTableName))
                Add("Procedure", Value(row, "name"), Value(row, "body_sql"), enabled: Value(row, "is_enabled") != "0",
                    metadata: JsonSerializer.Serialize(new
                    {
                        description = row.GetValueOrDefault("description"),
                        parameters = DeserializeProcedureParameters(Value(row, "params_json")),
                    }, DefinitionJson));
        });
        await Read(SavedQueryTableName, async () =>
        {
            foreach (var row in await Rows(SavedQueryTableName))
                if (!Value(row, "name").StartsWith("__designer_layout:", StringComparison.Ordinal))
                    Add("Saved query", Value(row, "name"), Value(row, "sql_text"), identity: "SavedQuery:" + Value(row, "id"));
        });
        await Read("__data_model_diagrams", async () =>
        {
            foreach (var row in await Rows("__data_model_diagrams"))
                AddDataModelDefinition(Value(row, "id"), Value(row, "name"), Value(row, "diagram_json"), Add, diagnostics, ct);
        });
        await Read("__external_tables", async () =>
            await AddExternalDefinitionsAsync(await Rows("__external_tables"), Add, diagnostics, ct));
        await Read("__validation_rules", async () =>
        {
            foreach (var row in await Rows("__validation_rules"))
                Add("Validation rule", Value(row, "rule_name"), JsonSerializer.Serialize(row, DefinitionJson), "validation", Value(row, "table_name"));
        });
        await Read("__code_modules", async () =>
        {
            foreach (var row in await Rows("__code_modules"))
                Add("Module", Value(row, "name"), Value(row, "source"), "module", Value(row, "owner_id"), "Module:" + Value(row, "module_id"),
                    metadata: JsonSerializer.Serialize(new { owner_id = Value(row, "owner_id"), owner_kind = Value(row, "owner_kind") }, DefinitionJson));
        });
        await Read("__forms", async () =>
        {
            foreach (var row in await Rows("__forms"))
                Add("Form", Value(row, "name"), Value(row, "definition_json"), "form", Value(row, "table_name"), "Form:" + Value(row, "id"));
        });
        await Read("__reports", async () =>
        {
            var chunks = new Dictionary<string, string>();
            foreach (var group in (await Rows("__report_definition_chunks")).GroupBy(r => Value(r, "storage_id")))
            {
                var parts = group.Select(r => (Ordinal: int.TryParse(Value(r, "chunk_ordinal"), NumberStyles.None, CultureInfo.InvariantCulture, out int n) ? n : -1, Text: Value(r, "chunk_text")))
                    .OrderBy(p => p.Ordinal).ToArray();
                if (parts.Where((p, i) => p.Ordinal != i).Any())
                { diagnostics.Add(new(group.Key, "Report definition chunks have missing, duplicate, or invalid ordinals.")); continue; }
                chunks[group.Key] = string.Concat(parts.Select(p => p.Text));
            }
            foreach (var row in await Rows("__reports"))
            {
                string source = Value(row, "definition_json");
                if (source.StartsWith("#chunked:", StringComparison.Ordinal))
                {
                    if (!chunks.TryGetValue(source[9..], out string? json))
                    { diagnostics.Add(new(Value(row, "name"), "Report definition chunks are missing.")); continue; }
                    source = json;
                }
                Add("Report", Value(row, "name"), source, "report", identity: "Report:" + Value(row, "id"));
            }
        });
        await Read("_etl_pipelines", async () =>
        {
            var versions = (await Rows("_etl_pipeline_versions")).ToLookup(r => Value(r, "name"));
            foreach (var row in await Rows("_etl_pipelines"))
            {
                var version = versions[Value(row, "name")].FirstOrDefault(v => Value(v, "revision") == Value(row, "current_revision"));
                if (version is null) { diagnostics.Add(new(Value(row, "name"), "Current pipeline revision is missing.")); continue; }
                Add("Pipeline", Value(row, "name"), Value(version, "package_json"), "pipeline");
            }
        });

        var annotations = new Dictionary<string, Dictionary<string, string?>>(StringComparer.Ordinal);
        await Read("__documentation_annotations", async () =>
        {
            foreach (var row in await Rows("__documentation_annotations"))
                annotations.Add(Value(row, "object_id"), row);
        });
        var identities = records.Select(r => r.Id).ToHashSet(StringComparer.Ordinal);
        int unmatched = annotations.Count(pair => !identities.Contains(pair.Key) && pair.Value.GetValueOrDefault("description") is not null);
        if (unmatched > 0)
            diagnostics.Add(new("Documentation", $"{unmatched} description(s) belong to unavailable objects and were retained without reassignment."));
        for (int i = 0; i < records.Count; i++)
        {
            var record = records[i];
            string fingerprint = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(
                JsonSerializer.Serialize(new { record.Id, record.Kind, record.Name, record.OwnerName, record.SourceHash, record.MetadataJson, record.IsEnabled }, DefinitionJson))));
            bool stable = stableIdentities.Contains(record.Id);
            string? nativeDescription = null;
            if (record.Kind == "Procedure" && record.MetadataJson is not null)
            {
                using var metadata = JsonDocument.Parse(record.MetadataJson);
                nativeDescription = metadata.RootElement.GetProperty("description").GetString();
            }
            annotations.TryGetValue(record.Id, out var annotation);
            long revision = 0;
            if (annotation is not null && (!long.TryParse(Value(annotation, "revision"), out revision) || revision < 1
                || annotation.GetValueOrDefault("description")?.Length > 4000))
            {
                diagnostics.Add(new("Documentation", "An invalid description was excluded. Repair the documentation catalog before saving."));
                annotation = null;
                revision = 0;
            }
            records[i] = record with { Documentation = new()
            {
                NativeDescription = nativeDescription,
                Description = annotation?.GetValueOrDefault("description"),
                Revision = revision, DefinitionFingerprint = fingerprint, HasStableIdentity = stable,
                NeedsReview = annotation?.GetValueOrDefault("description") is not null && !stable
                    && annotation.GetValueOrDefault("definition_hash") != fingerprint,
            } };
        }
        records = records.OrderBy(r => r.Id, StringComparer.Ordinal).ThenBy(r => r.PartIndex).ToList();
        string versionHash = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(
            JsonSerializer.Serialize(records.Select(r => new { r.Id, r.Kind, r.Name, r.OwnerName, r.Format, r.SourceHash, r.MetadataJson, r.IsEnabled, r.PartIndex, r.Documentation }), DefinitionJson) + JsonSerializer.Serialize(diagnostics))));
        ct.ThrowIfCancellationRequested();
        // A bounded, short-lived snapshot avoids re-reading the entire catalog for every transport page.
        while (_definitionReads.Count >= 4) _definitionReads.Remove(_definitionReads.MinBy(p => p.Value.LastRead).Key);
        string snapshotId = Guid.NewGuid().ToString("N");
        var snapshot = new DefinitionReadSnapshot(versionHash, now, records.ToArray(), diagnostics.ToArray());
        var firstPage = DefinitionPage(snapshot, snapshotId, 0, pageSize);
        if (firstPage.ContinuationToken is not null) _definitionReads[snapshotId] = (snapshot, DateTimeOffset.UtcNow);
        return firstPage;
    }

    private static DefinitionCatalogPage DefinitionPage(DefinitionReadSnapshot snapshot, string id, int offset, int pageSize)
    {
        // Metadata and descriptions count toward the same transport envelope as source fragments.
        var selected = new List<DefinitionCatalogRecord>();
        int bytes = JsonSerializer.SerializeToUtf8Bytes(snapshot.Diagnostics, DefinitionJson).Length + 1024;
        foreach (var record in snapshot.Records.Skip(offset).Take(pageSize))
        {
            int size = JsonSerializer.SerializeToUtf8Bytes(record, DefinitionJson).Length + 1;
            if (bytes + size > 3 * 1024 * 1024)
            {
                if (selected.Count == 0) throw new InvalidOperationException("Definition metadata exceeds the transport limit.");
                break;
            }
            bytes += size;
            selected.Add(record);
        }
        int next = offset + selected.Count;
        return new()
        {
            DocumentationVersion = 1, CatalogVersion = snapshot.Version, CapturedUtc = snapshot.CapturedUtc,
            Records = selected, Diagnostics = snapshot.Diagnostics,
            ContinuationToken = next < snapshot.Records.Length ? $"{id}:{next}" : null,
        };
    }

    // Escape delimiters before composing a name identity, including the escape character itself.
    private static string IdentityPart(string? value) => (value ?? "").Replace("%", "%25", StringComparison.Ordinal).Replace(":", "%3A", StringComparison.Ordinal);
}
