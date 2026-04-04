using System.Globalization;
using System.Text;
using System.Text.Json;
using CSharpDB.Admin.Reports.Contracts;
using CSharpDB.Admin.Reports.Models;
using CSharpDB.Admin.Reports.Serialization;
using CSharpDB.Client;

namespace CSharpDB.Admin.Reports.Services;

public sealed class DbReportRepository(ICSharpDbClient dbClient) : IReportRepository
{
    internal const string MetadataTableName = "__reports";
    private const string SourceIndexName = "idx___reports_source";
    private const string ChunkTableName = "__report_definition_chunks";
    private const string ChunkStorageIndexName = "idx___report_chunks_storage";
    private const string ChunkedDefinitionMarkerPrefix = "#chunked:";
    private const int MaxChunkLength = 1024;

    private readonly SemaphoreSlim _initLock = new(1, 1);
    private bool _initialized;

    public async Task<ReportDefinition?> GetAsync(string reportId)
    {
        await EnsureInitializedAsync();

        Dictionary<string, object?>? row = await GetMetadataRowAsync(reportId);
        return row is null ? null : await DeserializeReportAsync(row);
    }

    public async Task<ReportDefinition> CreateAsync(ReportDefinition report)
    {
        await EnsureInitializedAsync();

        ReportDefinition stored = NormalizeForCreate(report);
        string now = DateTime.UtcNow.ToString("O", CultureInfo.InvariantCulture);
        string json = JsonSerializer.Serialize(stored, JsonDefaults.Options);
        string storageId = Guid.NewGuid().ToString("N");
        string marker = BuildChunkedDefinitionMarker(storageId);

        await PersistDefinitionChunksAsync(stored.ReportId, storageId, json);
        try
        {
            string sql = $"""
                INSERT INTO {MetadataTableName} (
                    id,
                    name,
                    source_kind,
                    source_name,
                    definition_json,
                    definition_version,
                    source_schema_signature,
                    created_utc,
                    updated_utc
                )
                VALUES (
                    {ReportSql.FormatLiteral(stored.ReportId)},
                    {ReportSql.FormatLiteral(stored.Name)},
                    {ReportSql.FormatLiteral(stored.Source.Kind.ToString())},
                    {ReportSql.FormatLiteral(stored.Source.Name)},
                    {ReportSql.FormatLiteral(marker)},
                    {ReportSql.FormatLiteral(stored.DefinitionVersion)},
                    {ReportSql.FormatLiteral(stored.SourceSchemaSignature)},
                    {ReportSql.FormatLiteral(now)},
                    {ReportSql.FormatLiteral(now)}
                );
                """;

            ReportSql.ThrowIfError(await dbClient.ExecuteSqlAsync(sql));
            return stored;
        }
        catch
        {
            await TryDeleteChunksByStorageAsync(storageId);
            throw;
        }
    }

    public async Task<ReportUpdateResult> TryUpdateAsync(string reportId, int expectedVersion, ReportDefinition updated)
    {
        await EnsureInitializedAsync();

        Dictionary<string, object?>? currentRow = await GetMetadataRowAsync(reportId);
        if (currentRow is null)
            return new ReportUpdateResult.NotFound();

        int currentVersion = ReadDefinitionVersion(currentRow);
        if (currentVersion != expectedVersion)
            return new ReportUpdateResult.Conflict();

        ReportDefinition stored = NormalizeForUpdate(reportId, expectedVersion, updated);
        string now = DateTime.UtcNow.ToString("O", CultureInfo.InvariantCulture);
        string json = JsonSerializer.Serialize(stored, JsonDefaults.Options);
        string storageId = Guid.NewGuid().ToString("N");
        string marker = BuildChunkedDefinitionMarker(storageId);

        await PersistDefinitionChunksAsync(stored.ReportId, storageId, json);

        try
        {
            string sql = $"""
                UPDATE {MetadataTableName}
                SET name = {ReportSql.FormatLiteral(stored.Name)},
                    source_kind = {ReportSql.FormatLiteral(stored.Source.Kind.ToString())},
                    source_name = {ReportSql.FormatLiteral(stored.Source.Name)},
                    definition_json = {ReportSql.FormatLiteral(marker)},
                    definition_version = {ReportSql.FormatLiteral(stored.DefinitionVersion)},
                    source_schema_signature = {ReportSql.FormatLiteral(stored.SourceSchemaSignature)},
                    updated_utc = {ReportSql.FormatLiteral(now)}
                WHERE id = {ReportSql.FormatLiteral(reportId)}
                  AND definition_version = {ReportSql.FormatLiteral(expectedVersion)};
                """;

            var result = await dbClient.ExecuteSqlAsync(sql);
            ReportSql.ThrowIfError(result);
            if (result.RowsAffected == 1)
            {
                if (TryParseChunkStorageId(currentRow, out string? previousStorageId) && previousStorageId is not null)
                    await TryDeleteChunksByStorageAsync(previousStorageId);

                return new ReportUpdateResult.Ok(stored);
            }

            await TryDeleteChunksByStorageAsync(storageId);
            return await GetAsync(reportId) is null
                ? new ReportUpdateResult.NotFound()
                : new ReportUpdateResult.Conflict();
        }
        catch
        {
            await TryDeleteChunksByStorageAsync(storageId);
            throw;
        }
    }

    public async Task<IReadOnlyList<ReportDefinition>> ListAsync(ReportSourceKind? sourceKind = null, string? sourceName = null)
    {
        await EnsureInitializedAsync();

        var predicates = new List<string>();
        if (sourceKind.HasValue)
            predicates.Add($"source_kind = {ReportSql.FormatLiteral(sourceKind.Value.ToString())}");
        if (!string.IsNullOrWhiteSpace(sourceName))
            predicates.Add($"source_name = {ReportSql.FormatLiteral(sourceName)}");

        string whereClause = predicates.Count == 0 ? string.Empty : $"WHERE {string.Join(" AND ", predicates)}";
        string sql = $"""
            SELECT id,
                   name,
                   source_kind,
                   source_name,
                   definition_json,
                   definition_version,
                   source_schema_signature
            FROM {MetadataTableName}
            {whereClause}
            ORDER BY updated_utc DESC, name ASC;
            """;

        IReadOnlyList<Dictionary<string, object?>> rows = ReportSql.ReadRows(await dbClient.ExecuteSqlAsync(sql));
        var reports = new List<ReportDefinition>(rows.Count);
        foreach (Dictionary<string, object?> row in rows)
            reports.Add(await DeserializeReportAsync(row));

        return reports;
    }

    public async Task<bool> DeleteAsync(string reportId)
    {
        await EnsureInitializedAsync();

        string sql = $"""
            DELETE FROM {MetadataTableName}
            WHERE id = {ReportSql.FormatLiteral(reportId)};
            """;

        Dictionary<string, object?>? currentRow = await GetMetadataRowAsync(reportId);
        var result = await dbClient.ExecuteSqlAsync(sql);
        ReportSql.ThrowIfError(result);
        if (result.RowsAffected == 0)
            return false;

        await TryDeleteChunksByReportAsync(reportId);
        if (TryParseChunkStorageId(currentRow, out string? storageId) && storageId is not null)
            await TryDeleteChunksByStorageAsync(storageId);

        return true;
    }

    private async Task EnsureInitializedAsync()
    {
        if (_initialized)
            return;

        await _initLock.WaitAsync();
        try
        {
            if (_initialized)
                return;

            string sql = $"""
                CREATE TABLE IF NOT EXISTS {MetadataTableName} (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    source_kind TEXT NOT NULL,
                    source_name TEXT NOT NULL,
                    definition_json TEXT NOT NULL,
                    definition_version INTEGER NOT NULL,
                    source_schema_signature TEXT NOT NULL,
                    created_utc TEXT NOT NULL,
                    updated_utc TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS {SourceIndexName}
                ON {MetadataTableName} (source_kind, source_name);
                CREATE TABLE IF NOT EXISTS {ChunkTableName} (
                    chunk_id TEXT PRIMARY KEY,
                    report_id TEXT NOT NULL,
                    storage_id TEXT NOT NULL,
                    chunk_ordinal INTEGER NOT NULL,
                    chunk_text TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS {ChunkStorageIndexName}
                ON {ChunkTableName} (storage_id, chunk_ordinal);
                """;

            ReportSql.ThrowIfError(await dbClient.ExecuteSqlAsync(sql));
            _initialized = true;
        }
        finally
        {
            _initLock.Release();
        }
    }

    private async Task<Dictionary<string, object?>?> GetMetadataRowAsync(string reportId)
    {
        string sql = $"""
            SELECT id,
                   name,
                   source_kind,
                   source_name,
                   definition_json,
                   definition_version,
                   source_schema_signature
            FROM {MetadataTableName}
            WHERE id = {ReportSql.FormatLiteral(reportId)}
            LIMIT 1;
            """;

        IReadOnlyList<Dictionary<string, object?>> rows = ReportSql.ReadRows(await dbClient.ExecuteSqlAsync(sql));
        return rows.Count == 0 ? null : rows[0];
    }

    private async Task<ReportDefinition> DeserializeReportAsync(Dictionary<string, object?> row)
    {
        string storedDefinition = row["definition_json"]?.ToString()
            ?? throw new InvalidOperationException("Stored report definition is missing JSON.");

        string json = await LoadDefinitionJsonAsync(row, storedDefinition);
        return DeserializeReportJson(json);
    }

    private async Task<string> LoadDefinitionJsonAsync(Dictionary<string, object?> row, string storedDefinition)
    {
        if (!TryParseChunkStorageId(storedDefinition, out string? storageId))
            return storedDefinition;

        string sql = $"""
            SELECT chunk_text
            FROM {ChunkTableName}
            WHERE storage_id = {ReportSql.FormatLiteral(storageId)}
            ORDER BY chunk_ordinal ASC;
            """;

        IReadOnlyList<Dictionary<string, object?>> rows = ReportSql.ReadRows(await dbClient.ExecuteSqlAsync(sql));
        if (rows.Count == 0)
        {
            string reportId = row["id"]?.ToString() ?? "<unknown>";
            throw new InvalidOperationException($"Stored report definition chunks are missing for report '{reportId}'.");
        }

        var json = new StringBuilder(rows.Sum(static chunk => chunk["chunk_text"]?.ToString()?.Length ?? 0));
        foreach (Dictionary<string, object?> chunk in rows)
            json.Append(chunk["chunk_text"]?.ToString() ?? string.Empty);

        return json.ToString();
    }

    private static ReportDefinition DeserializeReportJson(string json)
    {
        return JsonSerializer.Deserialize<ReportDefinition>(json, JsonDefaults.Options)
            ?? throw new InvalidOperationException("Stored report definition JSON could not be deserialized.");
    }

    private static ReportDefinition NormalizeForCreate(ReportDefinition report)
    {
        ValidateForPersistence(report);
        return report with
        {
            ReportId = string.IsNullOrWhiteSpace(report.ReportId) ? Guid.NewGuid().ToString("N") : report.ReportId,
            Name = string.IsNullOrWhiteSpace(report.Name) ? $"{report.Source.Name} Report" : report.Name.Trim(),
            DefinitionVersion = 1,
        };
    }

    private static ReportDefinition NormalizeForUpdate(string reportId, int expectedVersion, ReportDefinition report)
    {
        ValidateForPersistence(report);
        return report with
        {
            ReportId = reportId,
            Name = string.IsNullOrWhiteSpace(report.Name) ? $"{report.Source.Name} Report" : report.Name.Trim(),
            DefinitionVersion = expectedVersion + 1,
        };
    }

    private static void ValidateForPersistence(ReportDefinition report)
    {
        if (string.IsNullOrWhiteSpace(report.Source.Name))
            throw new InvalidOperationException("Reports must be bound to a source before they can be saved.");

        if (string.IsNullOrWhiteSpace(report.SourceSchemaSignature))
            throw new InvalidOperationException("Reports must include a source schema signature before they can be saved.");
    }

    private async Task PersistDefinitionChunksAsync(string reportId, string storageId, string json)
    {
        await TryDeleteChunksByStorageAsync(storageId);

        IReadOnlyList<string> chunks = SplitIntoChunks(json);
        for (int start = 0; start < chunks.Count; start += 32)
        {
            int end = Math.Min(start + 32, chunks.Count);
            var sql = new StringBuilder((end - start) * 1600);
            for (int ordinal = start; ordinal < end; ordinal++)
            {
                string chunk = chunks[ordinal];
                string chunkId = $"{storageId}:{ordinal:D4}";
                sql.AppendLine($"""
                    INSERT INTO {ChunkTableName} (
                        chunk_id,
                        report_id,
                        storage_id,
                        chunk_ordinal,
                        chunk_text
                    )
                    VALUES (
                        {ReportSql.FormatLiteral(chunkId)},
                        {ReportSql.FormatLiteral(reportId)},
                        {ReportSql.FormatLiteral(storageId)},
                        {ReportSql.FormatLiteral(ordinal)},
                        {ReportSql.FormatLiteral(chunk)}
                    );
                    """);
            }

            ReportSql.ThrowIfError(await dbClient.ExecuteSqlAsync(sql.ToString()));
        }
    }

    private async Task TryDeleteChunksByStorageAsync(string storageId)
    {
        string sql = $"""
            DELETE FROM {ChunkTableName}
            WHERE storage_id = {ReportSql.FormatLiteral(storageId)};
            """;

        try
        {
            await dbClient.ExecuteSqlAsync(sql);
        }
        catch
        {
        }
    }

    private async Task TryDeleteChunksByReportAsync(string reportId)
    {
        string sql = $"""
            DELETE FROM {ChunkTableName}
            WHERE report_id = {ReportSql.FormatLiteral(reportId)};
            """;

        try
        {
            await dbClient.ExecuteSqlAsync(sql);
        }
        catch
        {
        }
    }

    private static int ReadDefinitionVersion(IReadOnlyDictionary<string, object?> row)
        => Convert.ToInt32(ReportSql.NormalizeValue(row["definition_version"]), CultureInfo.InvariantCulture);

    private static string BuildChunkedDefinitionMarker(string storageId) => $"{ChunkedDefinitionMarkerPrefix}{storageId}";

    private static bool TryParseChunkStorageId(IReadOnlyDictionary<string, object?>? row, out string? storageId)
        => TryParseChunkStorageId(row?["definition_json"]?.ToString(), out storageId);

    private static bool TryParseChunkStorageId(string? storedDefinition, out string? storageId)
    {
        if (!string.IsNullOrWhiteSpace(storedDefinition) &&
            storedDefinition.StartsWith(ChunkedDefinitionMarkerPrefix, StringComparison.Ordinal))
        {
            storageId = storedDefinition[ChunkedDefinitionMarkerPrefix.Length..];
            return !string.IsNullOrWhiteSpace(storageId);
        }

        storageId = null;
        return false;
    }

    private static IReadOnlyList<string> SplitIntoChunks(string value)
    {
        if (string.IsNullOrEmpty(value))
            return [string.Empty];

        var chunks = new List<string>((value.Length + MaxChunkLength - 1) / MaxChunkLength);
        for (int offset = 0; offset < value.Length; offset += MaxChunkLength)
        {
            int length = Math.Min(MaxChunkLength, value.Length - offset);
            chunks.Add(value.Substring(offset, length));
        }

        return chunks;
    }
}
