using System.Globalization;
using System.Text.Json;
using CSharpDB.Admin.Forms.Contracts;
using CSharpDB.Admin.Forms.Models;
using CSharpDB.Admin.Forms.Serialization;
using CSharpDB.Client;

namespace CSharpDB.Admin.Forms.Services;

public sealed class DbFormRepository(ICSharpDbClient dbClient) : IFormRepository
{
    internal const string MetadataTableName = "__forms";
    private const string TableNameIndex = "idx___forms_table_name";

    private readonly SemaphoreSlim _initLock = new(1, 1);
    private bool _initialized;

    public async Task<FormDefinition?> GetAsync(string formId)
    {
        await EnsureInitializedAsync();
        string sql = $"""
            SELECT definition_json
            FROM {MetadataTableName}
            WHERE id = {FormSql.FormatLiteral(formId)}
            LIMIT 1;
            """;

        var rows = FormSql.ReadRows(await dbClient.ExecuteSqlAsync(sql));
        if (rows.Count == 0)
            return null;

        return DeserializeForm(rows[0]);
    }

    public async Task<FormDefinition> CreateAsync(FormDefinition form)
    {
        await EnsureInitializedAsync();
        FormDefinition stored = NormalizeForCreate(form);
        string now = DateTime.UtcNow.ToString("O", CultureInfo.InvariantCulture);
        string json = JsonSerializer.Serialize(stored, JsonDefaults.Options);

        string sql = $"""
            INSERT INTO {MetadataTableName} (
                id,
                name,
                table_name,
                definition_json,
                definition_version,
                source_schema_signature,
                created_utc,
                updated_utc
            )
            VALUES (
                {FormSql.FormatLiteral(stored.FormId)},
                {FormSql.FormatLiteral(stored.Name)},
                {FormSql.FormatLiteral(stored.TableName)},
                {FormSql.FormatLiteral(json)},
                {FormSql.FormatLiteral(stored.DefinitionVersion)},
                {FormSql.FormatLiteral(stored.SourceSchemaSignature)},
                {FormSql.FormatLiteral(now)},
                {FormSql.FormatLiteral(now)}
            );
            """;

        FormSql.ThrowIfError(await dbClient.ExecuteSqlAsync(sql));
        return stored;
    }

    public async Task<UpdateResult> TryUpdateAsync(string formId, int expectedVersion, FormDefinition updated)
    {
        await EnsureInitializedAsync();
        FormDefinition stored = NormalizeForUpdate(formId, expectedVersion, updated);
        string now = DateTime.UtcNow.ToString("O", CultureInfo.InvariantCulture);
        string json = JsonSerializer.Serialize(stored, JsonDefaults.Options);

        string sql = $"""
            UPDATE {MetadataTableName}
            SET name = {FormSql.FormatLiteral(stored.Name)},
                table_name = {FormSql.FormatLiteral(stored.TableName)},
                definition_json = {FormSql.FormatLiteral(json)},
                definition_version = {FormSql.FormatLiteral(stored.DefinitionVersion)},
                source_schema_signature = {FormSql.FormatLiteral(stored.SourceSchemaSignature)},
                updated_utc = {FormSql.FormatLiteral(now)}
            WHERE id = {FormSql.FormatLiteral(formId)}
              AND definition_version = {FormSql.FormatLiteral(expectedVersion)};
            """;

        var result = await dbClient.ExecuteSqlAsync(sql);
        FormSql.ThrowIfError(result);
        if (result.RowsAffected == 1)
            return new UpdateResult.Ok(stored);

        return await GetAsync(formId) is null
            ? new UpdateResult.NotFound()
            : new UpdateResult.Conflict();
    }

    public async Task<IReadOnlyList<FormDefinition>> ListAsync(string? tableName = null)
    {
        await EnsureInitializedAsync();
        string whereClause = string.IsNullOrWhiteSpace(tableName)
            ? string.Empty
            : $"WHERE table_name = {FormSql.FormatLiteral(tableName)}";

        string sql = $"""
            SELECT definition_json
            FROM {MetadataTableName}
            {whereClause}
            ORDER BY updated_utc DESC, name ASC;
            """;

        return FormSql.ReadRows(await dbClient.ExecuteSqlAsync(sql))
            .Select(DeserializeForm)
            .ToArray();
    }

    public async Task<bool> DeleteAsync(string formId)
    {
        await EnsureInitializedAsync();
        string sql = $"""
            DELETE FROM {MetadataTableName}
            WHERE id = {FormSql.FormatLiteral(formId)};
            """;

        var result = await dbClient.ExecuteSqlAsync(sql);
        FormSql.ThrowIfError(result);
        return result.RowsAffected > 0;
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
                    table_name TEXT NOT NULL,
                    definition_json TEXT NOT NULL,
                    definition_version INTEGER NOT NULL,
                    source_schema_signature TEXT NOT NULL,
                    created_utc TEXT NOT NULL,
                    updated_utc TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS {TableNameIndex}
                ON {MetadataTableName} (table_name);
                """;

            FormSql.ThrowIfError(await dbClient.ExecuteSqlAsync(sql));
            _initialized = true;
        }
        finally
        {
            _initLock.Release();
        }
    }

    private static FormDefinition DeserializeForm(Dictionary<string, object?> row)
    {
        string json = row["definition_json"]?.ToString()
            ?? throw new InvalidOperationException("Stored form definition is missing JSON.");

        return JsonSerializer.Deserialize<FormDefinition>(json, JsonDefaults.Options)
            ?? throw new InvalidOperationException("Stored form definition JSON could not be deserialized.");
    }

    private static FormDefinition NormalizeForCreate(FormDefinition form)
    {
        ValidateForPersistence(form);
        return form with
        {
            FormId = string.IsNullOrWhiteSpace(form.FormId) ? Guid.NewGuid().ToString("N") : form.FormId,
            Name = string.IsNullOrWhiteSpace(form.Name) ? $"{form.TableName} Form" : form.Name.Trim(),
            DefinitionVersion = 1,
        };
    }

    private static FormDefinition NormalizeForUpdate(string formId, int expectedVersion, FormDefinition form)
    {
        ValidateForPersistence(form);
        return form with
        {
            FormId = formId,
            Name = string.IsNullOrWhiteSpace(form.Name) ? $"{form.TableName} Form" : form.Name.Trim(),
            DefinitionVersion = expectedVersion + 1,
        };
    }

    private static void ValidateForPersistence(FormDefinition form)
    {
        if (string.IsNullOrWhiteSpace(form.TableName))
            throw new InvalidOperationException("Forms must be bound to a source table before they can be saved.");

        if (string.IsNullOrWhiteSpace(form.SourceSchemaSignature))
            throw new InvalidOperationException("Forms must include a source schema signature before they can be saved.");
    }
}
