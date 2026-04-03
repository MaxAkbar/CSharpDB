using CSharpDB.Admin.Forms.Contracts;
using CSharpDB.Admin.Forms.Models;
using CSharpDB.Client;
using System.Globalization;

namespace CSharpDB.Admin.Forms.Services;

public sealed class DbFormRecordService(ICSharpDbClient dbClient) : IFormRecordService
{
    public string GetPrimaryKeyColumn(FormTableDefinition table)
    {
        if (table.PrimaryKey.Count != 1)
            throw new InvalidOperationException($"Table '{table.TableName}' must have exactly one primary key column.");

        return table.PrimaryKey[0];
    }

    public Task<Dictionary<string, object?>?> GetRecordAsync(FormTableDefinition table, object pkValue, CancellationToken ct = default)
        => dbClient.GetRowByPkAsync(table.TableName, GetPrimaryKeyColumn(table), pkValue, ct);

    public async Task<FormRecordPage> ListRecordPageAsync(FormTableDefinition table, int pageNumber, int pageSize, CancellationToken ct = default)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(pageNumber, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(pageSize, 1);

        string tableName = FormSql.RequireIdentifier(table.TableName, nameof(table.TableName));
        string pkColumn = FormSql.RequireIdentifier(GetPrimaryKeyColumn(table), "primaryKey");
        int totalCount = await ExecuteCountAsync($"""
            SELECT COUNT(*) AS RowCount
            FROM {tableName};
            """, ct);

        int totalPages = totalCount == 0 ? 1 : (int)Math.Ceiling(totalCount / (double)pageSize);
        int effectivePage = Math.Min(pageNumber, totalPages);
        int offset = (effectivePage - 1) * pageSize;

        List<Dictionary<string, object?>> rows = await ExecuteRowsAsync($"""
            SELECT *
            FROM {tableName}
            ORDER BY {pkColumn}
            LIMIT {pageSize}
            OFFSET {offset};
            """, ct);

        return new FormRecordPage(effectivePage, pageSize, totalCount, rows);
    }

    public async Task<FormRecordPage> SearchRecordPageAsync(FormTableDefinition table, string searchField, string searchValue, int pageNumber, int pageSize, CancellationToken ct = default)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(pageNumber, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(pageSize, 1);
        ArgumentException.ThrowIfNullOrWhiteSpace(searchField);
        ArgumentException.ThrowIfNullOrWhiteSpace(searchValue);

        string tableName = FormSql.RequireIdentifier(table.TableName, nameof(table.TableName));
        string pkColumn = FormSql.RequireIdentifier(GetPrimaryKeyColumn(table), "primaryKey");
        string whereClause = BuildTextSearchWhereClause(table, searchField, searchValue);
        int totalCount = await ExecuteCountAsync($"""
            SELECT COUNT(*) AS RowCount
            FROM {tableName}
            WHERE {whereClause};
            """, ct);

        int totalPages = totalCount == 0 ? 1 : (int)Math.Ceiling(totalCount / (double)pageSize);
        int effectivePage = Math.Min(pageNumber, totalPages);
        int offset = (effectivePage - 1) * pageSize;

        List<Dictionary<string, object?>> rows = await ExecuteRowsAsync($"""
            SELECT *
            FROM {tableName}
            WHERE {whereClause}
            ORDER BY {pkColumn}
            LIMIT {pageSize}
            OFFSET {offset};
            """, ct);

        return new FormRecordPage(effectivePage, pageSize, totalCount, rows);
    }

    public Task<List<Dictionary<string, object?>>> ListRecordsAsync(FormTableDefinition table, CancellationToken ct = default)
        => ExecuteRowsAsync($"""
            SELECT *
            FROM {FormSql.RequireIdentifier(table.TableName, nameof(table.TableName))}
            ORDER BY {FormSql.RequireIdentifier(GetPrimaryKeyColumn(table), "primaryKey")};
            """, ct);

    public async Task<int?> GetRecordOrdinalAsync(FormTableDefinition table, object pkValue, CancellationToken ct = default)
    {
        string tableName = FormSql.RequireIdentifier(table.TableName, nameof(table.TableName));
        string pkColumn = FormSql.RequireIdentifier(GetPrimaryKeyColumn(table), "primaryKey");
        string pkLiteral = FormSql.FormatLiteral(pkValue);

        int matchingRecords = await ExecuteCountAsync($"""
            SELECT COUNT(*) AS RowCount
            FROM {tableName}
            WHERE {pkColumn} = {pkLiteral};
            """, ct);
        if (matchingRecords == 0)
            return null;

        return await ExecuteCountAsync($"""
            SELECT COUNT(*) AS RowCount
            FROM {tableName}
            WHERE {pkColumn} < {pkLiteral};
            """, ct);
    }

    public async Task<int?> GetRecordOrdinalAsync(FormTableDefinition table, object pkValue, string searchField, string searchValue, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(searchField);
        ArgumentException.ThrowIfNullOrWhiteSpace(searchValue);

        string tableName = FormSql.RequireIdentifier(table.TableName, nameof(table.TableName));
        string pkColumn = FormSql.RequireIdentifier(GetPrimaryKeyColumn(table), "primaryKey");
        string pkLiteral = FormSql.FormatLiteral(pkValue);
        string whereClause = BuildTextSearchWhereClause(table, searchField, searchValue);

        int matchingRecords = await ExecuteCountAsync($"""
            SELECT COUNT(*) AS RowCount
            FROM {tableName}
            WHERE {whereClause}
              AND {pkColumn} = {pkLiteral};
            """, ct);
        if (matchingRecords == 0)
            return null;

        return await ExecuteCountAsync($"""
            SELECT COUNT(*) AS RowCount
            FROM {tableName}
            WHERE {whereClause}
              AND {pkColumn} < {pkLiteral};
            """, ct);
    }

    public Task<List<Dictionary<string, object?>>> ListFilteredRecordsAsync(FormTableDefinition table, string filterField, object? filterValue, CancellationToken ct = default)
    {
        RequireField(table, filterField);
        string whereClause = filterValue is null
            ? $"{FormSql.RequireIdentifier(filterField, nameof(filterField))} IS NULL"
            : $"{FormSql.RequireIdentifier(filterField, nameof(filterField))} = {FormSql.FormatLiteral(filterValue)}";

        string sql = $"""
            SELECT *
            FROM {FormSql.RequireIdentifier(table.TableName, nameof(table.TableName))}
            WHERE {whereClause}
            ORDER BY {FormSql.RequireIdentifier(GetPrimaryKeyColumn(table), "primaryKey")};
            """;

        return ExecuteRowsAsync(sql, ct);
    }

    public async Task<Dictionary<string, object?>> CreateRecordAsync(FormTableDefinition table, Dictionary<string, object?> values, CancellationToken ct = default)
    {
        var writeValues = FilterWriteValues(table, values, includePrimaryKey: true);
        if (writeValues.Count > 0)
        {
            await dbClient.InsertRowAsync(table.TableName, writeValues, ct);
        }
        else
        {
            await dbClient.InsertRowAsync(table.TableName, BuildEmptyInsertValues(table), ct);
        }

        string pkColumn = GetPrimaryKeyColumn(table);
        if (TryGetCaseInsensitive(values, pkColumn, out object? pkValue) && pkValue is not null)
        {
            return await GetRecordAsync(table, pkValue, ct)
                ?? throw new InvalidOperationException("The inserted record could not be reloaded.");
        }

        var latest = await ExecuteRowsAsync($"""
            SELECT *
            FROM {FormSql.RequireIdentifier(table.TableName, nameof(table.TableName))}
            ORDER BY {FormSql.RequireIdentifier(pkColumn, nameof(pkColumn))} DESC
            LIMIT 1;
            """, ct);

        return latest.Count > 0
            ? latest[0]
            : throw new InvalidOperationException("The inserted record could not be reloaded.");
    }

    public async Task<Dictionary<string, object?>> UpdateRecordAsync(FormTableDefinition table, object pkValue, Dictionary<string, object?> values, CancellationToken ct = default)
    {
        string pkColumn = GetPrimaryKeyColumn(table);
        var writeValues = FilterWriteValues(table, values, includePrimaryKey: false);
        if (writeValues.Count > 0)
            await dbClient.UpdateRowAsync(table.TableName, pkColumn, pkValue, writeValues, ct);

        return await GetRecordAsync(table, pkValue, ct)
            ?? throw new InvalidOperationException("The updated record could not be reloaded.");
    }

    public Task DeleteRecordAsync(FormTableDefinition table, object pkValue, CancellationToken ct = default)
        => dbClient.DeleteRowAsync(table.TableName, GetPrimaryKeyColumn(table), pkValue, ct);

    private async Task<List<Dictionary<string, object?>>> ExecuteRowsAsync(string sql, CancellationToken ct)
    {
        return FormSql.ReadRows(await dbClient.ExecuteSqlAsync(sql, ct)).ToList();
    }

    private async Task<int> ExecuteCountAsync(string sql, CancellationToken ct)
    {
        IReadOnlyList<Dictionary<string, object?>> rows = FormSql.ReadRows(await dbClient.ExecuteSqlAsync(sql, ct));
        if (rows.Count == 0 || rows[0].Count == 0)
            return 0;

        object? rawValue = rows[0].Values.FirstOrDefault();
        object? normalized = FormSql.NormalizeValue(rawValue);
        return normalized is null
            ? 0
            : Convert.ToInt32(normalized, CultureInfo.InvariantCulture);
    }

    private static string BuildTextSearchWhereClause(FormTableDefinition table, string searchField, string searchValue)
    {
        RequireField(table, searchField);
        string columnName = FormSql.RequireIdentifier(searchField, nameof(searchField));
        string searchPattern = $"%{FormSql.EscapeLikePattern(searchValue)}%";
        return $"TEXT({columnName}) LIKE {FormSql.FormatLiteral(searchPattern)} ESCAPE '!'";
    }

    private static Dictionary<string, object?> FilterWriteValues(FormTableDefinition table, Dictionary<string, object?> values, bool includePrimaryKey)
    {
        var filtered = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        string pkColumn = table.PrimaryKey.Count == 1 ? table.PrimaryKey[0] : string.Empty;

        foreach (var field in table.Fields)
        {
            if (!includePrimaryKey && string.Equals(field.Name, pkColumn, StringComparison.OrdinalIgnoreCase))
                continue;

            if (TryGetCaseInsensitive(values, field.Name, out object? value))
                filtered[field.Name] = value;
        }

        return filtered;
    }

    private static Dictionary<string, object?> BuildEmptyInsertValues(FormTableDefinition table)
    {
        string pkColumn = GetSinglePrimaryKeyColumn(table);
        FormFieldDefinition? identityPrimaryKey = table.Fields.FirstOrDefault(field =>
            string.Equals(field.Name, pkColumn, StringComparison.OrdinalIgnoreCase) &&
            field.IsReadOnly);
        if (identityPrimaryKey is not null)
        {
            return new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
            {
                [identityPrimaryKey.Name] = null,
            };
        }

        FormFieldDefinition? nullableField = table.Fields.FirstOrDefault(field =>
            !field.IsReadOnly &&
            field.IsNullable &&
            !string.Equals(field.Name, pkColumn, StringComparison.OrdinalIgnoreCase));
        if (nullableField is not null)
        {
            return new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
            {
                [nullableField.Name] = null,
            };
        }

        throw new InvalidOperationException(
            $"Table '{table.TableName}' requires explicit values and cannot be inserted as an empty record.");
    }

    private static void RequireField(FormTableDefinition table, string fieldName)
    {
        if (!table.Fields.Any(field => string.Equals(field.Name, fieldName, StringComparison.OrdinalIgnoreCase)))
            throw new InvalidOperationException($"Field '{fieldName}' does not exist on table '{table.TableName}'.");
    }

    private static string GetSinglePrimaryKeyColumn(FormTableDefinition table)
    {
        if (table.PrimaryKey.Count != 1)
            throw new InvalidOperationException($"Table '{table.TableName}' must have exactly one primary key column.");

        return table.PrimaryKey[0];
    }

    private static bool TryGetCaseInsensitive(IReadOnlyDictionary<string, object?> values, string key, out object? value)
    {
        if (values.TryGetValue(key, out value))
            return true;

        string? actualKey = values.Keys.FirstOrDefault(candidate => string.Equals(candidate, key, StringComparison.OrdinalIgnoreCase));
        if (actualKey is not null)
        {
            value = values[actualKey];
            return true;
        }

        value = null;
        return false;
    }
}
