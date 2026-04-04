using CSharpDB.Admin.Forms.Contracts;
using CSharpDB.Admin.Forms.Models;
using CSharpDB.Client;
using System.Globalization;

namespace CSharpDB.Admin.Forms.Services;

public sealed class DbFormRecordService(ICSharpDbClient dbClient) : IFormRecordService
{
    private const int AdjacentLookupPageSize = 1;

    public string GetPrimaryKeyColumn(FormTableDefinition table)
    {
        if (table.PrimaryKey.Count != 1)
            throw new InvalidOperationException($"Table '{table.TableName}' must have exactly one primary key column.");

        return table.PrimaryKey[0];
    }

    public Task<Dictionary<string, object?>?> GetRecordAsync(FormTableDefinition table, object pkValue, CancellationToken ct = default)
        => dbClient.GetRowByPkAsync(table.TableName, GetPrimaryKeyColumn(table), pkValue, ct);

    public async Task<FormRecordWindow?> GetRecordWindowAsync(FormTableDefinition table, object pkValue, int pageSize, CancellationToken ct = default)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(pageSize, 1);

        FormFieldDefinition? primaryKeyField = GetPrimaryKeyField(table);
        if (primaryKeyField is null)
            return await GetRecordWindowFallbackAsync(table, pkValue, pageSize, ct);

        if (!SupportsFocusedNavigationFastPath(primaryKeyField))
            return await GetRecordWindowFallbackAsync(table, pkValue, pageSize, ct);

        string tableName = FormSql.RequireIdentifier(table.TableName, nameof(table.TableName));
        string pkColumn = FormSql.RequireIdentifier(primaryKeyField.Name, "primaryKey");
        string pkLiteral = FormSql.FormatLiteral(pkValue);
        int fetchLimit = pageSize + 1;

        List<Dictionary<string, object?>> beforeDescending = await ExecuteRowsAsync($"""
            SELECT *
            FROM {tableName}
            WHERE {pkColumn} < {pkLiteral}
            ORDER BY {pkColumn} DESC
            LIMIT {fetchLimit};
            """, ct);
        List<Dictionary<string, object?>> anchorAndAfter = await ExecuteRowsAsync($"""
            SELECT *
            FROM {tableName}
            WHERE {pkColumn} >= {pkLiteral}
            ORDER BY {pkColumn}
            LIMIT {fetchLimit};
            """, ct);

        int anchorIndexInAfter = FindRecordIndex(anchorAndAfter, pkColumn, pkValue);
        if (anchorIndexInAfter < 0)
            return await GetRecordWindowFallbackAsync(table, pkValue, pageSize, ct);

        beforeDescending.Reverse();

        var merged = new List<Dictionary<string, object?>>(beforeDescending.Count + anchorAndAfter.Count);
        merged.AddRange(beforeDescending);
        merged.AddRange(anchorAndAfter);

        int anchorIndex = FindRecordIndex(merged, pkColumn, pkValue);
        if (anchorIndex < 0)
            return await GetRecordWindowFallbackAsync(table, pkValue, pageSize, ct);

        int start = Math.Max(0, anchorIndex - (pageSize / 2));
        if (merged.Count - start < pageSize)
            start = Math.Max(0, merged.Count - pageSize);

        List<Dictionary<string, object?>> windowRows = merged
            .Skip(start)
            .Take(pageSize)
            .ToList();
        int selectedIndex = anchorIndex - start;
        int visibleBeforeCount = selectedIndex;
        int visibleAfterCount = windowRows.Count - selectedIndex - 1;
        int totalBeforeCount = anchorIndex;
        int totalAfterCount = merged.Count - anchorIndex - 1;

        return new FormRecordWindow(
            windowRows,
            selectedIndex,
            HasPreviousRecords: totalBeforeCount > visibleBeforeCount,
            HasNextRecords: totalAfterCount > visibleAfterCount);
    }

    public async Task<Dictionary<string, object?>?> GetAdjacentRecordAsync(FormTableDefinition table, object pkValue, bool previous, CancellationToken ct = default)
    {
        FormFieldDefinition? primaryKeyField = GetPrimaryKeyField(table);
        if (primaryKeyField is null)
            return null;

        if (SupportsFocusedNavigationFastPath(primaryKeyField))
        {
            string tableName = FormSql.RequireIdentifier(table.TableName, nameof(table.TableName));
            string pkColumn = FormSql.RequireIdentifier(primaryKeyField.Name, "primaryKey");
            string pkLiteral = FormSql.FormatLiteral(pkValue);
            string op = previous ? "<" : ">";
            string order = previous ? "DESC" : "ASC";

            List<Dictionary<string, object?>> rows = await ExecuteRowsAsync($"""
                SELECT *
                FROM {tableName}
                WHERE {pkColumn} {op} {pkLiteral}
                ORDER BY {pkColumn} {order}
                LIMIT 1;
                """, ct);

            return rows.Count == 0 ? null : rows[0];
        }

        int? ordinal = await GetRecordOrdinalAsync(table, pkValue, ct);
        if (!ordinal.HasValue)
            return null;

        int targetOrdinal = ordinal.Value + (previous ? -1 : 1);
        int totalCount = await GetTotalCountAsync(table, ct);
        if (targetOrdinal < 0 || targetOrdinal >= totalCount)
            return null;

        FormRecordPage page = await ListRecordPageAsync(table, targetOrdinal + 1, AdjacentLookupPageSize, ct);
        return page.Records.Count == 0 ? null : page.Records[0];
    }

    public async Task<FormRecordPage> ListRecordPageAsync(FormTableDefinition table, int pageNumber, int pageSize, CancellationToken ct = default)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(pageNumber, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(pageSize, 1);

        string tableName = FormSql.RequireIdentifier(table.TableName, nameof(table.TableName));
        string orderBySql = BuildOrderBySql(table);
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
            {orderBySql}
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
        string whereClause = BuildTextSearchWhereClause(table, searchField, searchValue);
        string orderBySql = BuildOrderBySql(table);
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
            {orderBySql}
            LIMIT {pageSize}
            OFFSET {offset};
            """, ct);

        return new FormRecordPage(effectivePage, pageSize, totalCount, rows);
    }

    public Task<List<Dictionary<string, object?>>> ListRecordsAsync(FormTableDefinition table, CancellationToken ct = default)
        => ExecuteRowsAsync($"""
            SELECT *
            FROM {FormSql.RequireIdentifier(table.TableName, nameof(table.TableName))}
            {BuildOrderBySql(table)};
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
            {BuildOrderBySql(table)};
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

    private async Task<FormRecordWindow?> GetRecordWindowFallbackAsync(FormTableDefinition table, object pkValue, int pageSize, CancellationToken ct)
    {
        int? ordinal = await GetRecordOrdinalAsync(table, pkValue, ct);
        if (!ordinal.HasValue)
            return null;

        int targetPage = (ordinal.Value / pageSize) + 1;
        FormRecordPage page = await ListRecordPageAsync(table, targetPage, pageSize, ct);
        string pkColumn = GetPrimaryKeyColumn(table);
        int selectedIndex = FindRecordIndex(page.Records, pkColumn, pkValue);
        if (selectedIndex < 0)
            return null;

        int visibleBeforeCount = selectedIndex;
        int visibleAfterCount = page.Records.Count - selectedIndex - 1;
        int totalBeforeCount = ordinal.Value;
        int totalAfterCount = page.TotalCount - ordinal.Value - 1;

        return new FormRecordWindow(
            page.Records.ToList(),
            selectedIndex,
            HasPreviousRecords: totalBeforeCount > visibleBeforeCount,
            HasNextRecords: totalAfterCount > visibleAfterCount);
    }

    private async Task<int> GetTotalCountAsync(FormTableDefinition table, CancellationToken ct)
    {
        string tableName = FormSql.RequireIdentifier(table.TableName, nameof(table.TableName));
        return await ExecuteCountAsync($"""
            SELECT COUNT(*) AS RowCount
            FROM {tableName};
            """, ct);
    }

    private static string BuildTextSearchWhereClause(FormTableDefinition table, string searchField, string searchValue)
    {
        RequireField(table, searchField);
        string columnName = FormSql.RequireIdentifier(searchField, nameof(searchField));
        string searchPattern = $"%{FormSql.EscapeLikePattern(searchValue)}%";
        return $"TEXT({columnName}) LIKE {FormSql.FormatLiteral(searchPattern)} ESCAPE '!'";
    }

    private static FormFieldDefinition? GetPrimaryKeyField(FormTableDefinition table)
    {
        if (table.PrimaryKey.Count != 1)
            return null;

        string primaryKey = table.PrimaryKey[0];
        return table.Fields.FirstOrDefault(field => string.Equals(field.Name, primaryKey, StringComparison.OrdinalIgnoreCase));
    }

    private static bool SupportsFocusedNavigationFastPath(FormFieldDefinition primaryKeyField)
        => primaryKeyField.DataType is FieldDataType.Int64 or FieldDataType.String;

    private static string BuildOrderBySql(FormTableDefinition table)
    {
        string clause = BuildOrderByClause(table);
        return string.IsNullOrWhiteSpace(clause)
            ? string.Empty
            : $"ORDER BY {clause}";
    }

    private static string BuildOrderByClause(FormTableDefinition table)
    {
        if (table.HasSinglePrimaryKey)
            return FormSql.RequireIdentifier(table.PrimaryKey[0], "primaryKey");

        string[] columns = table.Fields
            .Where(field => field.DataType != FieldDataType.Blob)
            .Select(field => FormSql.RequireIdentifier(field.Name, nameof(field.Name)))
            .ToArray();

        if (columns.Length == 0)
        {
            columns = table.Fields
                .Select(field => FormSql.RequireIdentifier(field.Name, nameof(field.Name)))
                .ToArray();
        }

        return string.Join(", ", columns);
    }

    private static int FindRecordIndex(IReadOnlyList<Dictionary<string, object?>> records, string pkColumn, object pkValue)
    {
        for (int i = 0; i < records.Count; i++)
        {
            if (!TryGetCaseInsensitive(records[i], pkColumn, out object? candidate))
                continue;

            if (AreValuesEqual(candidate, pkValue))
                return i;
        }

        return -1;
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

    private static bool AreValuesEqual(object? left, object? right)
        => Equals(FormSql.NormalizeValue(left), FormSql.NormalizeValue(right));
}
