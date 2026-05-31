using System.Globalization;
using CSharpDB.Admin.Models;
using CSharpDB.Client;
using CSharpDB.Client.Models;

namespace CSharpDB.Admin.Services;

public sealed class DataHygieneAdminService(ICSharpDbClient client)
{
    public async Task<IReadOnlyList<string>> GetUserTableNamesAsync(CancellationToken ct = default)
    {
        IReadOnlyList<string> names = await client.GetTableNamesAsync(ct);
        return names
            .Where(static name => !IsSystemTableName(name))
            .OrderBy(static name => name, StringComparer.OrdinalIgnoreCase)
            .ToArray();
    }

    public Task<TableSchema?> GetTableSchemaAsync(string tableName, CancellationToken ct = default)
        => client.GetTableSchemaAsync(tableName, ct);

    public async Task<IReadOnlyList<DataHygieneForeignKeyOption>> GetForeignKeyOptionsAsync(
        string? tableName = null,
        CancellationToken ct = default)
    {
        var options = new List<DataHygieneForeignKeyOption>();
        IReadOnlyList<string> tableNames = string.IsNullOrWhiteSpace(tableName)
            ? await GetUserTableNamesAsync(ct)
            : [tableName.Trim()];

        foreach (string currentTable in tableNames)
        {
            TableSchema? schema = await client.GetTableSchemaAsync(currentTable, ct);
            if (schema is null)
                continue;

            options.AddRange(schema.ForeignKeys.Select(foreignKey => new DataHygieneForeignKeyOption(
                foreignKey.ConstraintName,
                schema.TableName,
                foreignKey.ColumnName,
                foreignKey.ReferencedTableName,
                foreignKey.ReferencedColumnName)));
        }

        return options
            .OrderBy(option => option.ChildTable, StringComparer.OrdinalIgnoreCase)
            .ThenBy(option => option.ConstraintName, StringComparer.OrdinalIgnoreCase)
            .ToArray();
    }

    public async Task<DataHygieneResultSet<DataHygieneDuplicateGroup>> FindDuplicatesAsync(
        string tableName,
        string keyExpression,
        CancellationToken ct = default)
    {
        string sql = BuildFindDuplicatesSql(tableName, keyExpression);
        SqlExecutionResult result = await ExecuteAsync(sql, ct);
        return new DataHygieneResultSet<DataHygieneDuplicateGroup>(
            sql,
            result.Elapsed,
            MapRows(result, row => new DataHygieneDuplicateGroup(
                GetString(row, "key_values"),
                GetInt64(row, "group_size"),
                GetString(row, "winner_rowid"),
                GetString(row, "winner_primary_key"),
                GetString(row, "duplicate_rowids"),
                GetString(row, "duplicate_primary_keys"))),
            result.RowsAffected);
    }

    public async Task<DataHygieneResultSet<DataHygieneMutationSummary>> DedupAsync(
        string tableName,
        string keyExpression,
        DataHygieneKeepMode keepMode,
        CancellationToken ct = default)
    {
        string sql = BuildDedupSql(tableName, keyExpression, keepMode);
        SqlExecutionResult result = await ExecuteAsync(sql, ct);
        return new DataHygieneResultSet<DataHygieneMutationSummary>(
            sql,
            result.Elapsed,
            MapRows(result, row => MapMutationSummary("DEDUP", row)),
            result.RowsAffected);
    }

    public async Task<DataHygieneResultSet<DataHygieneMutationSummary>> MergeDuplicatesAsync(
        string tableName,
        string keyExpression,
        CancellationToken ct = default)
    {
        string sql = BuildMergeDuplicatesSql(tableName, keyExpression);
        SqlExecutionResult result = await ExecuteAsync(sql, ct);
        return new DataHygieneResultSet<DataHygieneMutationSummary>(
            sql,
            result.Elapsed,
            MapRows(result, row => MapMutationSummary("MERGE DUPLICATES", row)),
            result.RowsAffected);
    }

    public async Task<DataHygieneResultSet<DataHygieneValidationRuleRow>> ListValidationRulesAsync(
        CancellationToken ct = default)
    {
        const string sql = """
            SELECT rule_name, table_name, column_name, expression_sql, message, created_utc, is_enabled
            FROM sys.validation_rules
            ORDER BY table_name, column_name, rule_name;
            """;

        SqlExecutionResult result = await ExecuteAsync(sql, ct);
        return new DataHygieneResultSet<DataHygieneValidationRuleRow>(
            sql,
            result.Elapsed,
            MapRows(result, row => new DataHygieneValidationRuleRow(
                GetString(row, "rule_name") ?? string.Empty,
                GetString(row, "table_name") ?? string.Empty,
                GetString(row, "column_name"),
                GetString(row, "expression_sql") ?? string.Empty,
                GetString(row, "message") ?? string.Empty,
                GetString(row, "created_utc"),
                GetBoolean(row, "is_enabled"))),
            result.RowsAffected);
    }

    public async Task<DataHygieneResultSet<DataHygieneMutationSummary>> CreateValidationRuleAsync(
        string ruleName,
        string tableName,
        string? columnName,
        string expressionSql,
        string message,
        CancellationToken ct = default)
    {
        string sql = BuildCreateValidationRuleSql(ruleName, tableName, columnName, expressionSql, message);
        SqlExecutionResult result = await ExecuteAsync(sql, ct);
        return new DataHygieneResultSet<DataHygieneMutationSummary>(
            sql,
            result.Elapsed,
            [new DataHygieneMutationSummary("CREATE VALIDATION RULE", tableName.Trim(), 0, 0, 0, 0, 0, null)],
            result.RowsAffected);
    }

    public async Task<DataHygieneResultSet<DataHygieneValidationViolation>> ValidateTableAsync(
        string tableName,
        CancellationToken ct = default)
    {
        string sql = BuildValidateTableSql(tableName);
        SqlExecutionResult result = await ExecuteAsync(sql, ct);
        return new DataHygieneResultSet<DataHygieneValidationViolation>(
            sql,
            result.Elapsed,
            MapRows(result, row => new DataHygieneValidationViolation(
                GetString(row, "rule_name") ?? string.Empty,
                GetString(row, "table_name") ?? string.Empty,
                GetString(row, "column_name"),
                GetString(row, "rowid"),
                GetString(row, "primary_key"),
                GetString(row, "message") ?? string.Empty)),
            result.RowsAffected);
    }

    public async Task<DataHygieneResultSet<DataHygieneOrphanRow>> FindOrphansInTableAsync(
        string tableName,
        CancellationToken ct = default)
    {
        string sql = BuildFindOrphansInTableSql(tableName);
        SqlExecutionResult result = await ExecuteAsync(sql, ct);
        return MapOrphanResult(sql, result);
    }

    public async Task<DataHygieneResultSet<DataHygieneOrphanRow>> FindOrphansExplicitAsync(
        string childTable,
        string childColumn,
        string parentTable,
        string parentColumn,
        CancellationToken ct = default)
    {
        string sql = BuildFindOrphansExplicitSql(childTable, childColumn, parentTable, parentColumn);
        SqlExecutionResult result = await ExecuteAsync(sql, ct);
        return MapOrphanResult(sql, result);
    }

    public static string BuildFindDuplicatesSql(string tableName, string keyExpression)
        => $"FIND DUPLICATES IN {FormatIdentifier(tableName)} ON {FormatExpressionList(keyExpression)};";

    public static string BuildDedupSql(string tableName, string keyExpression, DataHygieneKeepMode keepMode)
        => $"DEDUP {FormatIdentifier(tableName)} ON {FormatExpressionList(keyExpression)} KEEP {keepMode.ToString().ToUpperInvariant()};";

    public static string BuildMergeDuplicatesSql(string tableName, string keyExpression)
        => $"MERGE DUPLICATES {FormatIdentifier(tableName)} ON {FormatExpressionList(keyExpression)};";

    public static string BuildCreateValidationRuleSql(
        string ruleName,
        string tableName,
        string? columnName,
        string expressionSql,
        string message)
    {
        string target = FormatIdentifier(tableName);
        if (!string.IsNullOrWhiteSpace(columnName))
            target += "." + FormatIdentifier(columnName);

        return "CREATE VALIDATION RULE "
            + FormatIdentifier(ruleName)
            + " ON "
            + target
            + " AS "
            + FormatExpression(expressionSql, "Validation expression")
            + " MESSAGE "
            + FormatStringLiteral(message)
            + ";";
    }

    public static string BuildValidateTableSql(string tableName)
        => $"VALIDATE TABLE {FormatIdentifier(tableName)};";

    public static string BuildFindOrphansInTableSql(string tableName)
        => $"FIND ORPHANS IN {FormatIdentifier(tableName)};";

    public static string BuildFindOrphansExplicitSql(
        string childTable,
        string childColumn,
        string parentTable,
        string parentColumn)
        => $"FIND ORPHANS IN {FormatIdentifier(childTable)}.{FormatIdentifier(childColumn)} REFERENCES {FormatIdentifier(parentTable)}.{FormatIdentifier(parentColumn)};";

    public static string FormatIdentifier(string identifier)
    {
        if (string.IsNullOrWhiteSpace(identifier))
            throw new InvalidOperationException("Identifier is required.");

        string trimmed = identifier.Trim();
        if (!IsSqlIdentifier(trimmed))
            throw new InvalidOperationException($"'{trimmed}' is not a valid CSharpDB identifier.");

        return trimmed;
    }

    public static string FormatStringLiteral(string value)
        => "'" + (value ?? string.Empty).Replace("'", "''", StringComparison.Ordinal) + "'";

    private async Task<SqlExecutionResult> ExecuteAsync(string sql, CancellationToken ct)
    {
        SqlExecutionResult result = await client.ExecuteSqlAsync(sql, ct);
        if (!string.IsNullOrWhiteSpace(result.Error))
            throw new InvalidOperationException(result.Error);

        return result;
    }

    private static DataHygieneResultSet<DataHygieneOrphanRow> MapOrphanResult(string sql, SqlExecutionResult result)
        => new(
            sql,
            result.Elapsed,
            MapRows(result, row => new DataHygieneOrphanRow(
                GetString(row, "constraint_name"),
                GetString(row, "child_table") ?? string.Empty,
                GetString(row, "child_column") ?? string.Empty,
                GetString(row, "child_rowid"),
                GetString(row, "child_value"),
                GetString(row, "parent_table") ?? string.Empty,
                GetString(row, "parent_column") ?? string.Empty)),
            result.RowsAffected);

    private static DataHygieneMutationSummary MapMutationSummary(string operation, ResultRow row)
        => new(
            operation,
            GetString(row, "table_name") ?? string.Empty,
            GetInt64(row, "duplicate_group_count"),
            GetInt64(row, "rows_deleted"),
            GetInt64(row, "rows_kept"),
            GetInt64(row, "rows_updated"),
            GetInt64(row, "merge_conflict_count"),
            GetString(row, "merge_conflicts"));

    private static IReadOnlyList<T> MapRows<T>(SqlExecutionResult result, Func<ResultRow, T> map)
    {
        if (result.Rows is null || result.ColumnNames is null)
            return [];

        var index = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < result.ColumnNames.Length; i++)
            index[result.ColumnNames[i]] = i;

        var mapped = new List<T>(result.Rows.Count);
        foreach (object?[] values in result.Rows)
            mapped.Add(map(new ResultRow(index, values)));

        return mapped;
    }

    private static string? GetString(ResultRow row, string columnName)
    {
        object? value = row.Get(columnName);
        return FormatValue(value);
    }

    private static long GetInt64(ResultRow row, string columnName)
    {
        object? value = row.Get(columnName);
        if (value is null)
            return 0;

        return value switch
        {
            long longValue => longValue,
            int intValue => intValue,
            short shortValue => shortValue,
            byte byteValue => byteValue,
            decimal decimalValue => (long)decimalValue,
            double doubleValue => (long)doubleValue,
            float floatValue => (long)floatValue,
            _ when long.TryParse(Convert.ToString(value, CultureInfo.InvariantCulture), NumberStyles.Integer, CultureInfo.InvariantCulture, out long parsed) => parsed,
            _ => 0,
        };
    }

    private static bool GetBoolean(ResultRow row, string columnName)
    {
        object? value = row.Get(columnName);
        if (value is null)
            return false;

        return value switch
        {
            bool boolean => boolean,
            long longValue => longValue != 0,
            int intValue => intValue != 0,
            string text => string.Equals(text, "true", StringComparison.OrdinalIgnoreCase)
                || string.Equals(text, "1", StringComparison.OrdinalIgnoreCase)
                || string.Equals(text, "yes", StringComparison.OrdinalIgnoreCase),
            _ => false,
        };
    }

    private static string? FormatValue(object? value) => value switch
    {
        null => null,
        byte[] bytes => "0x" + Convert.ToHexString(bytes),
        IFormattable formattable => formattable.ToString(null, CultureInfo.InvariantCulture),
        _ => value.ToString(),
    };

    private static string FormatExpressionList(string expressionList)
        => FormatExpression(expressionList, "Duplicate key expression");

    private static string FormatExpression(string expression, string label)
    {
        if (string.IsNullOrWhiteSpace(expression))
            throw new InvalidOperationException($"{label} is required.");

        string trimmed = expression.Trim();
        if (ContainsStatementSeparatorOutsideString(trimmed))
            throw new InvalidOperationException($"{label} cannot contain statement separators.");

        return trimmed;
    }

    private static bool IsSqlIdentifier(string value)
    {
        if (value.Length == 0 || !(char.IsLetter(value[0]) || value[0] == '_'))
            return false;

        for (int i = 1; i < value.Length; i++)
        {
            char c = value[i];
            if (!char.IsLetterOrDigit(c) && c != '_')
                return false;
        }

        return true;
    }

    private static bool ContainsStatementSeparatorOutsideString(string value)
    {
        bool inString = false;
        for (int i = 0; i < value.Length; i++)
        {
            char c = value[i];
            if (c == '\'')
            {
                if (inString && i + 1 < value.Length && value[i + 1] == '\'')
                {
                    i++;
                    continue;
                }

                inString = !inString;
            }
            else if (c == ';' && !inString)
            {
                return true;
            }
        }

        return false;
    }

    private static bool IsSystemTableName(string name)
        => name.StartsWith("_", StringComparison.Ordinal)
        || name.StartsWith("sys.", StringComparison.OrdinalIgnoreCase);

    private readonly record struct ResultRow(IReadOnlyDictionary<string, int> ColumnIndex, object?[] Values)
    {
        public object? Get(string columnName)
            => ColumnIndex.TryGetValue(columnName, out int index) && index >= 0 && index < Values.Length
                ? Values[index]
                : null;
    }
}
