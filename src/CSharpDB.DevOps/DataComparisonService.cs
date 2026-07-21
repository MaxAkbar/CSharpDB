using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using CSharpDB.Client.Models;

namespace CSharpDB.DevOps;

public sealed class DataComparisonService
{
    public async Task<DataDiffReport> CompareAsync(
        IDataCompareTarget source,
        IDataCompareTarget target,
        DataCompareOptions options,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(target);
        ArgumentNullException.ThrowIfNull(options);

        if (string.IsNullOrWhiteSpace(options.TableName))
            throw new ArgumentException("Data compare requires a table name.", nameof(options));

        TableSchema sourceSchema = await source.GetTableSchemaAsync(options.TableName, ct)
            ?? throw new InvalidOperationException($"Source table '{options.TableName}' was not found.");
        TableSchema targetSchema = await target.GetTableSchemaAsync(options.TableName, ct)
            ?? throw new InvalidOperationException($"Target table '{options.TableName}' was not found.");

        IReadOnlyList<string> keyColumns = ResolveKeyColumns(sourceSchema, options.KeyColumns);
        ValidateCompatibleSchemas(sourceSchema, targetSchema, keyColumns);
        bool hasRowVersion = sourceSchema.Columns.Any(static column => column.IsRowVersion);

        Dictionary<RowKey, IReadOnlyDictionary<string, object?>> sourceRows =
            await LoadRowsByKeyAsync(source, sourceSchema, keyColumns, ct);
        Dictionary<RowKey, IReadOnlyDictionary<string, object?>> targetRows =
            await LoadRowsByKeyAsync(target, targetSchema, keyColumns, ct);

        var changes = new List<DataDiffRow>();
        int sourceOnly = 0;
        int targetOnly = 0;
        int changed = 0;
        int compared = 0;
        int maxPreviewRows = Math.Max(0, options.MaxPreviewRows);

        foreach (var (key, sourceRow) in sourceRows.OrderBy(kvp => kvp.Key.Display, StringComparer.Ordinal))
        {
            if (!targetRows.TryGetValue(key, out IReadOnlyDictionary<string, object?>? targetRow))
            {
                sourceOnly++;
                AddPreview(changes, maxPreviewRows, new DataDiffRow
                {
                    ChangeKind = DataChangeKind.SourceOnly,
                    Key = key.Values,
                    SourceValues = sourceRow,
                });
                continue;
            }

            compared++;
            IReadOnlyList<string> changedColumns = GetChangedColumns(sourceSchema, sourceRow, targetRow, keyColumns);
            if (changedColumns.Count == 0)
                continue;

            changed++;
            AddPreview(changes, maxPreviewRows, new DataDiffRow
            {
                ChangeKind = DataChangeKind.Changed,
                Key = key.Values,
                SourceValues = sourceRow,
                TargetValues = targetRow,
                ChangedColumns = changedColumns,
            });
        }

        foreach (var (key, targetRow) in targetRows.OrderBy(kvp => kvp.Key.Display, StringComparer.Ordinal))
        {
            if (sourceRows.ContainsKey(key))
                continue;

            targetOnly++;
            AddPreview(changes, maxPreviewRows, new DataDiffRow
            {
                ChangeKind = DataChangeKind.TargetOnly,
                Key = key.Values,
                TargetValues = targetRow,
            });
        }

        return new DataDiffReport
        {
            Source = source.Descriptor,
            Target = target.Descriptor,
            TableName = sourceSchema.TableName,
            KeyColumns = keyColumns,
            Rows = changes,
            Warnings = hasRowVersion
                ? ["ROWVERSION columns are store-generated. Token values are excluded from comparison and sync SQL, and target tokens will be regenerated."]
                : [],
            Summary = new DataDiffSummary
            {
                SourceRowCount = sourceRows.Count,
                TargetRowCount = targetRows.Count,
                ComparedRowCount = compared,
                SourceOnlyRows = sourceOnly,
                TargetOnlyRows = targetOnly,
                ChangedRows = changed,
                PreviewRowCount = changes.Count,
            },
        };
    }

    public string RenderSyncScript(DataDiffReport report)
    {
        ArgumentNullException.ThrowIfNull(report);
        if (report.KeyColumns.Count == 0)
            throw new InvalidOperationException("Data sync script generation requires stable key columns.");

        var script = new StringBuilder();
        script.AppendLine("-- CSharpDB data sync preview");
        script.AppendLine($"-- Source: {report.Source.DisplayName}");
        script.AppendLine($"-- Target: {report.Target.DisplayName}");
        script.AppendLine($"-- Table: {report.TableName}");
        script.AppendLine($"-- Generated UTC: {report.GeneratedUtc:O}");
        script.AppendLine("-- Review before executing this script.");
        foreach (string warning in report.Warnings)
            script.AppendLine($"-- WARNING: {warning}");
        script.AppendLine();

        foreach (DataDiffRow row in report.Rows)
        {
            switch (row.ChangeKind)
            {
                case DataChangeKind.SourceOnly:
                    script.AppendLine(RenderInsert(report.TableName, row.SourceValues ?? throw new InvalidOperationException("Missing source row.")));
                    break;
                case DataChangeKind.TargetOnly:
                    script.AppendLine(RenderDelete(report.TableName, report.KeyColumns, row.Key));
                    break;
                case DataChangeKind.Changed:
                    script.AppendLine(RenderUpdate(
                        report.TableName,
                        report.KeyColumns,
                        row.Key,
                        row.SourceValues ?? throw new InvalidOperationException("Missing source row."),
                        row.ChangedColumns));
                    break;
            }
        }

        if (report.Rows.Count < report.Summary.SourceOnlyRows + report.Summary.TargetOnlyRows + report.Summary.ChangedRows)
        {
            script.AppendLine();
            script.AppendLine("-- The report contains a preview only. Re-run with a larger preview limit before using this script.");
        }

        if (report.Summary.SourceOnlyRows + report.Summary.TargetOnlyRows + report.Summary.ChangedRows == 0)
            script.AppendLine("-- No data changes detected.");

        return script.ToString();
    }

    private static IReadOnlyList<string> ResolveKeyColumns(TableSchema schema, IReadOnlyList<string> requestedKeys)
    {
        if (requestedKeys.Count > 0)
        {
            foreach (string key in requestedKeys)
            {
                ColumnDefinition? keyColumn = schema.Columns.FirstOrDefault(
                    column => column.Name.Equals(key, StringComparison.OrdinalIgnoreCase));
                if (keyColumn is null)
                    throw new InvalidOperationException($"Key column '{key}' was not found on table '{schema.TableName}'.");
                if (keyColumn.IsRowVersion)
                    throw new InvalidOperationException($"ROWVERSION column '{keyColumn.Name}' cannot be used as a data compare key because its value is store-generated.");
            }

            return requestedKeys.Select(key => ResolveColumnName(schema, key)).ToArray();
        }

        ColumnDefinition? primaryKey = schema.Columns.FirstOrDefault(column => column.IsPrimaryKey);
        if (primaryKey is null)
            throw new InvalidOperationException($"Table '{schema.TableName}' has no primary key. Supply --key <columns>.");
        if (primaryKey.IsRowVersion)
            throw new InvalidOperationException($"ROWVERSION column '{primaryKey.Name}' cannot be used as a data compare key because its value is store-generated.");

        return [primaryKey.Name];
    }

    private static void ValidateCompatibleSchemas(TableSchema source, TableSchema target, IReadOnlyList<string> keyColumns)
    {
        string[] sourceRowVersions = source.Columns
            .Where(static column => column.IsRowVersion)
            .Select(static column => column.Name)
            .OrderBy(static name => name, StringComparer.OrdinalIgnoreCase)
            .ToArray();
        string[] targetRowVersions = target.Columns
            .Where(static column => column.IsRowVersion)
            .Select(static column => column.Name)
            .OrderBy(static name => name, StringComparer.OrdinalIgnoreCase)
            .ToArray();
        if (!sourceRowVersions.SequenceEqual(targetRowVersions, StringComparer.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException(
                $"ROWVERSION columns differ between source and target table '{source.TableName}'. Run schema compare first.");
        }

        foreach (ColumnDefinition sourceColumn in source.Columns)
        {
            ColumnDefinition? targetColumn = target.Columns.FirstOrDefault(column => column.Name.Equals(sourceColumn.Name, StringComparison.OrdinalIgnoreCase));
            if (targetColumn is null)
                throw new InvalidOperationException($"Target table '{target.TableName}' is missing source column '{sourceColumn.Name}'. Run schema compare first.");
            if (targetColumn.Type != sourceColumn.Type)
                throw new InvalidOperationException($"Column '{sourceColumn.Name}' type differs between source and target. Run schema compare first.");
            if (targetColumn.IsRowVersion != sourceColumn.IsRowVersion)
                throw new InvalidOperationException($"Column '{sourceColumn.Name}' ROWVERSION metadata differs between source and target. Run schema compare first.");
        }

        foreach (string keyColumn in keyColumns)
        {
            if (!target.Columns.Any(column => column.Name.Equals(keyColumn, StringComparison.OrdinalIgnoreCase)))
                throw new InvalidOperationException($"Target table '{target.TableName}' is missing key column '{keyColumn}'.");
        }
    }

    private static async Task<Dictionary<RowKey, IReadOnlyDictionary<string, object?>>> LoadRowsByKeyAsync(
        IDataCompareTarget target,
        TableSchema schema,
        IReadOnlyList<string> keyColumns,
        CancellationToken ct)
    {
        var rows = new Dictionary<RowKey, IReadOnlyDictionary<string, object?>>();
        var duplicateKeys = new HashSet<RowKey>();

        await foreach (IReadOnlyDictionary<string, object?> row in target.ReadRowsAsync(schema, ct))
        {
            RowKey key = RowKey.Create(schema, keyColumns, row);
            IReadOnlyDictionary<string, object?> comparableRow = ExcludeRowVersionValues(schema, row);
            if (!rows.TryAdd(key, comparableRow))
                duplicateKeys.Add(key);
        }

        if (duplicateKeys.Count > 0)
            throw new InvalidOperationException($"Data compare found duplicate key value(s) for table '{schema.TableName}'. Choose a stable unique --key.");

        return rows;
    }

    private static IReadOnlyList<string> GetChangedColumns(
        TableSchema schema,
        IReadOnlyDictionary<string, object?> sourceRow,
        IReadOnlyDictionary<string, object?> targetRow,
        IReadOnlyList<string> keyColumns)
    {
        var changed = new List<string>();
        foreach (ColumnDefinition column in schema.Columns)
        {
            if (column.IsRowVersion || keyColumns.Contains(column.Name, StringComparer.OrdinalIgnoreCase))
                continue;

            object? sourceValue = GetValue(sourceRow, column.Name);
            object? targetValue = GetValue(targetRow, column.Name);
            if (!ValueEquals(sourceValue, targetValue, column.Collation))
                changed.Add(column.Name);
        }

        return changed;
    }

    private static IReadOnlyDictionary<string, object?> ExcludeRowVersionValues(
        TableSchema schema,
        IReadOnlyDictionary<string, object?> row)
    {
        var values = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        foreach (ColumnDefinition column in schema.Columns)
        {
            if (!column.IsRowVersion)
                values[column.Name] = GetValue(row, column.Name);
        }

        return values;
    }

    private static string RenderInsert(string tableName, IReadOnlyDictionary<string, object?> row)
    {
        string columns = string.Join(", ", row.Keys.Select(SchemaScriptRenderer.Identifier));
        string values = string.Join(", ", row.Values.Select(SqlLiteral));
        return $"INSERT INTO {SchemaScriptRenderer.Identifier(tableName)} ({columns}) VALUES ({values});";
    }

    private static string RenderDelete(string tableName, IReadOnlyList<string> keyColumns, IReadOnlyDictionary<string, object?> key)
        => $"DELETE FROM {SchemaScriptRenderer.Identifier(tableName)} WHERE {RenderPredicate(keyColumns, key)};";

    private static string RenderUpdate(
        string tableName,
        IReadOnlyList<string> keyColumns,
        IReadOnlyDictionary<string, object?> key,
        IReadOnlyDictionary<string, object?> sourceRow,
        IReadOnlyList<string> changedColumns)
    {
        string setClause = string.Join(
            ", ",
            changedColumns.Select(column => $"{SchemaScriptRenderer.Identifier(column)} = {SqlLiteral(GetValue(sourceRow, column))}"));

        return $"UPDATE {SchemaScriptRenderer.Identifier(tableName)} SET {setClause} WHERE {RenderPredicate(keyColumns, key)};";
    }

    private static string RenderPredicate(IReadOnlyList<string> keyColumns, IReadOnlyDictionary<string, object?> key)
        => string.Join(
            " AND ",
            keyColumns.Select(column =>
            {
                object? value = GetValue(key, column);
                return value is null
                    ? $"{SchemaScriptRenderer.Identifier(column)} IS NULL"
                    : $"{SchemaScriptRenderer.Identifier(column)} = {SqlLiteral(value)}";
            }));

    internal static string SqlLiteral(object? value) => value switch
    {
        null => "NULL",
        byte[] bytes => $"X'{Convert.ToHexString(bytes)}'",
        string text => $"'{text.Replace("'", "''", StringComparison.Ordinal)}'",
        bool boolean => boolean ? "1" : "0",
        int or long or short or byte => Convert.ToString(value, CultureInfo.InvariantCulture) ?? "NULL",
        float or double or decimal => Convert.ToString(value, CultureInfo.InvariantCulture) ?? "NULL",
        DateTime dateTime => $"'{dateTime.ToString("O", CultureInfo.InvariantCulture).Replace("'", "''", StringComparison.Ordinal)}'",
        _ => $"'{Convert.ToString(value, CultureInfo.InvariantCulture)?.Replace("'", "''", StringComparison.Ordinal)}'",
    };

    private static object? GetValue(IReadOnlyDictionary<string, object?> row, string columnName)
    {
        foreach (var (key, value) in row)
        {
            if (key.Equals(columnName, StringComparison.OrdinalIgnoreCase))
                return value;
        }

        return null;
    }

    private static string ResolveColumnName(TableSchema schema, string requestedName)
        => schema.Columns.First(column => column.Name.Equals(requestedName, StringComparison.OrdinalIgnoreCase)).Name;

    private static void AddPreview(List<DataDiffRow> changes, int maxPreviewRows, DataDiffRow row)
    {
        if (changes.Count < maxPreviewRows)
            changes.Add(row);
    }

    private static bool ValueEquals(object? left, object? right, string? collation)
    {
        if (left is null || right is null)
            return left is null && right is null;

        if (left is byte[] leftBytes && right is byte[] rightBytes)
            return leftBytes.AsSpan().SequenceEqual(rightBytes);

        if (left is string leftText && right is string rightText)
            return string.Equals(
                leftText,
                rightText,
                string.Equals(collation, "NOCASE", StringComparison.OrdinalIgnoreCase)
                    ? StringComparison.OrdinalIgnoreCase
                    : StringComparison.Ordinal);

        return Equals(NormalizeNumber(left), NormalizeNumber(right));
    }

    private static object NormalizeNumber(object value)
        => value switch
        {
            int integer => (long)integer,
            short integer => (long)integer,
            byte integer => (long)integer,
            float real => (double)real,
            decimal real => (double)real,
            _ => value,
        };

    private readonly struct RowKey : IEquatable<RowKey>
    {
        private RowKey(string display, IReadOnlyDictionary<string, object?> values)
        {
            Display = display;
            Values = values;
        }

        public string Display { get; }
        public IReadOnlyDictionary<string, object?> Values { get; }

        public static RowKey Create(TableSchema schema, IReadOnlyList<string> keyColumns, IReadOnlyDictionary<string, object?> row)
        {
            var values = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
            var display = new StringBuilder();
            foreach (string keyColumn in keyColumns)
            {
                object? value = GetValue(row, keyColumn);
                string? collation = schema.Columns
                    .FirstOrDefault(column => column.Name.Equals(keyColumn, StringComparison.OrdinalIgnoreCase))
                    ?.Collation;
                values[keyColumn] = value;
                if (display.Length > 0)
                    display.Append('|');
                display.Append(keyColumn).Append('=').Append(KeyPart(value, collation));
            }

            return new RowKey(display.ToString(), values);
        }

        public bool Equals(RowKey other)
            => string.Equals(Display, other.Display, StringComparison.Ordinal);

        public override bool Equals(object? obj)
            => obj is RowKey other && Equals(other);

        public override int GetHashCode()
            => StringComparer.Ordinal.GetHashCode(Display);

        private static string KeyPart(object? value, string? collation)
        {
            if (value is null)
                return "<NULL>";
            if (value is byte[] bytes)
                return Convert.ToHexString(SHA256.HashData(bytes));
            if (value is string text && string.Equals(collation, "NOCASE", StringComparison.OrdinalIgnoreCase))
                return text.ToUpperInvariant();
            return Convert.ToString(NormalizeNumber(value), CultureInfo.InvariantCulture) ?? string.Empty;
        }
    }
}
