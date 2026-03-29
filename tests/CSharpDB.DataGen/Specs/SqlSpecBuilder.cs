using System.Globalization;
using System.Text.Json;
using CSharpDB.Primitives;

namespace CSharpDB.DataGen.Specs;

public static class SqlSpecBuilder
{
    public static string BuildSchemaScript(IEnumerable<SqlTableSpec> tables, bool includeIndexes)
    {
        var lines = new List<string>();
        foreach (SqlTableSpec table in tables)
        {
            string columns = string.Join(", ", table.Columns.Select(BuildColumnSql));
            lines.Add($"CREATE TABLE {table.Name} ({columns});");
        }

        if (!includeIndexes)
            return string.Join(Environment.NewLine, lines);

        List<string> indexes = tables
            .SelectMany(static table => table.Indexes.Select(index => BuildIndexSql(table, index)))
            .ToList();

        if (indexes.Count > 0)
        {
            lines.Add(string.Empty);
            lines.AddRange(indexes);
        }

        return string.Join(Environment.NewLine, lines);
    }

    public static DbValue[] BuildDbValues(
        SqlTableSpec table,
        IReadOnlyDictionary<string, object?> row)
    {
        var values = new DbValue[table.Columns.Count];
        for (int i = 0; i < table.Columns.Count; i++)
        {
            SqlColumnSpec column = table.Columns[i];
            string sourceField = string.IsNullOrWhiteSpace(column.SourceField) ? column.Name : column.SourceField;
            row.TryGetValue(sourceField, out object? rawValue);
            values[i] = ConvertToDbValue(column, rawValue);
        }

        return values;
    }

    public static IReadOnlyList<string> GetCsvHeaders(SqlTableSpec table)
        => table.Columns.Select(static column => column.Name).ToArray();

    public static IReadOnlyList<string> GetCsvValues(SqlTableSpec table, IReadOnlyDictionary<string, object?> row)
    {
        var values = new string[table.Columns.Count];
        for (int i = 0; i < table.Columns.Count; i++)
        {
            SqlColumnSpec column = table.Columns[i];
            string sourceField = string.IsNullOrWhiteSpace(column.SourceField) ? column.Name : column.SourceField;
            row.TryGetValue(sourceField, out object? rawValue);
            values[i] = FormatCsvValue(rawValue);
        }

        return values;
    }

    private static string BuildColumnSql(SqlColumnSpec column)
    {
        var parts = new List<string>
        {
            column.Name,
            NormalizeType(column.Type),
        };

        if (column.PrimaryKey)
            parts.Add("PRIMARY KEY");
        else if (!column.Nullable)
            parts.Add("NOT NULL");

        return string.Join(" ", parts);
    }

    private static string BuildIndexSql(SqlTableSpec table, SqlIndexSpec index)
    {
        string unique = index.Unique ? "UNIQUE " : string.Empty;
        string columns = string.Join(", ", index.Columns);
        return $"CREATE {unique}INDEX {index.Name} ON {table.Name}({columns});";
    }

    private static DbValue ConvertToDbValue(SqlColumnSpec column, object? value)
    {
        if (value is null)
            return DbValue.Null;

        return NormalizeType(column.Type) switch
        {
            "INTEGER" => DbValue.FromInteger(ConvertToInt64(value)),
            "REAL" => DbValue.FromReal(ConvertToDouble(value)),
            "TEXT" => DbValue.FromText(ConvertToText(value)),
            "BLOB" => DbValue.FromBlob(ConvertToBlob(value)),
            var type => throw new InvalidOperationException(
                $"Unsupported SQL type '{type}' for column '{column.Name}'."),
        };
    }

    private static string FormatCsvValue(object? value)
    {
        return value switch
        {
            null => string.Empty,
            DateTime dateTime => dateTime.ToUniversalTime().ToString("O", CultureInfo.InvariantCulture),
            bool boolean => boolean ? "true" : "false",
            string text => text,
            IEnumerable<string> strings => JsonSerializer.Serialize(strings),
            System.Collections.IEnumerable enumerable when value is not string => JsonSerializer.Serialize(enumerable),
            IFormattable formattable => formattable.ToString(null, CultureInfo.InvariantCulture),
            _ => value.ToString() ?? string.Empty,
        };
    }

    private static long ConvertToInt64(object value)
    {
        return value switch
        {
            bool boolean => boolean ? 1L : 0L,
            byte byteValue => byteValue,
            sbyte sbyteValue => sbyteValue,
            short shortValue => shortValue,
            ushort ushortValue => ushortValue,
            int intValue => intValue,
            uint uintValue => checked((long)uintValue),
            long longValue => longValue,
            ulong ulongValue => checked((long)ulongValue),
            Enum enumValue => ConvertToInt64(Convert.ChangeType(enumValue, Enum.GetUnderlyingType(enumValue.GetType()), CultureInfo.InvariantCulture)!),
            _ => Convert.ToInt64(value, CultureInfo.InvariantCulture),
        };
    }

    private static double ConvertToDouble(object value)
    {
        return value switch
        {
            float floatValue => floatValue,
            double doubleValue => doubleValue,
            decimal decimalValue => (double)decimalValue,
            _ => Convert.ToDouble(value, CultureInfo.InvariantCulture),
        };
    }

    private static string ConvertToText(object value)
    {
        return value switch
        {
            DateTime dateTime => dateTime.ToUniversalTime().ToString("O", CultureInfo.InvariantCulture),
            string text => text,
            IEnumerable<string> strings => JsonSerializer.Serialize(strings),
            System.Collections.IEnumerable enumerable when value is not string => JsonSerializer.Serialize(enumerable),
            IFormattable formattable => formattable.ToString(null, CultureInfo.InvariantCulture),
            _ => value.ToString() ?? string.Empty,
        };
    }

    private static byte[] ConvertToBlob(object value)
    {
        return value switch
        {
            byte[] bytes => bytes,
            string text => Convert.FromBase64String(text),
            _ => throw new InvalidOperationException($"Value '{value}' cannot be converted to BLOB."),
        };
    }

    private static string NormalizeType(string type)
        => type.Trim().ToUpperInvariant();
}
