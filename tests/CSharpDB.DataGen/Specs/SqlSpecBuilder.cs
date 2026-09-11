using System.Globalization;
using System.Text.Json;
using CSharpDB.Primitives;

namespace CSharpDB.DataGen.Specs;

public static class SqlSpecBuilder
{
    private static readonly System.Collections.Concurrent.ConcurrentDictionary<string, SqlTypeDescriptor> Types = new(StringComparer.OrdinalIgnoreCase);
    public static string BuildSchemaScript(IEnumerable<SqlTableSpec> tables, bool includeIndexes)
    {
        var lines = new List<string>();
        foreach (SqlTableSpec table in tables)
        {
            string columns = string.Join(", ", table.Columns.Select(BuildColumnSql));
            lines.Add($"CREATE TABLE {SqlIdentifierRules.Quote(table.Name)} ({columns});");
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
        WriteDbValues(table, row, values);
        return values;
    }

    public static void WriteDbValues(
        SqlTableSpec table,
        IReadOnlyDictionary<string, object?> row,
        Span<DbValue> destination)
    {
        if (destination.Length < table.Columns.Count)
        {
            throw new ArgumentException(
                $"Destination must have at least {table.Columns.Count} values.",
                nameof(destination));
        }

        for (int i = 0; i < table.Columns.Count; i++)
        {
            SqlColumnSpec column = table.Columns[i];
            string sourceField = string.IsNullOrWhiteSpace(column.SourceField) ? column.Name : column.SourceField;
            row.TryGetValue(sourceField, out object? rawValue);
            destination[i] = ConvertToDbValue(column, rawValue);
        }
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
            SqlIdentifierRules.Quote(column.Name),
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
        string columns = string.Join(", ", index.Columns.Select(SqlIdentifierRules.Quote));
        return $"CREATE {unique}INDEX {SqlIdentifierRules.Quote(index.Name)} ON {SqlIdentifierRules.Quote(table.Name)}({columns});";
    }

    private static DbValue ConvertToDbValue(SqlColumnSpec column, object? value)
    {
        SqlTypeDescriptor type = Types.GetOrAdd(column.Type, sqlType =>
        {
            var statement = (CSharpDB.Sql.CreateTableStatement)CSharpDB.Sql.Parser.Parse($"CREATE TABLE datagen_type (value {sqlType});");
            if (statement.Columns.Count != 1) throw new InvalidOperationException("Expected one SQL type.");
            return statement.Columns[0].DeclaredType;
        });
        if (type.StorageType == DbType.Blob && value is string text && type.Kind is not SqlTypeKind.Uuid)
            value = System.Text.Encoding.UTF8.GetBytes(text);
        if (type.StorageType == DbType.Decimal && value is double number) value = (decimal)number;
        return CSharpDB.DataGeneration.GenerationValues.Assign(value, new ColumnDefinition
        {
            Name = column.Name, Type = type.StorageType, DeclaredType = type,
            Nullable = column.Nullable, IsPrimaryKey = column.PrimaryKey,
        }, "DataGen");
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
