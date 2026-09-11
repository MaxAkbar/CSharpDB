using System.Globalization;
using System.Text.Json;
using CSharpDB.Execution;
using CSharpDB.Primitives;
using CSharpDB.Sql;

namespace CSharpDB.DataGeneration;

public static class GenerationValues
{
    public static DbValue ToDbValue(object? value) => value switch
    {
        null => DbValue.Null,
        DbValue db => db,
        bool boolean => DbValue.FromInteger(boolean ? 1 : 0),
        byte[] bytes => DbValue.FromBlob(bytes),
        Guid guid => DbValue.FromBlob(guid.ToByteArray()),
        decimal number => DbValue.FromDecimal(number),
        double number => DbValue.FromReal(number),
        float number => DbValue.FromReal(number),
        byte or sbyte or short or ushort or int or uint or long => DbValue.FromInteger(Convert.ToInt64(value, CultureInfo.InvariantCulture)),
        DateTime date => DbValue.FromText(date.ToUniversalTime().ToString("O", CultureInfo.InvariantCulture)),
        DateTimeOffset date => DbValue.FromText(date.ToString("O", CultureInfo.InvariantCulture)),
        string text => DbValue.FromText(text),
        _ => throw new InvalidOperationException($"Unsupported generated value {value.GetType().Name}."),
    };

    public static DbValue Assign(object? value, ColumnDefinition column, string table)
    {
        DbValue db = SqlTypeCoercion.CoerceForAssignment(ToDbValue(value), column, table);
        if (db.IsNull && (!column.Nullable || column.IsPrimaryKey))
            throw new InvalidOperationException($"{table}.{column.Name} does not allow null values.");
        return db;
    }

    public static object? ToObject(DbValue value) => value.Type switch
    {
        DbType.Null => null, DbType.Integer => value.AsInteger, DbType.Real => value.AsReal,
        DbType.Decimal => value.AsDecimal, DbType.Blob => value.AsBlob, _ => value.AsText,
    };

    public static string Literal(DbValue value) => value.Type switch
    {
        DbType.Null => "NULL",
        DbType.Integer => value.AsInteger.ToString(CultureInfo.InvariantCulture),
        DbType.Real => SqlLiteralRules.FormatReal(value.AsReal),
        DbType.Decimal => value.AsDecimal.ToString(CultureInfo.InvariantCulture),
        DbType.Blob => "X'" + Convert.ToHexString(value.AsBlob) + "'",
        _ => "'" + value.AsText.Replace("'", "''", StringComparison.Ordinal) + "'",
    };

    public static string Display(object? value) => value is byte[] bytes ? "0x" + Convert.ToHexString(bytes) : Convert.ToString(value, CultureInfo.InvariantCulture) ?? "NULL";

    public static string InsertPrefix(string table, IReadOnlyList<string> columns)
        => $"INSERT INTO {SqlIdentifierRules.Quote(table)} ({string.Join(", ", columns.Select(SqlIdentifierRules.Quote))}) VALUES ";

    public static string RowLiteral(IReadOnlyList<string> columns, IReadOnlyDictionary<string, object?> row)
        => "(" + string.Join(", ", columns.Select(c => Literal(ToDbValue(row[c])))) + ")";

    public static string Key(TableSchema schema, IReadOnlyList<string> columns, IReadOnlyDictionary<string, object?> row,
        IReadOnlyList<string?>? collations = null)
        => JsonSerializer.Serialize(columns.Select((name, i) =>
        {
            var column = schema.Columns[schema.GetColumnIndex(name)];
            DbValue value = ToDbValue(row[name]);
            string? collation = collations is not null && i < collations.Count ? collations[i] : column.Collation;
            if (value.Type == DbType.Text) value = CollationSupport.NormalizeIndexValue(value, collation);
            return value.Type == DbType.Decimal ? value.AsDecimal.ToString("G29", CultureInfo.InvariantCulture) : Literal(value);
        }).ToArray());

    public static long Size(IEnumerable<object?> values) => values.Sum(value => value switch
    {
        byte[] bytes => (long)bytes.Length + 32,
        string text => (long)System.Text.Encoding.UTF8.GetByteCount(text) + 32,
        _ => 32L,
    });

    public static Expression ParseSafeExpression(string sql)
    {
        Expression expression = Parser.ParseExpressionSql(sql);
        ValidateExpression(expression);
        return expression;
    }

    private static void ValidateExpression(Expression expression)
    {
        switch (expression)
        {
            case LiteralExpression or ColumnRefExpression: return;
            case BinaryExpression binary: ValidateExpression(binary.Left); ValidateExpression(binary.Right); return;
            case UnaryExpression unary: ValidateExpression(unary.Operand); return;
            case IsNullExpression isNull: ValidateExpression(isNull.Operand); return;
            case BetweenExpression between: ValidateExpression(between.Operand); ValidateExpression(between.Low); ValidateExpression(between.High); return;
            case InExpression inside: ValidateExpression(inside.Operand); foreach (var value in inside.Values) ValidateExpression(value); return;
            case CollateExpression collate: ValidateExpression(collate.Operand); return;
            default: throw new InvalidOperationException("Generation supports constant/column CHECK expressions with arithmetic, comparison, AND/OR, IN, BETWEEN and IS NULL. Functions, subqueries and other expressions require a separate generation workflow.");
        }
    }

    public static DbValue Evaluate(Expression expression, TableSchema schema, IReadOnlyDictionary<string, object?> values)
        => ExpressionEvaluator.Evaluate(expression, schema.Columns.Select(c => ToDbValue(values.GetValueOrDefault(c.Name))).ToArray(), schema);
}
