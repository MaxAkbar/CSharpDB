using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using CSharpDB.Execution;
using CSharpDB.Primitives;
using CSharpDB.Sql;

namespace CSharpDB.DataPrivacy;

internal static class PrivacyValues
{
    internal static DbValue Db(object? value) => value switch
    {
        null => DbValue.Null, DbValue db => db, byte[] bytes => DbValue.FromBlob(bytes),
        string text => DbValue.FromText(text), decimal n => DbValue.FromDecimal(n), double n => DbValue.FromReal(n),
        float n => DbValue.FromReal(n), bool b => DbValue.FromInteger(b ? 1 : 0),
        _ => DbValue.FromInteger(Convert.ToInt64(value, CultureInfo.InvariantCulture))
    };
    internal static object? Object(DbValue value) => value.Type switch
    {
        DbType.Null => null, DbType.Text => value.AsText, DbType.Blob => value.AsBlob,
        DbType.Decimal => value.AsDecimal, DbType.Real => value.AsReal, _ => value.AsInteger
    };
    internal static string Literal(object? value) => Db(value) switch
    {
        { IsNull: true } => "NULL",
        { Type: DbType.Text } v => "'" + v.AsText.Replace("'", "''", StringComparison.Ordinal) + "'",
        { Type: DbType.Blob } v => "X'" + Convert.ToHexString(v.AsBlob) + "'",
        { Type: DbType.Decimal } v => v.AsDecimal.ToString(CultureInfo.InvariantCulture),
        { Type: DbType.Real } v => SqlLiteralRules.FormatReal(v.AsReal),
        var v => v.AsInteger.ToString(CultureInfo.InvariantCulture)
    };
    internal static string Display(object? value) => value is null ? "NULL" : value is byte[] b ? "0x" + Convert.ToHexString(b) : Convert.ToString(value, CultureInfo.InvariantCulture)!;
    internal static long Size(IEnumerable<object?> values) => values.Sum(v => v switch
    { string s => 64L + s.Length * 2L, byte[] b => 64L + b.Length, _ => 64L });
    internal static bool Equal(object? a, object? b) => Literal(a) == Literal(b);
    internal static ColumnDefinition Column(TableSchema schema, string name)
        => schema.Columns.FirstOrDefault(c => Same(c.Name, name)) ?? throw new PrivacyException($"{schema.TableName}: a configured column is missing.");
    internal static bool Same(string a, string b) => string.Equals(a, b, StringComparison.OrdinalIgnoreCase);
    internal static object? Assign(object? value, ColumnDefinition column, string table)
    {
        try
        {
            var result = SqlTypeCoercion.CoerceForAssignment(Db(value), column, table);
            if (result.IsNull && (!column.Nullable || column.IsPrimaryKey)) throw new PrivacyException($"{table}.{column.Name}: NULL is not allowed.");
            return Object(result);
        }
        catch (PrivacyException) { throw; }
        catch { throw new PrivacyException($"{table}.{column.Name}: replacement does not fit the declared type."); }
    }
    internal static object? Constant(string text, ColumnDefinition column, string table)
    {
        try
        {
            object value = column.Type switch
            {
                DbType.Integer => column.EffectiveType.Kind == SqlTypeKind.Boolean && bool.TryParse(text, out bool b) ? (b ? 1L : 0L) : long.Parse(text, CultureInfo.InvariantCulture),
                DbType.Real => double.Parse(text, CultureInfo.InvariantCulture),
                DbType.Decimal => decimal.Parse(text, CultureInfo.InvariantCulture),
                DbType.Blob when column.EffectiveType.Kind == SqlTypeKind.Uuid => Guid.Parse(text).ToByteArray(),
                DbType.Blob => Convert.FromHexString(text.StartsWith("0x", StringComparison.OrdinalIgnoreCase) ? text[2..] : text),
                _ => text
            };
            return Assign(value, column, table);
        }
        catch (PrivacyException) { throw; }
        catch { throw new PrivacyException($"{table}.{column.Name}: invalid typed replacement or comparison value."); }
    }
    internal static string? Key(TableSchema schema, IReadOnlyList<string> columns, IReadOnlyDictionary<string, object?> row, IReadOnlyList<string?>? collations = null)
    {
        if (columns.Any(c => row[c] is null)) return null;
        return JsonSerializer.Serialize(columns.Select((c, i) =>
        {
            var value = Db(row[c]);
            if (value.Type == DbType.Text) value = CollationSupport.NormalizeIndexValue(value, collations is null ? Column(schema, c).Collation : collations[i]);
            return value.Type == DbType.Decimal ? value.AsDecimal.ToString("G29", CultureInfo.InvariantCulture) : Literal(Object(value));
        }));
    }
    internal static object? Mask(PrivacyPolicy policy, TableSchema schema, string identity, PrivacyColumnMask mask, object? original)
    {
        if (original is null) return null;
        var column = Column(schema, mask.Column);
        object? result = mask.Kind switch
        {
            PrivacyMaskKind.Erase => null,
            PrivacyMaskKind.Constant => Constant(mask.Replacement, column, schema.TableName),
            // A per-record placeholder, never a hash of a person's original field value.
            PrivacyMaskKind.Anonymous => "anonymous-" + Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(
                JsonSerializer.Serialize(new { policy.Id, TableId = schema.SchemaId, ColumnId = column.SchemaId, identity })))).ToLowerInvariant(),
            PrivacyMaskKind.Partial => Partial((string)original, mask.KeepPrefix, mask.KeepSuffix),
            PrivacyMaskKind.Email => Email((string)original),
            _ => throw new PrivacyException("Unknown masking operation.")
        };
        return Assign(result, column, schema.TableName);
    }
    internal static string Partial(string value, int prefix, int suffix)
    {
        var elements = new List<string>(); var e = StringInfo.GetTextElementEnumerator(value);
        while (e.MoveNext()) elements.Add(e.GetTextElement());
        if (elements.Count == 0) return value;
        if (prefix < 0 || suffix < 0 || (long)prefix + suffix >= elements.Count)
            throw new PrivacyException("Partial mask would leave a value entirely visible. Reduce the retained prefix/suffix.");
        return string.Concat(elements.Take(prefix)) + new string('*', elements.Count - prefix - suffix) + string.Concat(elements.Skip(elements.Count - suffix));
    }
    private static string Email(string value)
    {
        int at = value.LastIndexOf('@');
        if (at <= 0 || at == value.Length - 1) throw new PrivacyException("Email mask encountered an invalid email. Use a full replacement instead.");
        return Partial(value[..at], 0, 0) + value[at..];
    }
    internal static bool TryDate(object value, PrivacyCondition condition, out DateTimeOffset result)
    {
        result = default;
        try
        {
            if (condition.DateEncoding is PrivacyDateEncoding.UnixSeconds or PrivacyDateEncoding.UnixMilliseconds)
            {
                if (!long.TryParse(Display(value), NumberStyles.Integer, CultureInfo.InvariantCulture, out long unix)) return false;
                result = condition.DateEncoding == PrivacyDateEncoding.UnixSeconds ? DateTimeOffset.FromUnixTimeSeconds(unix) : DateTimeOffset.FromUnixTimeMilliseconds(unix); return true;
            }
            string text = Display(value);
            if (condition.DateEncoding == PrivacyDateEncoding.Iso8601)
            {
                string[] formats = ["yyyy-MM-dd", "yyyy-MM-dd'T'HH:mm:ssK", "yyyy-MM-dd'T'HH:mm:ss.FFFFFFFK", "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd HH:mm:ss.FFFFFFF"];
                return DateTimeOffset.TryParseExact(text, formats, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out result);
            }
            if (!DateTime.TryParseExact(text, condition.DateFormat, CultureInfo.InvariantCulture, DateTimeStyles.None, out var local)) return false;
            // ExactFormat represents a wall-clock value in the explicitly selected zone.
            // Offset-bearing values belong to ISO storage; never reinterpret an offset as local time.
            if (local.Kind != DateTimeKind.Unspecified) return false;
            local = DateTime.SpecifyKind(local, DateTimeKind.Unspecified);
            var zone = TimeZoneInfo.FindSystemTimeZoneById(condition.TimeZoneId);
            if (zone.IsAmbiguousTime(local) || zone.IsInvalidTime(local)) return false;
            result = new DateTimeOffset(TimeZoneInfo.ConvertTimeToUtc(local, zone)); return true;
        }
        catch (Exception ex) when (ex is ArgumentException or FormatException or OverflowException) { return false; }
    }
    internal static Expression SafeCheck(string sql)
    {
        var expression = Parser.ParseExpressionSql(sql);
        void Check(Expression e)
        {
            switch (e)
            {
                case LiteralExpression or ColumnRefExpression: return;
                case BinaryExpression b: Check(b.Left); Check(b.Right); return;
                case UnaryExpression u: Check(u.Operand); return;
                case IsNullExpression n: Check(n.Operand); return;
                case BetweenExpression b: Check(b.Operand); Check(b.Low); Check(b.High); return;
                case InExpression i: Check(i.Operand); foreach (var v in i.Values) Check(v); return;
                case CollateExpression c: Check(c.Operand); return;
                default: throw new PrivacyException("This table has a CHECK expression unsupported by privacy preflight.");
            }
        }
        Check(expression); return expression;
    }
}
