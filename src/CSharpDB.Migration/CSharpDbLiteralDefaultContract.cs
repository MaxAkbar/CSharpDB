using System.Globalization;
using CSharpDB.Primitives;

namespace CSharpDB.Migration;

internal readonly record struct CSharpDbLiteralDefaultDescriptor(
    string Kind,
    string? LiteralType,
    string? Value,
    string Expression)
{
    internal bool ProducesNull =>
        string.Equals(Kind, "null", StringComparison.Ordinal);
}

/// <summary>
/// Canonical, non-executable transport contract for CSharpDB literal defaults.
/// Renderers reconstruct SQL from these typed fields and never trust retained
/// source SQL as executable text.
/// </summary>
internal static class CSharpDbLiteralDefaultContract
{
    internal const int MaxValueCharacters = 1024 * 1024;

    internal static bool TryCreate(
        DbType columnType,
        string? kind,
        string? literalType,
        string? value,
        out CSharpDbLiteralDefaultDescriptor descriptor,
        out string reason)
    {
        string normalizedKind = Normalize(kind);
        if (normalizedKind == "null")
        {
            if (literalType is not null || value is not null)
            {
                descriptor = default;
                reason =
                    "NULL defaults cannot carry defaultType or defaultValue facets.";
                return false;
            }

            descriptor = new CSharpDbLiteralDefaultDescriptor(
                "null",
                null,
                null,
                "NULL");
            reason = string.Empty;
            return true;
        }

        if (normalizedKind != "typed-literal")
        {
            descriptor = default;
            reason = $"Default kind '{kind}' is not a safe literal default.";
            return false;
        }

        string normalizedType = Normalize(literalType);
        if (string.IsNullOrEmpty(normalizedType) || value is null)
        {
            descriptor = default;
            reason =
                "Typed literal defaults require defaultType and defaultValue facets.";
            return false;
        }
        if (value.Length > MaxValueCharacters)
        {
            descriptor = default;
            reason =
                $"Literal default exceeds the {MaxValueCharacters} character limit.";
            return false;
        }

        DbType valueType;
        string canonicalValue;
        string expression;
        switch (normalizedType)
        {
            case "integer":
                if (!long.TryParse(
                        value,
                        NumberStyles.AllowLeadingSign,
                        CultureInfo.InvariantCulture,
                        out long integer))
                {
                    descriptor = default;
                    reason = "INTEGER defaultValue is not a signed 64-bit integer.";
                    return false;
                }
                valueType = DbType.Integer;
                canonicalValue = integer.ToString(CultureInfo.InvariantCulture);
                expression = canonicalValue;
                break;

            case "real":
                if (!double.TryParse(
                        value,
                        NumberStyles.Float,
                        CultureInfo.InvariantCulture,
                        out double real) ||
                    !double.IsFinite(real))
                {
                    descriptor = default;
                    reason = "REAL defaultValue must be finite.";
                    return false;
                }
                valueType = DbType.Real;
                canonicalValue = SqlLiteralRules.FormatReal(real);
                expression = canonicalValue;
                break;

            case "text":
                valueType = DbType.Text;
                canonicalValue = value;
                expression = string.Concat(
                    "'",
                    value.Replace("'", "''", StringComparison.Ordinal),
                    "'");
                break;

            case "blob":
                if ((value.Length & 1) != 0 ||
                    value.Any(static character => !Uri.IsHexDigit(character)))
                {
                    descriptor = default;
                    reason =
                        "BLOB defaultValue must contain an even number of hexadecimal characters.";
                    return false;
                }
                valueType = DbType.Blob;
                canonicalValue = value.ToUpperInvariant();
                expression = string.Concat("X'", canonicalValue, "'");
                break;

            case "decimal":
                if (!decimal.TryParse(
                        value,
                        NumberStyles.AllowLeadingSign |
                        NumberStyles.AllowDecimalPoint,
                        CultureInfo.InvariantCulture,
                        out decimal decimalValue))
                {
                    descriptor = default;
                    reason = "DECIMAL defaultValue is not an exact invariant decimal.";
                    return false;
                }
                try
                {
                    DbValue normalized = DbValue.FromDecimal(decimalValue);
                    valueType = DbType.Decimal;
                    canonicalValue = normalized.AsDecimal.ToString(
                        CultureInfo.InvariantCulture);
                    expression = canonicalValue;
                }
                catch (OverflowException)
                {
                    descriptor = default;
                    reason = "DECIMAL defaultValue exceeds 18 digits of precision.";
                    return false;
                }
                break;

            default:
                descriptor = default;
                reason = $"Default type '{literalType}' is not supported.";
                return false;
        }

        if (valueType != columnType &&
            !(columnType == DbType.Real && valueType == DbType.Integer))
        {
            descriptor = default;
            reason =
                $"Literal default type {valueType} is incompatible with column type {columnType}.";
            return false;
        }

        descriptor = new CSharpDbLiteralDefaultDescriptor(
            "typed-literal",
            normalizedType,
            canonicalValue,
            expression);
        reason = string.Empty;
        return true;
    }

    private static string Normalize(string? value) =>
        (value ?? string.Empty)
            .Trim()
            .Replace('_', '-')
            .ToLowerInvariant();
}
