using System.Globalization;
using System.Text;

namespace CSharpDB.Primitives;

/// <summary>
/// Canonical formatting rules for CSharpDB SQL literals.
/// </summary>
public static class SqlLiteralRules
{
    /// <summary>
    /// Formats a finite <see cref="double"/> as an invariant, exponent-free
    /// CSharpDB REAL literal. The result always contains a decimal point so
    /// the SQL parser preserves the REAL value kind, including for integral
    /// values and negative zero.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="value"/> is NaN or infinity.
    /// </exception>
    public static string FormatReal(double value)
    {
        if (!double.IsFinite(value))
        {
            throw new ArgumentOutOfRangeException(
                nameof(value),
                "CSharpDB SQL REAL literals must be finite.");
        }

        string roundTrip =
            value.ToString("R", CultureInfo.InvariantCulture);
        int exponentMarker = roundTrip.IndexOfAny(['E', 'e']);
        if (exponentMarker < 0)
        {
            return roundTrip.Contains('.')
                ? roundTrip
                : roundTrip + ".0";
        }

        string mantissa = roundTrip[..exponentMarker];
        int exponent = int.Parse(
            roundTrip[(exponentMarker + 1)..],
            NumberStyles.AllowLeadingSign,
            CultureInfo.InvariantCulture);
        bool negative = mantissa.StartsWith(
            "-",
            StringComparison.Ordinal);
        if (negative)
            mantissa = mantissa[1..];

        int originalPoint = mantissa.IndexOf('.');
        if (originalPoint < 0)
            originalPoint = mantissa.Length;
        string digits = mantissa.Replace(
            ".",
            string.Empty,
            StringComparison.Ordinal);
        int targetPoint = originalPoint + exponent;

        var builder = new StringBuilder(
            roundTrip.Length + Math.Abs(exponent) + 4);
        if (negative)
            builder.Append('-');
        if (targetPoint <= 0)
        {
            builder.Append("0.");
            builder.Append('0', -targetPoint);
            builder.Append(digits);
        }
        else if (targetPoint >= digits.Length)
        {
            builder.Append(digits);
            builder.Append('0', targetPoint - digits.Length);
            builder.Append(".0");
        }
        else
        {
            builder.Append(digits.AsSpan(0, targetPoint));
            builder.Append('.');
            builder.Append(digits.AsSpan(targetPoint));
        }

        return builder.ToString();
    }
}
