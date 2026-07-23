using System.Globalization;
using System.Text;

namespace CSharpDB.Migration.Files.Json;

[Flags]
internal enum JsonTableScalarCandidate
{
    None = 0,
    Text = 1,
    Boolean = 2,
    SignedInteger = 4,
    UnsignedInteger = 8,
    Decimal = 16,
    Json = 32,
}

internal readonly record struct JsonTableScalarClassification(
    JsonTableScalarCandidate Candidates,
    bool RequiresJsonLexemePreservation,
    int IntegralDigits,
    int Scale);

/// <summary>
/// Versioned, preservation-first classification for native JSON values used
/// by relational table inference. It never parses through binary floating
/// point and never infers semantic intent from JSON strings.
/// </summary>
internal static class JsonTableScalarPolicy
{
    internal const string AlgorithmId = "csharpdb-json-table-scalar-v1";

    internal static JsonTableScalarClassification Classify(
        JsonLogicalValue value,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(value);
        cancellationToken.ThrowIfCancellationRequested();
        return value.Kind switch
        {
            JsonLogicalValueKind.String => new(
                JsonTableScalarCandidate.Text | JsonTableScalarCandidate.Json,
                RequiresJsonLexemePreservation: false,
                IntegralDigits: 0,
                Scale: 0),
            JsonLogicalValueKind.Boolean => new(
                JsonTableScalarCandidate.Boolean | JsonTableScalarCandidate.Json,
                RequiresJsonLexemePreservation: false,
                IntegralDigits: 0,
                Scale: 0),
            JsonLogicalValueKind.Number => ClassifyNumber(
                value.NumberLexeme,
                cancellationToken),
            JsonLogicalValueKind.Object or JsonLogicalValueKind.Array => new(
                JsonTableScalarCandidate.Json,
                RequiresJsonLexemePreservation: false,
                IntegralDigits: 0,
                Scale: 0),
            JsonLogicalValueKind.Null => throw new ArgumentException(
                "JSON null does not contribute scalar type evidence.",
                nameof(value)),
            _ => throw new InvalidDataException("Unknown JSON logical value kind."),
        };
    }

    internal static bool IsCompatible(
        JsonLogicalValue value,
        JsonTableColumnLogicalType logicalType,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(value);
        cancellationToken.ThrowIfCancellationRequested();
        if (value.Kind == JsonLogicalValueKind.Null)
            return true;
        JsonTableScalarCandidate required = logicalType switch
        {
            JsonTableColumnLogicalType.Text => JsonTableScalarCandidate.Text,
            JsonTableColumnLogicalType.Boolean => JsonTableScalarCandidate.Boolean,
            JsonTableColumnLogicalType.SignedInteger => JsonTableScalarCandidate.SignedInteger,
            JsonTableColumnLogicalType.UnsignedInteger => JsonTableScalarCandidate.UnsignedInteger,
            JsonTableColumnLogicalType.Decimal => JsonTableScalarCandidate.Decimal,
            JsonTableColumnLogicalType.Json => JsonTableScalarCandidate.Json,
            _ => throw new ArgumentOutOfRangeException(nameof(logicalType)),
        };
        return (Classify(value, cancellationToken).Candidates & required) != 0;
    }

    /// <summary>
    /// Counts the exact UTF-8 bytes emitted by
    /// <see cref="JsonCanonicalValueSerializer"/> without retaining output.
    /// </summary>
    internal static long GetCanonicalUtf8ByteCount(
        JsonLogicalValue value,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(value);
        cancellationToken.ThrowIfCancellationRequested();
        return value.Kind switch
        {
            JsonLogicalValueKind.Null => 4,
            JsonLogicalValueKind.Boolean => value.BooleanValue ? 4 : 5,
            JsonLogicalValueKind.String => JsonStringByteCount(
                value.StringValue,
                cancellationToken),
            JsonLogicalValueKind.Number => value.NumberLexeme.Length,
            JsonLogicalValueKind.Object => ObjectByteCount(
                value,
                cancellationToken),
            JsonLogicalValueKind.Array => ArrayByteCount(
                value,
                cancellationToken),
            _ => throw new InvalidDataException("Unknown JSON logical value kind."),
        };
    }

    private static JsonTableScalarClassification ClassifyNumber(
        string lexeme,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        bool negative = lexeme[0] == '-';
        bool integral =
            FindDecimalOrExponent(
                lexeme,
                cancellationToken) < 0;
        bool negativeZero =
            negative &&
            IsMathematicalZero(lexeme, cancellationToken);
        JsonTableScalarCandidate candidates = JsonTableScalarCandidate.Json;

        if (integral &&
            !negativeZero &&
            lexeme.Length <= 20 &&
            long.TryParse(
                lexeme,
                NumberStyles.AllowLeadingSign,
                CultureInfo.InvariantCulture,
                out _))
        {
            candidates |= JsonTableScalarCandidate.SignedInteger;
        }

        if (integral &&
            !negative &&
            lexeme.Length <= 20 &&
            ulong.TryParse(
                lexeme,
                NumberStyles.None,
                CultureInfo.InvariantCulture,
                out _))
        {
            candidates |= JsonTableScalarCandidate.UnsignedInteger;
        }

        int integralDigits = 0;
        int scale = 0;
        bool canonicalDecimal = TryGetCanonicalDecimalShape(
            lexeme,
            cancellationToken,
            out integralDigits,
            out scale);
        if (canonicalDecimal)
            candidates |= JsonTableScalarCandidate.Decimal;

        bool jsonOnly = candidates == JsonTableScalarCandidate.Json;
        return new JsonTableScalarClassification(
            candidates,
            RequiresJsonLexemePreservation: jsonOnly,
            integralDigits,
            scale);
    }

    private static bool TryGetCanonicalDecimalShape(
        string lexeme,
        CancellationToken cancellationToken,
        out int integralDigits,
        out int scale)
    {
        cancellationToken.ThrowIfCancellationRequested();
        integralDigits = 0;
        scale = 0;
        if (FindExponent(lexeme, cancellationToken) >= 0 ||
            IsMathematicalZero(lexeme, cancellationToken) &&
            lexeme[0] == '-')
            return false;

        int offset = lexeme[0] == '-' ? 1 : 0;
        int dot = FindCharacter(
            lexeme,
            offset,
            '.',
            cancellationToken);
        ReadOnlySpan<char> integral = dot < 0
            ? lexeme.AsSpan(offset)
            : lexeme.AsSpan(offset, dot - offset);
        ReadOnlySpan<char> fraction = dot < 0
            ? ReadOnlySpan<char>.Empty
            : lexeme.AsSpan(dot + 1);
        if (!fraction.IsEmpty && fraction[^1] == '0')
            return false;

        int firstNonZero = 0;
        while (firstNonZero < integral.Length && integral[firstNonZero] == '0')
        {
            cancellationToken.ThrowIfCancellationRequested();
            firstNonZero++;
        }
        integralDigits = firstNonZero == integral.Length
            ? 1
            : integral.Length - firstNonZero;
        scale = fraction.Length;
        return true;
    }

    private static bool IsMathematicalZero(
        string lexeme,
        CancellationToken cancellationToken)
    {
        foreach (char character in lexeme)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (character is >= '1' and <= '9')
                return false;
        }

        return true;
    }

    private static int FindDecimalOrExponent(
        string lexeme,
        CancellationToken cancellationToken)
    {
        for (int index = 0; index < lexeme.Length; index++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (lexeme[index] is '.' or 'e' or 'E')
                return index;
        }

        return -1;
    }

    private static int FindExponent(
        string lexeme,
        CancellationToken cancellationToken)
    {
        for (int index = 0; index < lexeme.Length; index++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (lexeme[index] is 'e' or 'E')
                return index;
        }

        return -1;
    }

    private static int FindCharacter(
        string value,
        int startIndex,
        char sought,
        CancellationToken cancellationToken)
    {
        for (int index = startIndex; index < value.Length; index++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (value[index] == sought)
                return index;
        }

        return -1;
    }

    private static long JsonStringByteCount(
        string value,
        CancellationToken cancellationToken)
    {
        long count = 2;
        for (int index = 0; index < value.Length;)
        {
            cancellationToken.ThrowIfCancellationRequested();
            char character = value[index];
            if (character is '"' or '\\' or '\b' or '\t' or '\n' or '\f' or '\r')
            {
                count = checked(count + 2);
                index++;
            }
            else if (character <= 0x1F)
            {
                count = checked(count + 6);
                index++;
            }
            else
            {
                Rune rune = Rune.GetRuneAt(value, index);
                count = checked(count + rune.Utf8SequenceLength);
                index += rune.Utf16SequenceLength;
            }
        }

        return count;
    }

    private static long ObjectByteCount(
        JsonLogicalValue value,
        CancellationToken cancellationToken)
    {
        long count = 2;
        for (int index = 0; index < value.Properties.Count; index++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            JsonLogicalProperty property = value.Properties[index];
            if (index != 0)
                count = checked(count + 1);
            count = checked(
                count +
                JsonStringByteCount(
                    property.Name,
                    cancellationToken));
            count = checked(count + 1);
            count = checked(
                count +
                GetCanonicalUtf8ByteCount(
                    property.Value,
                    cancellationToken));
        }

        return count;
    }

    private static long ArrayByteCount(
        JsonLogicalValue value,
        CancellationToken cancellationToken)
    {
        long count = 2;
        for (int index = 0; index < value.Elements.Count; index++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (index != 0)
                count = checked(count + 1);
            count = checked(
                count +
                GetCanonicalUtf8ByteCount(
                    value.Elements[index],
                    cancellationToken));
        }

        return count;
    }
}
