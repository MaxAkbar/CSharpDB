using System.Globalization;
using CSharpDB.Primitives;

namespace CSharpDB.Migration.Files.Csv;

[Flags]
internal enum CsvScalarCandidate
{
    None = 0,
    Boolean = 1 << 0,
    SignedInteger = 1 << 1,
    UnsignedInteger = 1 << 2,
    Decimal = 1 << 3,
    Guid = 1 << 4,
    Date = 1 << 5,
    Time = 1 << 6,
    DateTime = 1 << 7,
    DateTimeOffset = 1 << 8,
}

internal readonly record struct CsvScalarClassification(
    CsvScalarCandidate Candidates,
    bool RequiresLexicalPreservation,
    int IntegralDigits,
    int Scale,
    bool IsTrue,
    bool IsFalse);

/// <summary>
/// Shared inference/apply lexical boundary. Future CSV streaming adapters must
/// call this policy before constructing typed migration values; permissive
/// target codecs are not a substitute for source-grammar validation.
/// </summary>
internal static class CsvScalarLexicalPolicy
{
    public const string AlgorithmId = "csharpdb-csv-scalar-v1";

    private static readonly string[] TimeFormats =
    [
        "HH:mm:ss",
        "HH:mm:ss.FFFFFFF",
    ];

    private static readonly string[] DateTimeFormats =
    [
        "yyyy-MM-dd'T'HH:mm:ss",
        "yyyy-MM-dd'T'HH:mm:ss.FFFFFFF",
        "yyyy-MM-dd HH:mm:ss",
        "yyyy-MM-dd HH:mm:ss.FFFFFFF",
    ];

    private static readonly string[] UtcDateTimeOffsetFormats =
    [
        "yyyy-MM-dd'T'HH:mm:ss'Z'",
        "yyyy-MM-dd'T'HH:mm:ss.FFFFFFF'Z'",
    ];

    private static readonly string[] OffsetDateTimeFormats =
    [
        "yyyy-MM-dd'T'HH:mm:sszzz",
        "yyyy-MM-dd'T'HH:mm:ss.FFFFFFFzzz",
        "yyyy-MM-dd HH:mm:sszzz",
        "yyyy-MM-dd HH:mm:ss.FFFFFFFzzz",
    ];

    public static CsvScalarClassification Classify(string text, CultureInfo culture)
    {
        CsvScalarCandidate candidates = CsvScalarCandidate.None;
        bool isTrue = string.Equals(text, "true", StringComparison.Ordinal);
        bool isFalse = string.Equals(text, "false", StringComparison.Ordinal);
        if (isTrue || isFalse)
            candidates |= CsvScalarCandidate.Boolean;

        int integralDigits = 0;
        int scale = 0;
        if (TryParseDecimalParts(
                text,
                culture.NumberFormat.NumberDecimalSeparator,
                allowLexicalNormalization: false,
                out DecimalParts number))
        {
            candidates |= CsvScalarCandidate.Decimal;
            integralDigits = number.IntegralLength;
            scale = number.FractionLength;
            if (!number.HasDecimalSeparator)
            {
                if (number.Negative)
                {
                    if (long.TryParse(
                            text,
                            NumberStyles.AllowLeadingSign,
                            CultureInfo.InvariantCulture,
                            out _))
                    {
                        candidates |= CsvScalarCandidate.SignedInteger;
                    }
                }
                else
                {
                    if (long.TryParse(text, NumberStyles.None, CultureInfo.InvariantCulture, out _))
                        candidates |= CsvScalarCandidate.SignedInteger;
                    if (ulong.TryParse(text, NumberStyles.None, CultureInfo.InvariantCulture, out _))
                        candidates |= CsvScalarCandidate.UnsignedInteger;
                }
            }
        }

        if (Guid.TryParseExact(text, "D", out _))
            candidates |= CsvScalarCandidate.Guid;
        if (TryParseDate(text, out _))
            candidates |= CsvScalarCandidate.Date;
        if (TryParseTime(text, out _))
            candidates |= CsvScalarCandidate.Time;
        if (TryParseDateTime(text, out _))
            candidates |= CsvScalarCandidate.DateTime;
        if (TryParseDateTimeOffset(text, out _))
            candidates |= CsvScalarCandidate.DateTimeOffset;

        bool preservation =
            (candidates & (CsvScalarCandidate.SignedInteger |
                           CsvScalarCandidate.UnsignedInteger |
                           CsvScalarCandidate.Decimal)) == 0 &&
            LooksLikeNumber(text, culture.NumberFormat.NumberDecimalSeparator);
        return new CsvScalarClassification(
            candidates,
            preservation,
            integralDigits,
            scale,
            isTrue,
            isFalse);
    }

    public static bool TryNormalize(
        string text,
        CsvColumnLogicalType logicalType,
        CultureInfo culture,
        bool allowLexicalNormalization,
        out string? canonicalText)
    {
        canonicalText = null;
        switch (logicalType)
        {
            case CsvColumnLogicalType.Text:
                canonicalText = text;
                return true;
            case CsvColumnLogicalType.Boolean:
                if (text is not ("true" or "false"))
                    return false;
                canonicalText = text;
                return true;
            case CsvColumnLogicalType.SignedInteger:
                {
                    if (text.Length == 0 || text.Length != text.Trim().Length ||
                        !long.TryParse(
                            text,
                            NumberStyles.AllowLeadingSign,
                            CultureInfo.InvariantCulture,
                            out long signed) ||
                        !allowLexicalNormalization &&
                        (Classify(text, culture).Candidates & CsvScalarCandidate.SignedInteger) == 0)
                    {
                        return false;
                    }

                    canonicalText = signed.ToString(CultureInfo.InvariantCulture);
                    return true;
                }
            case CsvColumnLogicalType.UnsignedInteger:
                {
                    NumberStyles styles = allowLexicalNormalization
                        ? NumberStyles.AllowLeadingSign
                        : NumberStyles.None;
                    if (text.Length == 0 || text.Length != text.Trim().Length ||
                        !ulong.TryParse(text, styles, CultureInfo.InvariantCulture, out ulong unsigned) ||
                        !allowLexicalNormalization &&
                        (Classify(text, culture).Candidates & CsvScalarCandidate.UnsignedInteger) == 0)
                    {
                        return false;
                    }

                    canonicalText = unsigned.ToString(CultureInfo.InvariantCulture);
                    return true;
                }
            case CsvColumnLogicalType.Decimal:
                if (!TryParseDecimalParts(
                        text,
                        culture.NumberFormat.NumberDecimalSeparator,
                        allowLexicalNormalization,
                        out DecimalParts decimalParts))
                {
                    return false;
                }

                canonicalText = NormalizeDecimal(text, decimalParts);
                return true;
            case CsvColumnLogicalType.FloatingPoint:
                if (!allowLexicalNormalization || !TryParseFiniteDouble(text, culture, out double real))
                    return false;
                canonicalText = real.ToString("R", CultureInfo.InvariantCulture);
                return true;
            case CsvColumnLogicalType.Guid:
                if (!Guid.TryParseExact(text, "D", out Guid guid))
                    return false;
                canonicalText = CSharpDbTextCodec.FormatGuid(guid);
                return true;
            case CsvColumnLogicalType.Date:
                if (!TryParseDate(text, out DateOnly date))
                    return false;
                canonicalText = CSharpDbTextCodec.FormatDate(date);
                return true;
            case CsvColumnLogicalType.Time:
                if (!TryParseTime(text, out TimeOnly time))
                    return false;
                canonicalText = CSharpDbTextCodec.FormatTime(time);
                return true;
            case CsvColumnLogicalType.DateTime:
                if (!TryParseDateTime(text, out DateTime dateTime))
                    return false;
                canonicalText = CSharpDbTextCodec.FormatDateTime(dateTime);
                return true;
            case CsvColumnLogicalType.DateTimeOffset:
                if (!TryParseDateTimeOffset(text, out DateTimeOffset dateTimeOffset))
                    return false;
                canonicalText = CSharpDbTextCodec.FormatDateTimeOffset(dateTimeOffset);
                return true;
            default:
                return false;
        }
    }

    private static bool TryParseDecimalParts(
        string text,
        string decimalSeparator,
        bool allowLexicalNormalization,
        out DecimalParts parts)
    {
        parts = default;
        if (text.Length == 0 || decimalSeparator.Length == 0 || text.Length != text.Trim().Length)
            return false;

        int offset = 0;
        bool negative = false;
        if (text[0] == '-')
        {
            negative = true;
            offset = 1;
        }
        else if (text[0] == '+')
        {
            if (!allowLexicalNormalization)
                return false;
            offset = 1;
        }

        if (offset == text.Length)
            return false;

        int separatorIndex = text.IndexOf(decimalSeparator, offset, StringComparison.Ordinal);
        if (separatorIndex >= 0 &&
            text.IndexOf(
                decimalSeparator,
                separatorIndex + decimalSeparator.Length,
                StringComparison.Ordinal) >= 0)
        {
            return false;
        }

        int integralEnd = separatorIndex < 0 ? text.Length : separatorIndex;
        int fractionalStart = separatorIndex < 0
            ? text.Length
            : separatorIndex + decimalSeparator.Length;
        if (integralEnd == offset || (separatorIndex >= 0 && fractionalStart == text.Length))
            return false;
        if (!AllAsciiDigits(text.AsSpan(offset, integralEnd - offset)) ||
            (separatorIndex >= 0 && !AllAsciiDigits(text.AsSpan(fractionalStart))))
        {
            return false;
        }

        ReadOnlySpan<char> integral = text.AsSpan(offset, integralEnd - offset);
        ReadOnlySpan<char> fraction = separatorIndex < 0
            ? ReadOnlySpan<char>.Empty
            : text.AsSpan(fractionalStart);
        bool zero = integral.IndexOfAnyExcept('0') < 0 &&
                    (fraction.IsEmpty || fraction.IndexOfAnyExcept('0') < 0);
        if (!allowLexicalNormalization &&
            (integral.Length > 1 && integral[0] == '0' || negative && zero))
        {
            return false;
        }

        parts = new DecimalParts(
            negative,
            offset,
            integral.Length,
            fractionalStart,
            fraction.Length,
            separatorIndex >= 0);
        return true;
    }

    private static string NormalizeDecimal(string text, DecimalParts parts)
    {
        ReadOnlySpan<char> integral = text.AsSpan(parts.IntegralStart, parts.IntegralLength);
        int leadingZeroes = 0;
        while (leadingZeroes < integral.Length - 1 && integral[leadingZeroes] == '0')
            leadingZeroes++;
        integral = integral[leadingZeroes..];

        ReadOnlySpan<char> fraction = parts.HasDecimalSeparator
            ? text.AsSpan(parts.FractionStart, parts.FractionLength)
            : ReadOnlySpan<char>.Empty;
        int fractionalLength = fraction.Length;
        while (fractionalLength > 0 && fraction[fractionalLength - 1] == '0')
            fractionalLength--;
        fraction = fraction[..fractionalLength];

        bool zero = integral.IndexOfAnyExcept('0') < 0 && fraction.IsEmpty;
        int signLength = parts.Negative && !zero ? 1 : 0;
        int resultLength = signLength + integral.Length +
                           (fraction.IsEmpty ? 0 : 1 + fraction.Length);
        var state = new DecimalNormalizationState(
            parts.Negative && !zero,
            text,
            parts.IntegralStart + leadingZeroes,
            integral.Length,
            parts.FractionStart,
            fraction.Length);
        return string.Create(
            resultLength,
            state,
            static (destination, state) =>
            {
                int position = 0;
                if (state.Negative)
                    destination[position++] = '-';
                state.Text.AsSpan(state.IntegralStart, state.IntegralLength)
                    .CopyTo(destination[position..]);
                position += state.IntegralLength;
                if (state.FractionLength > 0)
                {
                    destination[position++] = '.';
                    state.Text.AsSpan(state.FractionStart, state.FractionLength)
                        .CopyTo(destination[position..]);
                }
            });
    }

    private static bool TryParseFiniteDouble(
        string text,
        CultureInfo culture,
        out double value)
    {
        value = default;
        if (text.Length == 0 || text.Length != text.Trim().Length)
            return false;

        const NumberStyles styles = NumberStyles.AllowLeadingSign |
                                    NumberStyles.AllowDecimalPoint |
                                    NumberStyles.AllowExponent;
        if (!double.TryParse(text, styles, culture, out value) || !double.IsFinite(value))
            return false;

        if (value == 0)
        {
            int exponent = text.IndexOfAny(['e', 'E']);
            ReadOnlySpan<char> mantissa = exponent < 0 ? text : text.AsSpan(0, exponent);
            foreach (char character in mantissa)
            {
                if (character is >= '1' and <= '9')
                    return false;
            }
        }

        return true;
    }

    private static bool TryParseDate(string text, out DateOnly value) =>
        DateOnly.TryParseExact(
            text,
            "yyyy-MM-dd",
            CultureInfo.InvariantCulture,
            DateTimeStyles.None,
            out value);

    private static bool TryParseTime(string text, out TimeOnly value)
    {
        value = default;
        return HasValidFraction(text, integralLength: 8, suffixLength: 0) &&
               TimeOnly.TryParseExact(
            text,
            TimeFormats,
            CultureInfo.InvariantCulture,
            DateTimeStyles.None,
            out value);

    }

    private static bool TryParseDateTime(string text, out DateTime value)
    {
        value = default;
        return HasValidFraction(text, integralLength: 19, suffixLength: 0) &&
               DateTime.TryParseExact(
            text,
            DateTimeFormats,
            CultureInfo.InvariantCulture,
            DateTimeStyles.None,
            out value);

    }

    private static bool TryParseDateTimeOffset(string text, out DateTimeOffset value)
    {
        value = default;
        if (text.EndsWith('Z'))
        {
            return HasValidFraction(text, integralLength: 19, suffixLength: 1) &&
                   DateTimeOffset.TryParseExact(
                text,
                UtcDateTimeOffsetFormats,
                CultureInfo.InvariantCulture,
                DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal,
                out value);
        }

        return HasValidFraction(text, integralLength: 19, suffixLength: 6) &&
               DateTimeOffset.TryParseExact(
            text,
            OffsetDateTimeFormats,
            CultureInfo.InvariantCulture,
            DateTimeStyles.None,
            out value);
    }

    private static bool HasValidFraction(
        string text,
        int integralLength,
        int suffixLength)
    {
        if (text.Length < integralLength + suffixLength)
            return false;

        int separator = text.IndexOf('.', integralLength);
        if (separator < 0)
            return true;

        int fractionLength = text.Length - separator - 1 - suffixLength;
        return fractionLength is >= 1 and <= 7 &&
               AllAsciiDigits(text.AsSpan(separator + 1, fractionLength));
    }

    private static bool LooksLikeNumber(string text, string decimalSeparator)
    {
        string value = text.Trim();
        if (value.Length == 0)
            return false;

        int index = 0;
        if (value[index] is '+' or '-')
            index++;
        bool sawDigit = false;
        while (index < value.Length && IsAsciiDigit(value[index]))
        {
            sawDigit = true;
            index++;
        }

        if (index < value.Length &&
            decimalSeparator.Length > 0 &&
            value.AsSpan(index).StartsWith(decimalSeparator, StringComparison.Ordinal))
        {
            index += decimalSeparator.Length;
            while (index < value.Length && IsAsciiDigit(value[index]))
            {
                sawDigit = true;
                index++;
            }
        }

        if (index < value.Length && value[index] is 'e' or 'E')
        {
            index++;
            if (index < value.Length && value[index] is '+' or '-')
                index++;
            bool exponentDigit = false;
            while (index < value.Length && IsAsciiDigit(value[index]))
            {
                exponentDigit = true;
                index++;
            }

            if (!exponentDigit)
                return false;
        }

        return sawDigit && index == value.Length;
    }

    private static bool AllAsciiDigits(ReadOnlySpan<char> value)
    {
        foreach (char character in value)
        {
            if (!IsAsciiDigit(character))
                return false;
        }

        return true;
    }

    private static bool IsAsciiDigit(char value) => value is >= '0' and <= '9';

    private readonly record struct DecimalParts(
        bool Negative,
        int IntegralStart,
        int IntegralLength,
        int FractionStart,
        int FractionLength,
        bool HasDecimalSeparator);

    private readonly record struct DecimalNormalizationState(
        bool Negative,
        string Text,
        int IntegralStart,
        int IntegralLength,
        int FractionStart,
        int FractionLength);
}
