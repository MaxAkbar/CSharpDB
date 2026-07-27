using System.Globalization;
using System.Numerics;
using System.Security.Cryptography;
using System.Text;
using CSharpDB.Migration.Canonicalization;

namespace CSharpDB.Migration.DualRun;

internal static class DualRunCanonicalizer
{
    private static readonly UTF8Encoding StrictUtf8 = new(false, true);
    private static readonly DateOnly UnixEpochDate = new(1970, 1, 1);

    internal static CanonicalValue Canonicalize(object? value, CanonicalType type)
    {
        if (value is null || value is DBNull)
            return CanonicalValue.Null(type);

        try
        {
            return type switch
            {
                CanonicalType.Boolean => CanonicalValue.Boolean(ToBoolean(value)),
                CanonicalType.Int64 => CanonicalValue.Int64(ToInt64(value)),
                CanonicalType.UInt64 => CanonicalValue.UInt64(ToUInt64(value)),
                CanonicalType.Decimal => CanonicalValue.Decimal(ToDecimal(value)),
                CanonicalType.Binary32 => CanonicalValue.Binary32(ToSingle(value)),
                CanonicalType.Binary64 => CanonicalValue.Binary64(ToDouble(value)),
                CanonicalType.Text => CanonicalValue.Text(ToText(value)),
                CanonicalType.Blob => CanonicalValue.Blob(ToBlob(value)),
                CanonicalType.Guid => CanonicalValue.Guid(ToGuid(value)),
                CanonicalType.Date => CanonicalValue.Date(ToDate(value)),
                CanonicalType.Time => CanonicalValue.Time(ToTime(value)),
                CanonicalType.WallDateTime => CanonicalValue.WallDateTime(ToWallDateTime(value)),
                CanonicalType.UtcInstant => CanonicalValue.UtcInstant(ToUtcInstant(value)),
                CanonicalType.OffsetDateTime => CanonicalValue.OffsetDateTime(ToOffsetDateTime(value)),
                _ => throw new InvalidDataException($"Unknown canonical type 0x{(byte)type:x2}."),
            };
        }
        catch (DualRunExecutionException)
        {
            throw;
        }
        catch (Exception ex) when (
            ex is ArgumentException or ArithmeticException or FormatException or InvalidDataException)
        {
            throw new DualRunExecutionException(
                DualRunErrorKind.InvalidResult,
                "DUALRUN_VALUE_CANONICALIZATION_FAILED",
                ex);
        }
    }

    internal static int GetRawSize(object? value)
    {
        if (value is null || value is DBNull)
            return 0;

        try
        {
            return value switch
            {
                string text => StrictUtf8.GetByteCount(text),
                char => 4,
                byte[] bytes => bytes.Length,
                ReadOnlyMemory<byte> memory => memory.Length,
                Memory<byte> memory => memory.Length,
                Guid => 16,
                DateOnly or TimeOnly or DateTime or DateTimeOffset or TimeSpan => 16,
                bool or sbyte or byte => 1,
                short or ushort => 2,
                int or uint or float => 4,
                long or ulong or double => 8,
                decimal => 16,
                BigInteger integer => integer.GetByteCount(),
                _ => throw new DualRunExecutionException(
                    DualRunErrorKind.InvalidResult,
                    "DUALRUN_UNSUPPORTED_PROVIDER_VALUE"),
            };
        }
        catch (EncoderFallbackException ex)
        {
            throw new DualRunExecutionException(
                DualRunErrorKind.InvalidResult,
                "DUALRUN_VALUE_CANONICALIZATION_FAILED",
                ex);
        }
    }

    internal static string NormalizeIdentifier(string value)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value);
        return value.Normalize(NormalizationForm.FormC);
    }

    internal static string Sha256(ReadOnlySpan<byte> bytes) =>
        Convert.ToHexString(SHA256.HashData(bytes)).ToLowerInvariant();

    internal static byte[] Utf8(string value) => StrictUtf8.GetBytes(value);

    private static bool ToBoolean(object value) => value switch
    {
        bool result => result,
        byte result when result <= 1 => result != 0,
        sbyte result when result is 0 or 1 => result != 0,
        short result when result is 0 or 1 => result != 0,
        ushort result when result <= 1 => result != 0,
        int result when result is 0 or 1 => result != 0,
        uint result when result <= 1 => result != 0,
        long result when result is 0 or 1 => result != 0,
        ulong result when result <= 1 => result != 0,
        _ => throw new InvalidDataException("A canonical Boolean must be Boolean or the integer 0 or 1."),
    };

    private static long ToInt64(object value) => value switch
    {
        sbyte result => result,
        byte result => result,
        short result => result,
        ushort result => result,
        int result => result,
        uint result => result,
        long result => result,
        ulong result => checked((long)result),
        BigInteger result => checked((long)result),
        decimal result when decimal.Truncate(result) == result => checked((long)result),
        _ => throw new InvalidDataException("A canonical Int64 must be an in-range integral value."),
    };

    private static ulong ToUInt64(object value) => value switch
    {
        sbyte result => checked((ulong)result),
        byte result => result,
        short result => checked((ulong)result),
        ushort result => result,
        int result => checked((ulong)result),
        uint result => result,
        long result => checked((ulong)result),
        ulong result => result,
        BigInteger result => checked((ulong)result),
        decimal result when decimal.Truncate(result) == result => checked((ulong)result),
        _ => throw new InvalidDataException("A canonical UInt64 must be an in-range non-negative integral value."),
    };

    private static CanonicalDecimal ToDecimal(object value) => value switch
    {
        decimal result => new CanonicalDecimal(result),
        sbyte result => new CanonicalDecimal(new BigInteger(result), 0),
        byte result => new CanonicalDecimal(new BigInteger(result), 0),
        short result => new CanonicalDecimal(new BigInteger(result), 0),
        ushort result => new CanonicalDecimal(new BigInteger(result), 0),
        int result => new CanonicalDecimal(new BigInteger(result), 0),
        uint result => new CanonicalDecimal(new BigInteger(result), 0),
        long result => new CanonicalDecimal(new BigInteger(result), 0),
        ulong result => new CanonicalDecimal(new BigInteger(result), 0),
        BigInteger result => new CanonicalDecimal(result, 0),
        string result => ParseDecimal(result),
        _ => throw new InvalidDataException("A canonical Decimal must be an integral, decimal, or invariant decimal text value."),
    };

    private static CanonicalDecimal ParseDecimal(string text)
    {
        if (text.Length == 0 || text.Length > 100_000)
            throw new FormatException("Decimal text has an invalid length.");

        int index = 0;
        bool negative = false;
        if (text[index] is '+' or '-')
        {
            negative = text[index] == '-';
            index++;
        }

        int integerStart = index;
        while (index < text.Length && char.IsAsciiDigit(text[index]))
            index++;
        int integerDigits = index - integerStart;

        int fractionalStart = index;
        int fractionalDigits = 0;
        if (index < text.Length && text[index] == '.')
        {
            index++;
            fractionalStart = index;
            while (index < text.Length && char.IsAsciiDigit(text[index]))
                index++;
            fractionalDigits = index - fractionalStart;
        }

        if (integerDigits == 0 && fractionalDigits == 0)
            throw new FormatException("Decimal text has no digits.");

        int exponent = 0;
        if (index < text.Length && text[index] is 'e' or 'E')
        {
            index++;
            int exponentStart = index;
            if (index < text.Length && text[index] is '+' or '-')
                index++;
            int exponentDigitsStart = index;
            while (index < text.Length && char.IsAsciiDigit(text[index]))
                index++;
            if (exponentDigitsStart == index || index != text.Length)
                throw new FormatException("Decimal exponent is invalid.");
            if (!int.TryParse(
                    text.AsSpan(exponentStart),
                    NumberStyles.AllowLeadingSign,
                    CultureInfo.InvariantCulture,
                    out exponent) ||
                Math.Abs((long)exponent) > 100_000)
            {
                throw new FormatException("Decimal exponent is outside the bounded range.");
            }
        }
        else if (index != text.Length)
        {
            throw new FormatException("Decimal text contains an invalid character.");
        }

        string digits = string.Concat(
            text.AsSpan(integerStart, integerDigits),
            text.AsSpan(fractionalStart, fractionalDigits));
        BigInteger coefficient = BigInteger.Parse(digits, NumberStyles.None, CultureInfo.InvariantCulture);
        if (negative)
            coefficient = -coefficient;

        long scale = (long)fractionalDigits - exponent;
        if (scale < 0)
        {
            coefficient *= BigInteger.Pow(10, checked((int)-scale));
            scale = 0;
        }

        return new CanonicalDecimal(coefficient, checked((uint)scale));
    }

    private static float ToSingle(object value) => value switch
    {
        float result => result,
        double result => checked((float)result),
        decimal result => checked((float)result),
        _ => throw new InvalidDataException("A canonical Binary32 must be a floating-point value."),
    };

    private static double ToDouble(object value) => value switch
    {
        float result => result,
        double result => result,
        decimal result => checked((double)result),
        _ => throw new InvalidDataException("A canonical Binary64 must be a floating-point value."),
    };

    private static string ToText(object value) => value switch
    {
        string result => result,
        char result => result.ToString(),
        _ => throw new InvalidDataException("A canonical Text value must be text."),
    };

    private static ReadOnlyMemory<byte> ToBlob(object value) => value switch
    {
        byte[] result => result,
        Memory<byte> result => result,
        ReadOnlyMemory<byte> result => result,
        _ => throw new InvalidDataException("A canonical Blob value must be binary."),
    };

    private static Guid ToGuid(object value) => value switch
    {
        Guid result => result,
        string result when Guid.TryParseExact(result, "D", out Guid parsed) => parsed,
        _ => throw new InvalidDataException("A canonical Guid must be a Guid or D-format text."),
    };

    private static DateOnly ToDate(object value) => value switch
    {
        DateOnly result => result,
        DateTime result when result.TimeOfDay == TimeSpan.Zero => DateOnly.FromDateTime(result),
        string result when DateOnly.TryParseExact(
            result,
            "yyyy-MM-dd",
            CultureInfo.InvariantCulture,
            DateTimeStyles.None,
            out DateOnly parsed) => parsed,
        _ => throw new InvalidDataException("A canonical Date must be a date-only value or ISO date text."),
    };

    private static TimeOnly ToTime(object value) => value switch
    {
        TimeOnly result => result,
        TimeSpan result when result >= TimeSpan.Zero && result < TimeSpan.FromDays(1) =>
            TimeOnly.FromTimeSpan(result),
        string result when TimeOnly.TryParseExact(
            result,
            ["HH:mm:ss", "HH:mm:ss.FFFFFFF"],
            CultureInfo.InvariantCulture,
            DateTimeStyles.None,
            out TimeOnly parsed) => parsed,
        _ => throw new InvalidDataException("A canonical Time must be a time-only value or ISO time text."),
    };

    private static DateTime ToWallDateTime(object value)
    {
        DateTime result = value switch
        {
            DateTime dateTime when dateTime.Kind == DateTimeKind.Unspecified => dateTime,
            string text when DateTime.TryParseExact(
                text,
                ["yyyy-MM-dd'T'HH:mm:ss", "yyyy-MM-dd'T'HH:mm:ss.FFFFFFF",
                 "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd HH:mm:ss.FFFFFFF"],
                CultureInfo.InvariantCulture,
                DateTimeStyles.None,
                out DateTime parsed) => parsed,
            _ => throw new InvalidDataException(
                "A canonical wall date-time must have unspecified kind or be ISO wall-time text."),
        };
        return DateTime.SpecifyKind(result, DateTimeKind.Unspecified);
    }

    private static DateTimeOffset ToUtcInstant(object value) => value switch
    {
        DateTimeOffset result => result.ToUniversalTime(),
        DateTime result when result.Kind == DateTimeKind.Utc => new DateTimeOffset(result),
        string result when DateTimeOffset.TryParseExact(
            result,
            ["yyyy-MM-dd'T'HH:mm:ss'Z'", "yyyy-MM-dd'T'HH:mm:ss.FFFFFFF'Z'",
             "yyyy-MM-dd'T'HH:mm:ssK", "yyyy-MM-dd'T'HH:mm:ss.FFFFFFFK"],
            CultureInfo.InvariantCulture,
            DateTimeStyles.None,
            out DateTimeOffset parsed) => parsed.ToUniversalTime(),
        _ => throw new InvalidDataException(
            "A canonical UTC instant must carry an explicit UTC or offset designation."),
    };

    private static DateTimeOffset ToOffsetDateTime(object value) => value switch
    {
        DateTimeOffset result => result,
        string result when DateTimeOffset.TryParseExact(
            result,
            ["yyyy-MM-dd'T'HH:mm:sszzz", "yyyy-MM-dd'T'HH:mm:ss.FFFFFFFzzz"],
            CultureInfo.InvariantCulture,
            DateTimeStyles.None,
            out DateTimeOffset parsed) => parsed,
        _ => throw new InvalidDataException(
            "A canonical offset date-time must carry an explicit numeric offset."),
    };
}
