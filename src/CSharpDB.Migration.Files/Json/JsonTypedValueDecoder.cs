using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using CSharpDB.Migration;
using CSharpDB.Primitives;

namespace CSharpDB.Migration.Files.Json;

internal static class JsonTypedValueDecoder
{
    internal const string AlgorithmId =
        "csharpdb-json-typed-value/v1";

    internal static bool TryDecode(
        JsonLogicalValue logicalValue,
        JsonTypedColumnIntent intent,
        JsonTypedIntentManifest manifest,
        int maximumValueBytes,
        CancellationToken cancellationToken,
        out JsonTypedDecodedValue decoded)
    {
        ArgumentNullException.ThrowIfNull(logicalValue);
        ArgumentNullException.ThrowIfNull(intent);
        ArgumentNullException.ThrowIfNull(manifest);
        cancellationToken.ThrowIfCancellationRequested();

        decoded = default;
        return intent.Codec switch
        {
            JsonTypedValueCodec.BinaryBase64 =>
                TryDecodeBinary(
                    logicalValue,
                    manifest.MaxDecodedBinaryBytes,
                    maximumValueBytes,
                    cancellationToken,
                    out decoded),
            JsonTypedValueCodec.DecimalString =>
                TryDecodeDecimal(
                    logicalValue,
                    JsonLogicalValueKind.String,
                    intent,
                    manifest.MaxDecimalDigits,
                    maximumValueBytes,
                    cancellationToken,
                    out decoded),
            JsonTypedValueCodec.DecimalNumber =>
                TryDecodeDecimal(
                    logicalValue,
                    JsonLogicalValueKind.Number,
                    intent,
                    manifest.MaxDecimalDigits,
                    maximumValueBytes,
                    cancellationToken,
                    out decoded),
            JsonTypedValueCodec.GuidD =>
                TryDecodeCanonicalText(
                    logicalValue,
                    MigrationSourceValueKind.Guid,
                    CSharpDbTextCodec.ParseGuid,
                    CSharpDbTextCodec.FormatGuid,
                    maximumValueBytes,
                    cancellationToken,
                    out decoded),
            JsonTypedValueCodec.DateCSharpDbText =>
                TryDecodeCanonicalText(
                    logicalValue,
                    MigrationSourceValueKind.Date,
                    CSharpDbTextCodec.ParseDate,
                    CSharpDbTextCodec.FormatDate,
                    maximumValueBytes,
                    cancellationToken,
                    out decoded),
            JsonTypedValueCodec.TimeCSharpDbText =>
                TryDecodeCanonicalText(
                    logicalValue,
                    MigrationSourceValueKind.Time,
                    CSharpDbTextCodec.ParseTime,
                    CSharpDbTextCodec.FormatTime,
                    maximumValueBytes,
                    cancellationToken,
                    out decoded),
            JsonTypedValueCodec.DateTimeCSharpDbText =>
                TryDecodeCanonicalText(
                    logicalValue,
                    MigrationSourceValueKind.DateTime,
                    CSharpDbTextCodec.ParseDateTime,
                    CSharpDbTextCodec.FormatDateTime,
                    maximumValueBytes,
                    cancellationToken,
                    out decoded),
            JsonTypedValueCodec
                .DateTimeOffsetCSharpDbText =>
                TryDecodeCanonicalText(
                    logicalValue,
                    MigrationSourceValueKind.DateTimeOffset,
                    CSharpDbTextCodec.ParseDateTimeOffset,
                    CSharpDbTextCodec
                        .FormatDateTimeOffset,
                    maximumValueBytes,
                    cancellationToken,
                    out decoded),
            JsonTypedValueCodec.Int64String =>
                TryDecodeInt64(
                    logicalValue,
                    maximumValueBytes,
                    cancellationToken,
                    out decoded),
            JsonTypedValueCodec.UInt64String =>
                TryDecodeUInt64(
                    logicalValue,
                    maximumValueBytes,
                    cancellationToken,
                    out decoded),
            _ => throw new ArgumentOutOfRangeException(
                nameof(intent)),
        };
    }

    private static bool TryDecodeBinary(
        JsonLogicalValue value,
        int manifestLimit,
        int maximumValueBytes,
        CancellationToken cancellationToken,
        out JsonTypedDecodedValue decoded)
    {
        decoded = default;
        if (value.Kind != JsonLogicalValueKind.String)
            return false;

        string text = value.StringValue;
        long maximumEncodedCharacters =
            checked((manifestLimit + 2L) / 3L * 4L);
        if (text.Length > maximumEncodedCharacters)
            throw new JsonTypedValueSizeException();
        if (!TryGetBase64DecodedLength(
                text,
                out int decodedLength))
        {
            return false;
        }
        RequireWithinValueLimit(
            decodedLength,
            manifestLimit,
            maximumValueBytes);

        byte[] bytes = new byte[decodedLength];
        bool transferred = false;
        try
        {
            if (!Convert.TryFromBase64String(
                    text,
                    bytes,
                    out int written) ||
                written != decodedLength ||
                !string.Equals(
                    Convert.ToBase64String(bytes),
                    text,
                    StringComparison.Ordinal))
            {
                return false;
            }

            cancellationToken.ThrowIfCancellationRequested();
            decoded = new JsonTypedDecodedValue(
                new MigrationSourceValue
                {
                    Kind = MigrationSourceValueKind.Binary,
                    BinaryValue = bytes,
                },
                checked(5L + decodedLength));
            transferred = true;
            return true;
        }
        finally
        {
            if (!transferred)
                CryptographicOperations.ZeroMemory(bytes);
        }
    }

    private static bool TryDecodeDecimal(
        JsonLogicalValue value,
        JsonLogicalValueKind requiredKind,
        JsonTypedColumnIntent intent,
        int manifestLimit,
        int maximumValueBytes,
        CancellationToken cancellationToken,
        out JsonTypedDecodedValue decoded)
    {
        decoded = default;
        if (value.Kind != requiredKind)
            return false;

        string text = requiredKind ==
            JsonLogicalValueKind.String
            ? value.StringValue
            : value.NumberLexeme;
        if (!TryGetDecimalShape(
                text,
                manifestLimit,
                maximumValueBytes,
                cancellationToken,
                out int integralDigits,
                out int scale))
        {
            return false;
        }

        int precision = intent.Precision ??
            throw new InvalidDataException(
                "A retained decimal intent is missing precision.");
        int maximumScale = intent.Scale ??
            throw new InvalidDataException(
                "A retained decimal intent is missing scale.");
        if (integralDigits >
                precision - maximumScale ||
            scale > maximumScale)
        {
            return false;
        }

        long canonicalBytes = checked(5L + text.Length);
        decoded = new JsonTypedDecodedValue(
            new MigrationSourceValue
            {
                Kind = MigrationSourceValueKind.Decimal,
                CanonicalText = text,
            },
            Math.Max(9L, canonicalBytes));
        return true;
    }

    private static bool TryDecodeCanonicalText<T>(
        JsonLogicalValue value,
        MigrationSourceValueKind sourceKind,
        Func<string, T> parse,
        Func<T, string> format,
        int maximumValueBytes,
        CancellationToken cancellationToken,
        out JsonTypedDecodedValue decoded)
    {
        decoded = default;
        if (value.Kind != JsonLogicalValueKind.String)
            return false;

        string text = value.StringValue;
        long canonicalBytes = CanonicalTextBytes(
            text,
            cancellationToken);
        RequireWithinRequestLimit(
            canonicalBytes,
            maximumValueBytes);

        try
        {
            T parsed = parse(text);
            if (!string.Equals(
                    format(parsed),
                    text,
                    StringComparison.Ordinal))
            {
                return false;
            }
        }
        catch (Exception exception) when (
            exception is ArgumentException or
            FormatException or
            OverflowException)
        {
            return false;
        }

        decoded = new JsonTypedDecodedValue(
            new MigrationSourceValue
            {
                Kind = sourceKind,
                CanonicalText = text,
            },
            canonicalBytes);
        return true;
    }

    private static bool TryDecodeInt64(
        JsonLogicalValue value,
        int maximumValueBytes,
        CancellationToken cancellationToken,
        out JsonTypedDecodedValue decoded)
    {
        decoded = default;
        if (value.Kind != JsonLogicalValueKind.String)
            return false;

        string text = value.StringValue;
        long canonicalBytes = CanonicalTextBytes(
            text,
            cancellationToken);
        RequireWithinRequestLimit(
            canonicalBytes,
            maximumValueBytes);
        if (!long.TryParse(
                text,
                NumberStyles.AllowLeadingSign,
                CultureInfo.InvariantCulture,
                out long parsed) ||
            !string.Equals(
                parsed.ToString(
                    CultureInfo.InvariantCulture),
                text,
                StringComparison.Ordinal))
        {
            return false;
        }

        decoded = new JsonTypedDecodedValue(
            new MigrationSourceValue
            {
                Kind =
                    MigrationSourceValueKind.SignedInteger,
                CanonicalText = text,
            },
            Math.Max(9L, canonicalBytes));
        return true;
    }

    private static bool TryDecodeUInt64(
        JsonLogicalValue value,
        int maximumValueBytes,
        CancellationToken cancellationToken,
        out JsonTypedDecodedValue decoded)
    {
        decoded = default;
        if (value.Kind != JsonLogicalValueKind.String)
            return false;

        string text = value.StringValue;
        long canonicalBytes = CanonicalTextBytes(
            text,
            cancellationToken);
        RequireWithinRequestLimit(
            canonicalBytes,
            maximumValueBytes);
        if (!ulong.TryParse(
                text,
                NumberStyles.None,
                CultureInfo.InvariantCulture,
                out ulong parsed) ||
            !string.Equals(
                parsed.ToString(
                    CultureInfo.InvariantCulture),
                text,
                StringComparison.Ordinal))
        {
            return false;
        }

        decoded = new JsonTypedDecodedValue(
            new MigrationSourceValue
            {
                Kind =
                    MigrationSourceValueKind.UnsignedInteger,
                CanonicalText = text,
            },
            Math.Max(9L, canonicalBytes));
        return true;
    }

    private static bool TryGetBase64DecodedLength(
        string text,
        out int decodedLength)
    {
        decodedLength = 0;
        if (text.Length == 0)
            return true;
        if ((text.Length & 3) != 0)
            return false;

        int padding = text[^1] == '=' ? 1 : 0;
        if (text.Length >= 2 && text[^2] == '=')
            padding++;
        try
        {
            decodedLength = checked(
                text.Length / 4 * 3 - padding);
            return decodedLength >= 0;
        }
        catch (OverflowException)
        {
            throw new JsonTypedValueSizeException();
        }
    }

    private static bool TryGetDecimalShape(
        string text,
        int manifestLimit,
        int maximumValueBytes,
        CancellationToken cancellationToken,
        out int integralDigits,
        out int scale)
    {
        integralDigits = 0;
        scale = 0;
        if (text.Length == 0)
            return false;
        if (text.Length >
            checked(manifestLimit + 2))
        {
            throw new JsonTypedValueSizeException();
        }

        RequireWithinRequestLimit(
            checked(5L + text.Length),
            maximumValueBytes);
        int index = text[0] == '-' ? 1 : 0;
        if (index == text.Length ||
            text[0] == '+')
        {
            return false;
        }

        int integerStart = index;
        if (text[index] == '0')
        {
            index++;
            if (index < text.Length &&
                text[index] is >= '0' and <= '9')
            {
                return false;
            }
        }
        else
        {
            if (text[index] is < '1' or > '9')
                return false;
            do
            {
                cancellationToken
                    .ThrowIfCancellationRequested();
                index++;
            }
            while (index < text.Length &&
                   text[index] is >= '0' and <= '9');
        }

        int integerLength = index - integerStart;
        bool zeroInteger =
            integerLength == 1 &&
            text[integerStart] == '0';
        integralDigits = zeroInteger
            ? 0
            : integerLength;
        int digitCount = integerLength;

        if (index < text.Length)
        {
            if (text[index] != '.')
                return false;
            index++;
            int fractionStart = index;
            while (index < text.Length &&
                   text[index] is >= '0' and <= '9')
            {
                cancellationToken
                    .ThrowIfCancellationRequested();
                index++;
            }
            scale = index - fractionStart;
            if (scale == 0 ||
                text[index - 1] == '0')
            {
                return false;
            }
            digitCount = checked(digitCount + scale);
        }

        if (index != text.Length ||
            text[0] == '-' &&
            zeroInteger &&
            scale == 0)
        {
            return false;
        }
        if (digitCount > manifestLimit)
            throw new JsonTypedValueSizeException();

        return true;
    }

    private static long CanonicalTextBytes(
        string text,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        long bytes = checked(
            5L + Encoding.UTF8.GetByteCount(text));
        cancellationToken.ThrowIfCancellationRequested();
        return bytes;
    }

    private static void RequireWithinValueLimit(
        int decodedLength,
        int manifestLimit,
        int maximumValueBytes)
    {
        if (decodedLength > manifestLimit)
            throw new JsonTypedValueSizeException();
        RequireWithinRequestLimit(
            checked(5L + decodedLength),
            maximumValueBytes);
    }

    private static void RequireWithinRequestLimit(
        long canonicalBytes,
        int maximumValueBytes)
    {
        if (canonicalBytes > maximumValueBytes)
            throw new JsonTypedValueSizeException();
    }
}

internal readonly record struct JsonTypedDecodedValue(
    MigrationSourceValue Value,
    long CanonicalBatchBytes);

internal sealed class JsonTypedValueSizeException : Exception;
