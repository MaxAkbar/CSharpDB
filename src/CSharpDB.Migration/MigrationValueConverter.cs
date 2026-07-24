using System.Globalization;
using System.Text;
using CSharpDB.Primitives;

namespace CSharpDB.Migration;

public sealed class MigrationValueException : Exception
{
    public MigrationValueException(string code, string objectId, long rowOrdinal, string message)
        : base($"{code} at object '{objectId}', row {rowOrdinal}: {message}")
    {
        Code = code;
        ObjectId = objectId;
        RowOrdinal = rowOrdinal;
    }

    public string Code { get; }

    public string ObjectId { get; }

    public long RowOrdinal { get; }
}

/// <summary>
/// Executes the versioned standard mapping conversions and validates every
/// streamed source value before it reaches a target transaction.
/// </summary>
public static class MigrationValueConverter
{
    public static DbValue Convert(
        MigrationSourceValue source,
        MigrationCatalogObject column,
        MigrationTypeMapping mapping,
        long rowOrdinal)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(column);
        ArgumentNullException.ThrowIfNull(mapping);

        if (source.Kind == MigrationSourceValueKind.Null)
        {
            RequireNoPayload(source, column.ObjectId, rowOrdinal);
            if (!IsNullable(column))
                throw Error("MIG-APPLY-NULL-001", column, rowOrdinal, "NULL is not allowed by the planned column.");
            return DbValue.Null;
        }

        if (mapping.TargetType is not DbType targetType || targetType == DbType.Null)
            throw Error("MIG-APPLY-MAPPING-001", column, rowOrdinal, "The planned mapping has no persistent target type.");

        DbValue result = mapping.Conversion is null
            ? ConvertExact(source, targetType, column, rowOrdinal)
            : ConvertVersioned(source, mapping.Conversion, column, rowOrdinal);

        if (result.Type != targetType)
        {
            throw Error(
                "MIG-APPLY-TAG-001",
                column,
                rowOrdinal,
                $"Conversion produced '{result.Type}' but the plan requires '{targetType}'.");
        }
        if (result.Type == DbType.Real && !double.IsFinite(result.AsReal))
            throw Error("MIG-APPLY-REAL-001", column, rowOrdinal, "Non-finite REAL values are not supported.");

        ValidateTargetFacets(result, column, rowOrdinal);
        return result;
    }

    public static int GetCanonicalByteCount(DbValue value) => value.Type switch
    {
        DbType.Null => 1,
        DbType.Integer or DbType.Real => 9,
        DbType.Text => checked(5 + Encoding.UTF8.GetByteCount(value.AsText)),
        DbType.Blob => checked(5 + value.AsBlob.Length),
        _ => throw new InvalidDataException($"Unsupported target value tag '{value.Type}'."),
    };

    private static DbValue ConvertExact(
        MigrationSourceValue source,
        DbType targetType,
        MigrationCatalogObject column,
        long rowOrdinal) => targetType switch
    {
        DbType.Integer when source.Kind == MigrationSourceValueKind.SignedInteger =>
            DbValue.FromInteger(ParseInt64(Text(source, column, rowOrdinal), column, rowOrdinal)),
        DbType.Real when source.Kind == MigrationSourceValueKind.FloatingPoint =>
            DbValue.FromReal(ParseFiniteDouble(Text(source, column, rowOrdinal), column, rowOrdinal)),
        DbType.Text when source.Kind == MigrationSourceValueKind.Text =>
            DbValue.FromText(Text(source, column, rowOrdinal)),
        DbType.Blob when source.Kind == MigrationSourceValueKind.Binary =>
            DbValue.FromBlob(source.BinaryValue.ToArray()),
        _ => throw Error(
            "MIG-APPLY-KIND-001",
            column,
            rowOrdinal,
            $"Source value kind '{source.Kind}' is incompatible with exact target type '{targetType}'."),
    };

    private static DbValue ConvertVersioned(
        MigrationSourceValue source,
        MigrationConversionDescriptor conversion,
        MigrationCatalogObject column,
        long rowOrdinal)
    {
        if (conversion.Version != 1)
        {
            throw Error(
                "MIG-APPLY-CONVERSION-001",
                column,
                rowOrdinal,
                $"Conversion '{conversion.ConversionId}' version {conversion.Version} is not executable.");
        }

        string text = source.Kind == MigrationSourceValueKind.Binary
            ? string.Empty
            : Text(source, column, rowOrdinal);
        return conversion.ConversionId switch
        {
            "boolean-integer" => ConvertBoolean(source, text, column, rowOrdinal),
            "guid-text" => RequireKind(source, MigrationSourceValueKind.Guid, column, rowOrdinal,
                value => DbValue.FromText(CSharpDbTextCodec.FormatGuid(Guid.Parse(value)))),
            "date-text" => RequireKind(source, MigrationSourceValueKind.Date, column, rowOrdinal,
                value => DbValue.FromText(CSharpDbTextCodec.FormatDate(CSharpDbTextCodec.ParseDate(value)))),
            "time-text" => RequireKind(source, MigrationSourceValueKind.Time, column, rowOrdinal,
                value => DbValue.FromText(CSharpDbTextCodec.FormatTime(CSharpDbTextCodec.ParseTime(value)))),
            "datetime-text" => RequireKind(source, MigrationSourceValueKind.DateTime, column, rowOrdinal,
                value => DbValue.FromText(CSharpDbTextCodec.FormatDateTime(CSharpDbTextCodec.ParseDateTime(value)))),
            "datetimeoffset-text" => RequireKind(source, MigrationSourceValueKind.DateTimeOffset, column, rowOrdinal,
                value => DbValue.FromText(CSharpDbTextCodec.FormatDateTimeOffset(CSharpDbTextCodec.ParseDateTimeOffset(value)))),
            "decimal-scaled-int64" => ConvertScaledDecimal(source, text, conversion, column, rowOrdinal),
            "decimal-text" => ConvertDecimalText(source, text, conversion, column, rowOrdinal),
            "json-typed-decimal-text" =>
                ConvertTypedJsonDecimalText(
                    source,
                    text,
                    conversion,
                    column,
                    rowOrdinal),
            "unsigned-integer-text" => ConvertUnsignedText(source, text, column, rowOrdinal),
            "unsigned-integer-binary64" => ConvertUnsignedReal(source, text, column, rowOrdinal),
            "decimal-binary64" or "numeric-binary64" => ConvertNumericReal(source, text, column, rowOrdinal),
            "canonical-text" => ConvertCanonicalText(source, text, column, rowOrdinal),
            _ => throw Error(
                "MIG-APPLY-CONVERSION-001",
                column,
                rowOrdinal,
                $"Conversion '{conversion.ConversionId}' version {conversion.Version} is not executable."),
        };
    }

    private static DbValue ConvertBoolean(
        MigrationSourceValue source,
        string text,
        MigrationCatalogObject column,
        long rowOrdinal)
    {
        RequireSourceKind(source, MigrationSourceValueKind.Boolean, column, rowOrdinal);
        return text switch
        {
            "true" => DbValue.FromInteger(1),
            "false" => DbValue.FromInteger(0),
            _ => throw Error("MIG-APPLY-BOOLEAN-001", column, rowOrdinal, "BOOLEAN text must be exactly 'true' or 'false'."),
        };
    }

    private static DbValue ConvertScaledDecimal(
        MigrationSourceValue source,
        string text,
        MigrationConversionDescriptor conversion,
        MigrationCatalogObject column,
        long rowOrdinal)
    {
        RequireSourceKind(source, MigrationSourceValueKind.Decimal, column, rowOrdinal);
        int precision = ParameterInt(conversion, "precision", column, rowOrdinal);
        int scale = ParameterInt(conversion, "scale", column, rowOrdinal);
        if (!decimal.TryParse(text, NumberStyles.AllowLeadingSign | NumberStyles.AllowDecimalPoint,
                CultureInfo.InvariantCulture, out decimal value))
        {
            throw Error("MIG-APPLY-DECIMAL-001", column, rowOrdinal, "DECIMAL text is not a supported invariant value.");
        }

        try
        {
            return DbValue.FromInteger(CSharpDbDecimalCodec.ToScaledInt64(value, precision, scale));
        }
        catch (Exception ex) when (ex is OverflowException or InvalidOperationException or NotSupportedException)
        {
            throw Error(
                "MIG-APPLY-DECIMAL-002",
                column,
                rowOrdinal,
                "DECIMAL precision or scale exceeds the planned exact target representation.");
        }
    }

    private static DbValue ConvertDecimalText(
        MigrationSourceValue source,
        string text,
        MigrationConversionDescriptor conversion,
        MigrationCatalogObject column,
        long rowOrdinal)
    {
        RequireSourceKind(source, MigrationSourceValueKind.Decimal, column, rowOrdinal);
        if (!TryCanonicalDecimal(text, out string? canonical, out int precision, out int scale))
            throw Error("MIG-APPLY-DECIMAL-001", column, rowOrdinal, "DECIMAL text is not a supported invariant value.");

        int? plannedPrecision = OptionalParameterInt(conversion, "precision", column, rowOrdinal);
        int? plannedScale = OptionalParameterInt(conversion, "scale", column, rowOrdinal);
        if (plannedPrecision is int maximumPrecision && precision > maximumPrecision)
            throw Error("MIG-APPLY-DECIMAL-002", column, rowOrdinal, "DECIMAL precision exceeds the planned source facet.");
        if (plannedScale is int maximumScale && scale > maximumScale)
            throw Error("MIG-APPLY-DECIMAL-002", column, rowOrdinal, "DECIMAL scale exceeds the planned source facet.");

        return DbValue.FromText(canonical!);
    }

    private static DbValue ConvertTypedJsonDecimalText(
        MigrationSourceValue source,
        string text,
        MigrationConversionDescriptor conversion,
        MigrationCatalogObject column,
        long rowOrdinal)
    {
        RequireSourceKind(
            source,
            MigrationSourceValueKind.Decimal,
            column,
            rowOrdinal);
        int precision = ParameterInt(
            conversion,
            "precision",
            column,
            rowOrdinal);
        int maximumScale = ParameterInt(
            conversion,
            "scale",
            column,
            rowOrdinal);
        string? contract = conversion.Parameters
            .FirstOrDefault(
                parameter =>
                    string.Equals(
                        parameter.Name,
                        "contract",
                        StringComparison.Ordinal))
            ?.Value;
        if (precision < 1 ||
            maximumScale < 0 ||
            maximumScale > precision ||
            !string.Equals(
                contract,
                "csharpdb-json-typed-value/v1",
                StringComparison.Ordinal) ||
            !TryCanonicalTypedJsonDecimal(
                text,
                out int integralDigits,
                out int actualScale) ||
            integralDigits > precision - maximumScale ||
            actualScale > maximumScale)
        {
            throw Error(
                "MIG-APPLY-DECIMAL-002",
                column,
                rowOrdinal,
                "Typed JSON decimal text does not match the planned canonical precision and scale.");
        }

        return DbValue.FromText(text);
    }

    private static DbValue ConvertUnsignedText(
        MigrationSourceValue source,
        string text,
        MigrationCatalogObject column,
        long rowOrdinal)
    {
        RequireSourceKind(source, MigrationSourceValueKind.UnsignedInteger, column, rowOrdinal);
        if (!ulong.TryParse(text, NumberStyles.None, CultureInfo.InvariantCulture, out ulong value))
            throw Error("MIG-APPLY-UINT-001", column, rowOrdinal, "Unsigned integer text is outside UInt64 or is not canonical base-10.");
        return DbValue.FromText(value.ToString(CultureInfo.InvariantCulture));
    }

    private static DbValue ConvertUnsignedReal(
        MigrationSourceValue source,
        string text,
        MigrationCatalogObject column,
        long rowOrdinal)
    {
        RequireSourceKind(source, MigrationSourceValueKind.UnsignedInteger, column, rowOrdinal);
        if (!ulong.TryParse(text, NumberStyles.None, CultureInfo.InvariantCulture, out ulong value))
            throw Error("MIG-APPLY-UINT-001", column, rowOrdinal, "Unsigned integer text is outside UInt64 or is not canonical base-10.");
        return DbValue.FromReal(value);
    }

    private static DbValue ConvertNumericReal(
        MigrationSourceValue source,
        string text,
        MigrationCatalogObject column,
        long rowOrdinal)
    {
        if (source.Kind is not (MigrationSourceValueKind.SignedInteger or
            MigrationSourceValueKind.UnsignedInteger or MigrationSourceValueKind.Decimal))
        {
            throw Error("MIG-APPLY-KIND-001", column, rowOrdinal, "The numeric conversion received a non-numeric source kind.");
        }

        return DbValue.FromReal(ParseFiniteDouble(text, column, rowOrdinal));
    }

    private static DbValue ConvertCanonicalText(
        MigrationSourceValue source,
        string text,
        MigrationCatalogObject column,
        long rowOrdinal)
    {
        if (source.Kind is MigrationSourceValueKind.Binary or MigrationSourceValueKind.Native)
            throw Error("MIG-APPLY-KIND-001", column, rowOrdinal, "The source kind cannot use canonical text conversion.");
        return DbValue.FromText(text);
    }

    private static DbValue RequireKind(
        MigrationSourceValue source,
        MigrationSourceValueKind expected,
        MigrationCatalogObject column,
        long rowOrdinal,
        Func<string, DbValue> convert)
    {
        RequireSourceKind(source, expected, column, rowOrdinal);
        try
        {
            return convert(Text(source, column, rowOrdinal));
        }
        catch (Exception ex) when (ex is FormatException or ArgumentException or OverflowException)
        {
            throw Error("MIG-APPLY-TEXT-CODEC-001", column, rowOrdinal, "Logical text does not match the planned codec.");
        }
    }

    private static void ValidateTargetFacets(DbValue value, MigrationCatalogObject column, long rowOrdinal)
    {
        if (value.Type != DbType.Text)
            return;

        string? maxLengthText = Facet(column, "maxLength");
        if (int.TryParse(maxLengthText, NumberStyles.None, CultureInfo.InvariantCulture, out int maxLength) &&
            value.AsText.Length > maxLength)
        {
            throw Error("MIG-APPLY-LENGTH-001", column, rowOrdinal, "TEXT length exceeds the planned source facet.");
        }
    }

    private static string Text(MigrationSourceValue source, MigrationCatalogObject column, long rowOrdinal)
    {
        if (source.CanonicalText is null)
            throw Error("MIG-APPLY-VALUE-001", column, rowOrdinal, "A non-null source value is missing canonical text.");
        if (!source.BinaryValue.IsEmpty)
            throw Error("MIG-APPLY-VALUE-001", column, rowOrdinal, "A non-binary source value carries an unexpected binary payload.");
        return source.CanonicalText;
    }

    private static void RequireNoPayload(MigrationSourceValue source, string objectId, long rowOrdinal)
    {
        if (source.CanonicalText is not null || !source.BinaryValue.IsEmpty)
            throw new MigrationValueException("MIG-APPLY-VALUE-001", objectId, rowOrdinal, "NULL carries an unexpected payload.");
    }

    private static void RequireSourceKind(
        MigrationSourceValue source,
        MigrationSourceValueKind expected,
        MigrationCatalogObject column,
        long rowOrdinal)
    {
        if (source.Kind != expected)
        {
            throw Error(
                "MIG-APPLY-KIND-001",
                column,
                rowOrdinal,
                $"Source value kind '{source.Kind}' does not match planned logical kind '{expected}'.");
        }
    }

    private static long ParseInt64(string text, MigrationCatalogObject column, long rowOrdinal)
    {
        if (!long.TryParse(text, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out long value))
            throw Error("MIG-APPLY-INT-001", column, rowOrdinal, "Signed integer text is outside Int64 or is not invariant base-10.");
        return value;
    }

    private static double ParseFiniteDouble(string text, MigrationCatalogObject column, long rowOrdinal)
    {
        if (text.Length != text.Trim().Length ||
            !double.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out double value) ||
            !double.IsFinite(value))
        {
            throw Error("MIG-APPLY-REAL-001", column, rowOrdinal, "Numeric text is not a finite invariant REAL value.");
        }

        return value;
    }

    private static int ParameterInt(
        MigrationConversionDescriptor conversion,
        string name,
        MigrationCatalogObject column,
        long rowOrdinal) => OptionalParameterInt(conversion, name, column, rowOrdinal) ??
        throw Error("MIG-APPLY-CONVERSION-002", column, rowOrdinal, $"Conversion parameter '{name}' is required.");

    private static int? OptionalParameterInt(
        MigrationConversionDescriptor conversion,
        string name,
        MigrationCatalogObject column,
        long rowOrdinal)
    {
        string? text = conversion.Parameters.FirstOrDefault(parameter => parameter.Name == name)?.Value;
        if (text is null || string.Equals(text, "unspecified", StringComparison.Ordinal))
            return null;
        if (!int.TryParse(text, NumberStyles.None, CultureInfo.InvariantCulture, out int value) || value < 0)
            throw Error("MIG-APPLY-CONVERSION-002", column, rowOrdinal, $"Conversion parameter '{name}' is invalid.");
        return value;
    }

    private static bool TryCanonicalDecimal(
        string text,
        out string? canonical,
        out int precision,
        out int scale)
    {
        canonical = null;
        precision = 0;
        scale = 0;
        if (string.IsNullOrEmpty(text) || text.Length != text.Trim().Length)
            return false;

        bool negative = text[0] == '-';
        int offset = negative || text[0] == '+' ? 1 : 0;
        if (offset == text.Length)
            return false;
        int dot = text.IndexOf('.', offset);
        if (dot >= 0 && text.IndexOf('.', dot + 1) >= 0)
            return false;

        string integer = dot < 0 ? text[offset..] : text[offset..dot];
        string fraction = dot < 0 ? string.Empty : text[(dot + 1)..];
        if (integer.Length == 0 || integer.Any(character => character is < '0' or > '9') ||
            fraction.Any(character => character is < '0' or > '9'))
        {
            return false;
        }

        integer = integer.TrimStart('0');
        if (integer.Length == 0)
            integer = "0";
        fraction = fraction.TrimEnd('0');
        bool zero = integer == "0" && fraction.Length == 0;
        canonical = (negative && !zero ? "-" : string.Empty) + integer +
            (fraction.Length == 0 ? string.Empty : "." + fraction);
        scale = fraction.Length;
        precision = checked(integer.Length + fraction.Length);
        return true;
    }

    private static bool TryCanonicalTypedJsonDecimal(
        string text,
        out int integralDigits,
        out int scale)
    {
        integralDigits = 0;
        scale = 0;
        if (string.IsNullOrEmpty(text))
            return false;

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
            while (index < text.Length &&
                   text[index] is >= '0' and <= '9')
            {
                index++;
            }
        }

        int integerLength = index - integerStart;
        bool zeroInteger =
            integerLength == 1 &&
            text[integerStart] == '0';
        integralDigits = zeroInteger
            ? 0
            : integerLength;

        if (index < text.Length)
        {
            if (text[index] != '.')
                return false;
            index++;
            int fractionStart = index;
            while (index < text.Length &&
                   text[index] is >= '0' and <= '9')
            {
                index++;
            }
            scale = index - fractionStart;
            if (scale == 0 ||
                text[index - 1] == '0')
            {
                return false;
            }
        }

        return index == text.Length &&
            !(text[0] == '-' &&
              zeroInteger &&
              scale == 0);
    }

    private static bool IsNullable(MigrationCatalogObject column) =>
        !bool.TryParse(Facet(column, "nullable"), out bool nullable) || nullable;

    private static string? Facet(MigrationCatalogObject column, string name) =>
        column.Facets.FirstOrDefault(facet => string.Equals(facet.Name, name, StringComparison.Ordinal))?.Value;

    private static MigrationValueException Error(
        string code,
        MigrationCatalogObject column,
        long rowOrdinal,
        string message) => new(code, column.ObjectId, rowOrdinal, message);
}
