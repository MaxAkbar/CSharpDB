using System.Globalization;
using System.Text;
using CSharpDB.Primitives;
using MySqlConnector;

namespace CSharpDB.Migration.MySql;

internal enum MySqlScalarCodecKind
{
    SignedInteger,
    UnsignedInteger,
    Decimal,
    Binary32,
    Binary64,
    Text,
    Binary,
    Date,
    DateTime,
}

internal readonly record struct MySqlProjectedScalar(
    MigrationSourceValue Value,
    int PayloadBytes);

internal static class MySqlScalarCodec
{
    private static readonly UTF8Encoding StrictUtf8 =
        new(
            encoderShouldEmitUTF8Identifier: false,
            throwOnInvalidBytes: true);

    internal static bool TryResolve(
        string dataType,
        bool unsigned,
        out MySqlScalarCodecKind codec)
    {
        switch (dataType.ToLowerInvariant())
        {
            case "tinyint":
            case "smallint":
            case "mediumint":
            case "int":
            case "integer":
            case "bigint":
                codec = unsigned
                    ? MySqlScalarCodecKind.UnsignedInteger
                    : MySqlScalarCodecKind.SignedInteger;
                return true;
            case "decimal":
            case "numeric":
                codec = MySqlScalarCodecKind.Decimal;
                return true;
            case "float":
                codec = MySqlScalarCodecKind.Binary32;
                return true;
            case "double":
            case "real":
                codec = MySqlScalarCodecKind.Binary64;
                return true;
            case "char":
            case "varchar":
            case "tinytext":
            case "text":
            case "mediumtext":
            case "longtext":
                codec = MySqlScalarCodecKind.Text;
                return true;
            case "binary":
            case "varbinary":
            case "tinyblob":
            case "blob":
            case "mediumblob":
            case "longblob":
                codec = MySqlScalarCodecKind.Binary;
                return true;
            case "date":
                codec = MySqlScalarCodecKind.Date;
                return true;
            case "datetime":
                codec = MySqlScalarCodecKind.DateTime;
                return true;
            default:
                codec = default;
                return false;
        }
    }

    internal static MySqlProjectedScalar Read(
        MySqlDataReader reader,
        int lengthOrdinal,
        int valueOrdinal,
        MySqlRetainedColumnBinding column,
        int maximumValueBytes)
    {
        ArgumentNullException.ThrowIfNull(reader);
        ArgumentNullException.ThrowIfNull(column);
        if (column.Codec is not MySqlScalarCodecKind codec)
        {
            throw new MySqlMigrationException(
                "A retained MySQL column has no scalar codec.");
        }

        bool nullLength = reader.IsDBNull(lengthOrdinal);
        long sourceBytes = 0;
        if (!nullLength)
        {
            sourceBytes = reader.GetInt64(lengthOrdinal);
            if (sourceBytes < 0 ||
                sourceBytes > maximumValueBytes)
            {
                throw new MySqlRetainedCaptureLimitException(
                    "A MySQL scalar exceeds the retained value bound.");
            }
        }

        bool nullValue = reader.IsDBNull(valueOrdinal);
        if (nullLength != nullValue)
        {
            throw new MySqlMigrationException(
                "MySQL returned inconsistent scalar length metadata.");
        }
        if (nullValue)
        {
            if (!column.Nullable)
            {
                throw new MySqlMigrationException(
                    "MySQL returned NULL for a nonnullable retained column.");
            }
            return new MySqlProjectedScalar(
                new MigrationSourceValue
                {
                    Kind = MigrationSourceValueKind.Null,
                },
                0);
        }

        string dataType =
            MySqlRetainedBinding.Facet(
                column.CatalogObject,
                "mysqlDataType") ??
            throw new MySqlMigrationException(
                "A retained MySQL column is missing its data type.");
        object providerValue = codec switch
        {
            MySqlScalarCodecKind.SignedInteger =>
                ReadSignedInteger(
                    reader,
                    valueOrdinal,
                    dataType),
            MySqlScalarCodecKind.UnsignedInteger =>
                ReadUnsignedInteger(
                    reader,
                    valueOrdinal,
                    dataType),
            MySqlScalarCodecKind.Decimal =>
                reader.GetMySqlDecimal(valueOrdinal),
            MySqlScalarCodecKind.Binary32 =>
                reader.GetFloat(valueOrdinal),
            MySqlScalarCodecKind.Binary64 =>
                reader.GetDouble(valueOrdinal),
            MySqlScalarCodecKind.Text =>
                reader.GetString(valueOrdinal),
            MySqlScalarCodecKind.Binary =>
                reader.GetFieldValue<byte[]>(valueOrdinal),
            MySqlScalarCodecKind.Date or
                MySqlScalarCodecKind.DateTime =>
                reader.GetMySqlDateTime(valueOrdinal),
            _ => throw new MySqlMigrationException(
                "The MySQL scalar codec does not match its data type."),
        };
        return Project(
            codec,
            providerValue,
            maximumValueBytes);
    }

    internal static MySqlProjectedScalar Project(
        MySqlScalarCodecKind codec,
        object providerValue,
        int maximumValueBytes)
    {
        ArgumentNullException.ThrowIfNull(providerValue);
        if (maximumValueBytes <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maximumValueBytes));
        }

        MigrationSourceValue value = codec switch
        {
            MySqlScalarCodecKind.SignedInteger =>
                TextValue(
                    MigrationSourceValueKind.SignedInteger,
                    SignedIntegerText(providerValue)),
            MySqlScalarCodecKind.UnsignedInteger =>
                TextValue(
                    MigrationSourceValueKind.UnsignedInteger,
                    UnsignedIntegerText(providerValue)),
            MySqlScalarCodecKind.Decimal =>
                TextValue(
                    MigrationSourceValueKind.Decimal,
                    providerValue is MySqlDecimal decimalValue
                        ? CanonicalDecimal(
                            decimalValue.ToString())
                        : providerValue is string decimalText
                            ? CanonicalDecimal(decimalText)
                            : throw InvalidProviderValue()),
            MySqlScalarCodecKind.Binary32 =>
                TextValue(
                    MigrationSourceValueKind.FloatingPoint,
                    Binary32Text(providerValue)),
            MySqlScalarCodecKind.Binary64 =>
                TextValue(
                    MigrationSourceValueKind.FloatingPoint,
                    Binary64Text(providerValue)),
            MySqlScalarCodecKind.Text =>
                TextValue(
                    MigrationSourceValueKind.Text,
                    providerValue as string ??
                    throw InvalidProviderValue()),
            MySqlScalarCodecKind.Binary =>
                new MigrationSourceValue
                {
                    Kind = MigrationSourceValueKind.Binary,
                    BinaryValue = providerValue as byte[] ??
                        throw InvalidProviderValue(),
                },
            MySqlScalarCodecKind.Date =>
                TextValue(
                    MigrationSourceValueKind.Date,
                    DateText(providerValue)),
            MySqlScalarCodecKind.DateTime =>
                TextValue(
                    MigrationSourceValueKind.DateTime,
                    DateTimeText(providerValue)),
            _ => throw new ArgumentOutOfRangeException(
                nameof(codec)),
        };

        int payloadBytes;
        try
        {
            payloadBytes =
                value.Kind == MigrationSourceValueKind.Binary
                    ? value.BinaryValue.Length
                    : StrictUtf8.GetByteCount(
                        value.CanonicalText ??
                        throw InvalidProviderValue());
        }
        catch (EncoderFallbackException)
        {
            throw new MySqlMigrationException(
                "MySQL text cannot be represented losslessly as retained UTF-8.");
        }
        if (payloadBytes > maximumValueBytes)
        {
            throw new MySqlRetainedCaptureLimitException(
                "A MySQL scalar exceeds the retained value bound.");
        }
        return new MySqlProjectedScalar(
            value,
            payloadBytes);
    }

    internal static string CanonicalDecimal(string value)
    {
        ArgumentNullException.ThrowIfNull(value);
        int index = 0;
        bool negative = false;
        if (value.Length > 0 && value[0] == '-')
        {
            negative = true;
            index = 1;
        }
        if (index == value.Length)
            throw InvalidProviderValue();

        int point = -1;
        int digitCount = 0;
        for (int current = index;
             current < value.Length;
             current++)
        {
            char character = value[current];
            if (character == '.')
            {
                if (point >= 0 ||
                    current == index ||
                    current == value.Length - 1)
                {
                    throw InvalidProviderValue();
                }
                point = current;
                continue;
            }
            if (character is < '0' or > '9')
                throw InvalidProviderValue();
            digitCount++;
        }
        int scale = point < 0
            ? 0
            : value.Length - point - 1;
        if (digitCount is < 1 or > 65 ||
            scale > 30)
        {
            throw InvalidProviderValue();
        }

        string integer = point < 0
            ? value[index..]
            : value[index..point];
        string fraction = point < 0
            ? string.Empty
            : value[(point + 1)..];
        integer = integer.TrimStart('0');
        if (integer.Length == 0)
            integer = "0";
        fraction = fraction.TrimEnd('0');
        bool zero =
            integer == "0" &&
            fraction.Length == 0;
        return string.Concat(
            negative && !zero ? "-" : string.Empty,
            integer,
            fraction.Length == 0 ? string.Empty : ".",
            fraction);
    }

    private static object ReadSignedInteger(
        MySqlDataReader reader,
        int ordinal,
        string dataType) =>
        dataType switch
        {
            "tinyint" =>
                reader.GetSByte(ordinal),
            "smallint" =>
                reader.GetInt16(ordinal),
            "mediumint" or "int" or "integer" =>
                reader.GetInt32(ordinal),
            "bigint" =>
                reader.GetInt64(ordinal),
            _ => throw new MySqlMigrationException(
                "The signed-integer codec does not match its MySQL type."),
        };

    private static object ReadUnsignedInteger(
        MySqlDataReader reader,
        int ordinal,
        string dataType) =>
        dataType switch
        {
            "tinyint" =>
                reader.GetByte(ordinal),
            "smallint" =>
                reader.GetUInt16(ordinal),
            "mediumint" or "int" or "integer" =>
                reader.GetUInt32(ordinal),
            "bigint" =>
                reader.GetUInt64(ordinal),
            _ => throw new MySqlMigrationException(
                "The unsigned-integer codec does not match its MySQL type."),
        };

    private static string SignedIntegerText(
        object value) =>
        value switch
        {
            sbyte number =>
                number.ToString(CultureInfo.InvariantCulture),
            short number =>
                number.ToString(CultureInfo.InvariantCulture),
            int number =>
                number.ToString(CultureInfo.InvariantCulture),
            long number =>
                number.ToString(CultureInfo.InvariantCulture),
            _ => throw InvalidProviderValue(),
        };

    private static string UnsignedIntegerText(
        object value) =>
        value switch
        {
            byte number =>
                number.ToString(CultureInfo.InvariantCulture),
            ushort number =>
                number.ToString(CultureInfo.InvariantCulture),
            uint number =>
                number.ToString(CultureInfo.InvariantCulture),
            ulong number =>
                number.ToString(CultureInfo.InvariantCulture),
            _ => throw InvalidProviderValue(),
        };

    private static string Binary32Text(object value)
    {
        float number = value is float single
            ? single
            : throw InvalidProviderValue();
        if (!float.IsFinite(number))
        {
            throw new MySqlMigrationException(
                "MySQL returned a nonfinite binary32 value.");
        }
        return ((double)number).ToString(
            "R",
            CultureInfo.InvariantCulture);
    }

    private static string Binary64Text(object value)
    {
        double number = value is double binary64
            ? binary64
            : throw InvalidProviderValue();
        if (!double.IsFinite(number))
        {
            throw new MySqlMigrationException(
                "MySQL returned a nonfinite binary64 value.");
        }
        return number.ToString(
            "R",
            CultureInfo.InvariantCulture);
    }

    private static string DateText(object value)
    {
        MySqlDateTime date =
            RequireValidMySqlDateTime(value);
        if (date.Hour != 0 ||
            date.Minute != 0 ||
            date.Second != 0 ||
            date.Microsecond != 0)
        {
            throw InvalidProviderValue();
        }
        return CSharpDbTextCodec.FormatDate(
            new DateOnly(
                date.Year,
                date.Month,
                date.Day));
    }

    private static string DateTimeText(object value)
    {
        MySqlDateTime source =
            RequireValidMySqlDateTime(value);
        DateTime dateTime = new(
            source.Year,
            source.Month,
            source.Day,
            source.Hour,
            source.Minute,
            source.Second,
            DateTimeKind.Unspecified);
        dateTime = dateTime.AddTicks(
            checked(source.Microsecond * 10L));
        return CSharpDbTextCodec.FormatDateTime(dateTime);
    }

    private static MySqlDateTime
        RequireValidMySqlDateTime(object value)
    {
        if (value is not MySqlDateTime dateTime ||
            !dateTime.IsValidDateTime ||
            dateTime.Microsecond is < 0 or > 999_999)
        {
            throw new MySqlMigrationException(
                "MySQL returned a zero, partial, or invalid date value.");
        }
        return dateTime;
    }

    private static MigrationSourceValue TextValue(
        MigrationSourceValueKind kind,
        string text) => new()
        {
            Kind = kind,
            CanonicalText = text,
        };

    private static MySqlMigrationException
        InvalidProviderValue() => new(
            "MySQL returned an invalid provider scalar.");
}
