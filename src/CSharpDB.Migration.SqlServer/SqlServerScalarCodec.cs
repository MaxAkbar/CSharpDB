using System.Data.SqlTypes;
using System.Globalization;
using System.Numerics;
using System.Text;
using CSharpDB.Migration;
using CSharpDB.Primitives;
using Microsoft.Data.SqlClient;

namespace CSharpDB.Migration.SqlServer;

internal enum SqlServerScalarCodecKind
{
    SignedInteger,
    Boolean,
    Decimal,
    Binary32,
    Binary64,
    Text,
    Binary,
    Guid,
    Date,
    Time,
    DateTime,
    DateTimeOffset,
}

internal readonly record struct SqlServerProjectedScalar(
    MigrationSourceValue Value,
    int PayloadBytes);

internal static class SqlServerScalarCodec
{
    private static readonly UTF8Encoding StrictUtf8 =
        new(
            encoderShouldEmitUTF8Identifier: false,
            throwOnInvalidBytes: true);

    internal static bool TryResolve(
        string systemTypeName,
        byte precision,
        out SqlServerScalarCodecKind codec,
        out int? binaryWidth)
    {
        binaryWidth = null;
        switch (systemTypeName.ToLowerInvariant())
        {
            case "bigint":
            case "int":
            case "smallint":
            case "tinyint":
                codec = SqlServerScalarCodecKind.SignedInteger;
                return true;
            case "bit":
                codec = SqlServerScalarCodecKind.Boolean;
                return true;
            case "decimal":
            case "numeric":
            case "money":
            case "smallmoney":
                codec = SqlServerScalarCodecKind.Decimal;
                return true;
            case "real":
                codec = SqlServerScalarCodecKind.Binary32;
                binaryWidth = 32;
                return true;
            case "float" when precision is >= 1 and <= 24:
                codec = SqlServerScalarCodecKind.Binary32;
                binaryWidth = 32;
                return true;
            case "float" when precision is >= 25 and <= 53:
                codec = SqlServerScalarCodecKind.Binary64;
                binaryWidth = 64;
                return true;
            case "char":
            case "varchar":
            case "nchar":
            case "nvarchar":
            case "text":
            case "ntext":
            case "sysname":
                codec = SqlServerScalarCodecKind.Text;
                return true;
            case "binary":
            case "varbinary":
            case "image":
                codec = SqlServerScalarCodecKind.Binary;
                return true;
            case "uniqueidentifier":
                codec = SqlServerScalarCodecKind.Guid;
                return true;
            case "date":
                codec = SqlServerScalarCodecKind.Date;
                return true;
            case "time":
                codec = SqlServerScalarCodecKind.Time;
                return true;
            case "datetime":
            case "datetime2":
            case "smalldatetime":
                codec = SqlServerScalarCodecKind.DateTime;
                return true;
            case "datetimeoffset":
                codec = SqlServerScalarCodecKind.DateTimeOffset;
                return true;
            default:
                codec = default;
                return false;
        }
    }

    internal static SqlServerProjectedScalar Read(
        SqlDataReader reader,
        int lengthOrdinal,
        int valueOrdinal,
        SqlServerRetainedColumnBinding column,
        int maximumValueBytes)
    {
        ArgumentNullException.ThrowIfNull(reader);
        ArgumentNullException.ThrowIfNull(column);
        if (column.Codec is not SqlServerScalarCodecKind codec)
        {
            throw new SqlServerMigrationException(
                "A retained SQL Server column has no scalar codec.");
        }

        bool nullLength = reader.IsDBNull(lengthOrdinal);
        bool nullValue = reader.IsDBNull(valueOrdinal);
        if (nullLength != nullValue)
        {
            throw new SqlServerMigrationException(
                "SQL Server returned inconsistent scalar length metadata.");
        }
        if (nullValue)
        {
            if (!column.Nullable)
            {
                throw new SqlServerMigrationException(
                    "SQL Server returned NULL for a nonnullable retained column.");
            }
            return new SqlServerProjectedScalar(
                new MigrationSourceValue
                {
                    Kind = MigrationSourceValueKind.Null,
                },
                0);
        }

        long sourceBytes = reader.GetInt64(lengthOrdinal);
        if (sourceBytes < 0 || sourceBytes > maximumValueBytes)
        {
            throw new SqlServerRetainedCaptureLimitException(
                "A SQL Server scalar exceeds the retained value bound.");
        }

        string systemType = Facet(
            column.CatalogObject,
            "sqlServerSystemTypeName") ??
            throw new SqlServerMigrationException(
                "A retained SQL Server column is missing its system type.");
        object providerValue = codec switch
        {
            SqlServerScalarCodecKind.SignedInteger =>
                ReadInteger(reader, valueOrdinal, systemType),
            SqlServerScalarCodecKind.Boolean =>
                reader.GetBoolean(valueOrdinal),
            SqlServerScalarCodecKind.Decimal
                when systemType is "decimal" or "numeric" =>
                    reader.GetSqlDecimal(valueOrdinal),
            SqlServerScalarCodecKind.Decimal
                when systemType is "money" or "smallmoney" =>
                    reader.GetSqlMoney(valueOrdinal),
            SqlServerScalarCodecKind.Binary32
                when systemType == "real" =>
                    reader.GetFloat(valueOrdinal),
            SqlServerScalarCodecKind.Binary32 =>
                reader.GetDouble(valueOrdinal),
            SqlServerScalarCodecKind.Binary64 =>
                reader.GetDouble(valueOrdinal),
            SqlServerScalarCodecKind.Text =>
                reader.GetString(valueOrdinal),
            SqlServerScalarCodecKind.Binary =>
                reader.GetFieldValue<byte[]>(valueOrdinal),
            SqlServerScalarCodecKind.Guid =>
                reader.GetGuid(valueOrdinal),
            SqlServerScalarCodecKind.Date =>
                reader.GetDateTime(valueOrdinal),
            SqlServerScalarCodecKind.Time =>
                reader.GetTimeSpan(valueOrdinal),
            SqlServerScalarCodecKind.DateTime =>
                reader.GetDateTime(valueOrdinal),
            SqlServerScalarCodecKind.DateTimeOffset =>
                reader.GetDateTimeOffset(valueOrdinal),
            _ => throw new SqlServerMigrationException(
                "The SQL Server scalar codec does not match its system type."),
        };
        return Project(
            codec,
            providerValue,
            maximumValueBytes);
    }

    internal static SqlServerProjectedScalar Project(
        SqlServerScalarCodecKind codec,
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
            SqlServerScalarCodecKind.SignedInteger =>
                TextValue(
                    MigrationSourceValueKind.SignedInteger,
                    Convert.ToInt64(
                            providerValue,
                            CultureInfo.InvariantCulture)
                        .ToString(CultureInfo.InvariantCulture)),
            SqlServerScalarCodecKind.Boolean =>
                TextValue(
                    MigrationSourceValueKind.Boolean,
                    Convert.ToBoolean(
                            providerValue,
                            CultureInfo.InvariantCulture)
                        ? "true"
                        : "false"),
            SqlServerScalarCodecKind.Decimal =>
                TextValue(
                    MigrationSourceValueKind.Decimal,
                    DecimalText(providerValue)),
            SqlServerScalarCodecKind.Binary32 =>
                TextValue(
                    MigrationSourceValueKind.FloatingPoint,
                    Binary32Text(providerValue)),
            SqlServerScalarCodecKind.Binary64 =>
                TextValue(
                    MigrationSourceValueKind.FloatingPoint,
                    Binary64Text(providerValue)),
            SqlServerScalarCodecKind.Text =>
                TextValue(
                    MigrationSourceValueKind.Text,
                    providerValue as string ??
                    throw InvalidProviderValue()),
            SqlServerScalarCodecKind.Binary =>
                new MigrationSourceValue
                {
                    Kind = MigrationSourceValueKind.Binary,
                    BinaryValue = providerValue as byte[] ??
                        throw InvalidProviderValue(),
                },
            SqlServerScalarCodecKind.Guid =>
                TextValue(
                    MigrationSourceValueKind.Guid,
                    CSharpDbTextCodec.FormatGuid(
                        providerValue is Guid guid
                            ? guid
                            : throw InvalidProviderValue())),
            SqlServerScalarCodecKind.Date =>
                TextValue(
                    MigrationSourceValueKind.Date,
                    CSharpDbTextCodec.FormatDate(
                        DateOnly.FromDateTime(
                            providerValue is DateTime date
                                ? date
                                : throw InvalidProviderValue()))),
            SqlServerScalarCodecKind.Time =>
                TextValue(
                    MigrationSourceValueKind.Time,
                    CSharpDbTextCodec.FormatTime(
                        TimeOnly.FromTimeSpan(
                            providerValue is TimeSpan time
                                ? time
                                : throw InvalidProviderValue()))),
            SqlServerScalarCodecKind.DateTime =>
                TextValue(
                    MigrationSourceValueKind.DateTime,
                    CSharpDbTextCodec.FormatDateTime(
                        DateTime.SpecifyKind(
                            providerValue is DateTime dateTime
                                ? dateTime
                                : throw InvalidProviderValue(),
                            DateTimeKind.Unspecified))),
            SqlServerScalarCodecKind.DateTimeOffset =>
                TextValue(
                    MigrationSourceValueKind.DateTimeOffset,
                    CSharpDbTextCodec.FormatDateTimeOffset(
                        providerValue is DateTimeOffset offset
                            ? offset
                            : throw InvalidProviderValue())),
            _ => throw new ArgumentOutOfRangeException(nameof(codec)),
        };

        int payloadBytes;
        try
        {
            payloadBytes = value.Kind == MigrationSourceValueKind.Binary
                ? value.BinaryValue.Length
                : StrictUtf8.GetByteCount(
                    value.CanonicalText ??
                    throw InvalidProviderValue());
        }
        catch (EncoderFallbackException)
        {
            throw new SqlServerMigrationException(
                "SQL Server text cannot be represented losslessly as retained UTF-8.");
        }
        if (payloadBytes > maximumValueBytes)
        {
            throw new SqlServerRetainedCaptureLimitException(
                "A SQL Server scalar exceeds the retained value bound.");
        }
        return new SqlServerProjectedScalar(value, payloadBytes);
    }

    internal static string FormatSqlDecimal(SqlDecimal value)
    {
        if (value.IsNull)
            throw InvalidProviderValue();

        int[] words = value.Data;
        BigInteger coefficient = BigInteger.Zero;
        for (int index = words.Length - 1; index >= 0; index--)
        {
            coefficient <<= 32;
            coefficient += unchecked((uint)words[index]);
        }

        string digits = coefficient.ToString(CultureInfo.InvariantCulture);
        int scale = value.Scale;
        if (scale > 0)
        {
            digits = digits.PadLeft(scale + 1, '0');
            int point = digits.Length - scale;
            digits = digits.Insert(point, ".");
            digits = digits.TrimEnd('0').TrimEnd('.');
        }
        if (digits.Length == 0)
            digits = "0";
        if (!value.IsPositive && coefficient != BigInteger.Zero)
            digits = "-" + digits;
        return digits;
    }

    private static object ReadInteger(
        SqlDataReader reader,
        int ordinal,
        string systemType) =>
        systemType switch
        {
            "bigint" => reader.GetInt64(ordinal),
            "int" => reader.GetInt32(ordinal),
            "smallint" => reader.GetInt16(ordinal),
            "tinyint" => reader.GetByte(ordinal),
            _ => throw new SqlServerMigrationException(
                "The signed-integer codec does not match its SQL Server type."),
        };

    private static string DecimalText(object value) =>
        value switch
        {
            SqlDecimal sqlDecimal => FormatSqlDecimal(sqlDecimal),
            SqlMoney sqlMoney when !sqlMoney.IsNull =>
                sqlMoney.Value.ToString(
                    "0.############################",
                    CultureInfo.InvariantCulture),
            decimal decimalValue =>
                decimalValue.ToString(
                    "0.############################",
                    CultureInfo.InvariantCulture),
            _ => throw InvalidProviderValue(),
        };

    private static string Binary32Text(object value)
    {
        float number = value switch
        {
            float single => single,
            double binary64 => checked((float)binary64),
            _ => throw InvalidProviderValue(),
        };
        if (!float.IsFinite(number))
        {
            throw new SqlServerMigrationException(
                "SQL Server returned a nonfinite binary32 value.");
        }
        return number.ToString("R", CultureInfo.InvariantCulture);
    }

    private static string Binary64Text(object value)
    {
        double number = value is double binary64
            ? binary64
            : throw InvalidProviderValue();
        if (!double.IsFinite(number))
        {
            throw new SqlServerMigrationException(
                "SQL Server returned a nonfinite binary64 value.");
        }
        return number.ToString("R", CultureInfo.InvariantCulture);
    }

    private static MigrationSourceValue TextValue(
        MigrationSourceValueKind kind,
        string text) => new()
        {
            Kind = kind,
            CanonicalText = text,
        };

    private static string? Facet(
        MigrationCatalogObject item,
        string name) =>
        item.Facets.FirstOrDefault(facet =>
            string.Equals(
                facet.Name,
                name,
                StringComparison.Ordinal))?.Value;

    private static SqlServerMigrationException
        InvalidProviderValue() => new(
            "SQL Server returned an invalid provider scalar.");
}
