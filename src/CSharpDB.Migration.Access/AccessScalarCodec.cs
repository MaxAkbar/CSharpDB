using System.Data.OleDb;
using System.Globalization;
using System.Runtime.Versioning;
using System.Text;
using CSharpDB.Migration;
using CSharpDB.Primitives;

namespace CSharpDB.Migration.Access;

internal readonly record struct AccessProjectedScalar(
    MigrationSourceValue Value,
    int PayloadBytes);

[SupportedOSPlatform("windows")]
internal static class AccessScalarCodec
{
    private static readonly UTF8Encoding StrictUtf8 =
        new(
            encoderShouldEmitUTF8Identifier: false,
            throwOnInvalidBytes: true);

    internal static AccessProjectedScalar Read(
        OleDbDataReader reader,
        int ordinal,
        AccessColumnBinding column,
        int maximumValueBytes)
    {
        ArgumentNullException.ThrowIfNull(reader);
        ArgumentNullException.ThrowIfNull(column);
        if (maximumValueBytes <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maximumValueBytes));
        }
        if (reader.IsDBNull(ordinal))
        {
            if (!column.Metadata.Nullable)
            {
                throw new AccessMigrationException(
                    AccessMigrationErrorCode.CaptureFailed,
                    "ACE returned NULL for a non-nullable Access column.");
            }
            return new AccessProjectedScalar(
                new MigrationSourceValue
                {
                    Kind =
                        MigrationSourceValueKind.Null,
                },
                0);
        }
        AccessScalarCodecKind codec =
            column.Codec ??
            throw new AccessMigrationException(
                AccessMigrationErrorCode.CaptureFailed,
                "An Access retained column has no scalar codec.");

        return codec switch
        {
            AccessScalarCodecKind.Text =>
                ReadText(
                    reader,
                    ordinal,
                    maximumValueBytes),
            AccessScalarCodecKind.Binary =>
                ReadBinary(
                    reader,
                    ordinal,
                    maximumValueBytes),
            _ => Project(
                codec,
                reader.GetValue(ordinal),
                maximumValueBytes),
        };
    }

    internal static AccessProjectedScalar Project(
        AccessScalarCodecKind codec,
        object providerValue,
        int maximumValueBytes)
    {
        ArgumentNullException.ThrowIfNull(
            providerValue);
        MigrationSourceValue value = codec switch
        {
            AccessScalarCodecKind.SignedInteger =>
                Text(
                    MigrationSourceValueKind
                        .SignedInteger,
                    Convert.ToInt64(
                            providerValue,
                            CultureInfo.InvariantCulture)
                        .ToString(
                            CultureInfo.InvariantCulture)),
            AccessScalarCodecKind.UnsignedInteger =>
                Text(
                    MigrationSourceValueKind
                        .UnsignedInteger,
                    Convert.ToUInt64(
                            providerValue,
                            CultureInfo.InvariantCulture)
                        .ToString(
                            CultureInfo.InvariantCulture)),
            AccessScalarCodecKind.Boolean =>
                Text(
                    MigrationSourceValueKind.Boolean,
                    Convert.ToBoolean(
                            providerValue,
                            CultureInfo.InvariantCulture)
                        ? "true"
                        : "false"),
            AccessScalarCodecKind.Decimal =>
                Text(
                    MigrationSourceValueKind.Decimal,
                    Convert.ToDecimal(
                            providerValue,
                            CultureInfo.InvariantCulture)
                        .ToString(
                            "G29",
                            CultureInfo.InvariantCulture)),
            AccessScalarCodecKind.FloatingPoint =>
                FloatingPoint(providerValue),
            AccessScalarCodecKind.Text =>
                Text(
                    MigrationSourceValueKind.Text,
                    providerValue as string ??
                    throw InvalidProviderValue()),
            AccessScalarCodecKind.Binary =>
                new MigrationSourceValue
                {
                    Kind =
                        MigrationSourceValueKind.Binary,
                    BinaryValue =
                        providerValue as byte[] ??
                        throw InvalidProviderValue(),
                },
            AccessScalarCodecKind.Guid =>
                Text(
                    MigrationSourceValueKind.Guid,
                    CSharpDbTextCodec.FormatGuid(
                        providerValue is Guid guid
                            ? guid
                            : Guid.Parse(
                                Convert.ToString(
                                    providerValue,
                                    CultureInfo
                                        .InvariantCulture) ??
                                throw InvalidProviderValue()))),
            AccessScalarCodecKind.DateTime =>
                Text(
                    MigrationSourceValueKind.DateTime,
                    CSharpDbTextCodec.FormatDateTime(
                        DateTime.SpecifyKind(
                            Convert.ToDateTime(
                                providerValue,
                                CultureInfo
                                    .InvariantCulture),
                            DateTimeKind.Unspecified))),
            _ => throw new ArgumentOutOfRangeException(
                nameof(codec)),
        };

        int bytes = value.Kind ==
            MigrationSourceValueKind.Binary
            ? value.BinaryValue.Length
            : StrictUtf8.GetByteCount(
                value.CanonicalText ??
                throw InvalidProviderValue());
        if (bytes > maximumValueBytes)
        {
            throw new AccessRetainedCaptureLimitException(
                "A Microsoft Access scalar exceeds the configured retained value bound.");
        }
        return new AccessProjectedScalar(
            value,
            bytes);
    }

    private static AccessProjectedScalar ReadText(
        OleDbDataReader reader,
        int ordinal,
        int maximumValueBytes)
    {
        long characterCount;
        try
        {
            characterCount =
                reader.GetChars(
                    ordinal,
                    0,
                    null,
                    0,
                    0);
        }
        catch (NotSupportedException)
        {
            throw new AccessMigrationException(
                AccessMigrationErrorCode.CaptureFailed,
                "The ACE provider cannot expose bounded text length for retained capture.");
        }
        if (characterCount < 0 ||
            characterCount > maximumValueBytes ||
            characterCount > int.MaxValue)
        {
            throw new AccessRetainedCaptureLimitException(
                "A Microsoft Access text value exceeds the configured retained value bound.");
        }
        char[] characters =
            GC.AllocateUninitializedArray<char>(
                checked((int)characterCount));
        long read = reader.GetChars(
            ordinal,
            0,
            characters,
            0,
            characters.Length);
        if (read != characterCount)
        {
            throw new AccessMigrationException(
                AccessMigrationErrorCode.CaptureFailed,
                "ACE returned an incomplete Access text value.");
        }
        return Project(
            AccessScalarCodecKind.Text,
            new string(characters),
            maximumValueBytes);
    }

    private static AccessProjectedScalar ReadBinary(
        OleDbDataReader reader,
        int ordinal,
        int maximumValueBytes)
    {
        long length =
            reader.GetBytes(
                ordinal,
                0,
                null,
                0,
                0);
        if (length < 0 ||
            length > maximumValueBytes ||
            length > int.MaxValue)
        {
            throw new AccessRetainedCaptureLimitException(
                "A Microsoft Access binary value exceeds the configured retained value bound.");
        }
        byte[] bytes =
            GC.AllocateUninitializedArray<byte>(
                checked((int)length));
        long read = reader.GetBytes(
            ordinal,
            0,
            bytes,
            0,
            bytes.Length);
        if (read != length)
        {
            throw new AccessMigrationException(
                AccessMigrationErrorCode.CaptureFailed,
                "ACE returned an incomplete Access binary value.");
        }
        return new AccessProjectedScalar(
            new MigrationSourceValue
            {
                Kind =
                    MigrationSourceValueKind.Binary,
                BinaryValue = bytes,
            },
            bytes.Length);
    }

    private static MigrationSourceValue FloatingPoint(
        object providerValue)
    {
        double value = Convert.ToDouble(
            providerValue,
            CultureInfo.InvariantCulture);
        if (!double.IsFinite(value))
        {
            throw new AccessMigrationException(
                AccessMigrationErrorCode.CaptureFailed,
                "Microsoft Access returned a non-finite floating-point value.");
        }
        return Text(
            MigrationSourceValueKind.FloatingPoint,
            value.ToString(
                "R",
                CultureInfo.InvariantCulture));
    }

    private static MigrationSourceValue Text(
        MigrationSourceValueKind kind,
        string value) =>
        new()
        {
            Kind = kind,
            CanonicalText = value,
        };

    private static InvalidDataException
        InvalidProviderValue() =>
        new(
            "ACE returned a value that does not match the inspected Access scalar type.");
}
