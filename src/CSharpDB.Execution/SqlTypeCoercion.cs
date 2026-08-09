using System.Buffers;
using System.Globalization;
using System.Text;
using System.Text.Json;
using CSharpDB.Primitives;

namespace CSharpDB.Execution;

/// <summary>
/// Single conversion boundary for values entering a declared SQL type. The
/// same rules are used by DML, defaults, CAST, and transactional table rewrites.
/// </summary>
internal static class SqlTypeCoercion
{
    private static readonly UTF8Encoding StrictUtf8 =
        new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

    public static DbValue CoerceForAssignment(
        DbValue value,
        ColumnDefinition column,
        string? tableName = null)
    {
        ArgumentNullException.ThrowIfNull(column);
        if (value.IsNull)
            return value;

        try
        {
            // Metadata written before logical descriptors existed retains the
            // established physical compatibility rules.
            return column.DeclaredType is null
                ? CoerceLegacy(value, column.Type)
                : Coerce(value, column.DeclaredType, explicitCast: false);
        }
        catch (CSharpDbException)
        {
            throw;
        }
        catch (Exception ex) when (ex is
            ArgumentException or
            FormatException or
            InvalidOperationException or
            OverflowException or
            JsonException)
        {
            string target = tableName is null
                ? $"column '{column.Name}'"
                : $"column '{tableName}.{column.Name}'";
            throw new CSharpDbException(
                ErrorCode.TypeMismatch,
                $"Value of type {value.Type} is not valid for {target} declared as {column.EffectiveType.ToSql()}: {ex.Message}",
                ex);
        }
    }

    public static DbValue Cast(
        DbValue value,
        SqlTypeDescriptor targetType,
        SqlTypeDescriptor? sourceType = null)
    {
        ArgumentNullException.ThrowIfNull(targetType);
        if (value.IsNull)
            return value;

        try
        {
            value = PrepareLogicalSourceForCast(value, sourceType, targetType);
            return Coerce(value, targetType, explicitCast: true);
        }
        catch (CSharpDbException)
        {
            throw;
        }
        catch (Exception ex) when (ex is
            ArgumentException or
            FormatException or
            InvalidOperationException or
            OverflowException or
            JsonException)
        {
            throw new CSharpDbException(
                ErrorCode.TypeMismatch,
                $"Cannot CAST {value.Type} value to {targetType.ToSql()}: {ex.Message}",
                ex);
        }
    }

    private static DbValue PrepareLogicalSourceForCast(
        DbValue value,
        SqlTypeDescriptor? sourceType,
        SqlTypeDescriptor targetType)
    {
        if (sourceType is null)
            return value;

        bool targetIsCharacter = targetType.Kind is
            SqlTypeKind.Char or
            SqlTypeKind.VarChar or
            SqlTypeKind.Text;
        if (targetIsCharacter)
        {
            if (sourceType.Kind == SqlTypeKind.Uuid)
            {
                return DbValue.FromText(CSharpDbTextCodec.FormatGuid(
                    new Guid(value.AsBlob, bigEndian: true)));
            }

            if (sourceType.Kind == SqlTypeKind.Boolean)
                return DbValue.FromText(value.AsInteger == 0 ? "FALSE" : "TRUE");
        }

        if (value.Type != DbType.Text)
            return value;

        string text = value.AsText;
        return (sourceType.Kind, targetType.Kind) switch
        {
            (SqlTypeKind.Date, SqlTypeKind.Timestamp) =>
                DbValue.FromText($"{text} 00:00:00"),
            (SqlTypeKind.Date, SqlTypeKind.TimestampWithTimeZone) =>
                DbValue.FromText($"{text} 00:00:00+00:00"),
            (SqlTypeKind.Timestamp, SqlTypeKind.Date) =>
                DbValue.FromText(text[..10]),
            (SqlTypeKind.Timestamp, SqlTypeKind.Time) =>
                DbValue.FromText(text[11..]),
            (SqlTypeKind.Timestamp, SqlTypeKind.TimestampWithTimeZone) =>
                DbValue.FromText($"{text}+00:00"),
            (SqlTypeKind.TimestampWithTimeZone, SqlTypeKind.Date) =>
                DbValue.FromText(RemoveCanonicalTimeZone(text)[..10]),
            (SqlTypeKind.TimestampWithTimeZone, SqlTypeKind.Time) =>
                DbValue.FromText(RemoveCanonicalTimeZone(text)[11..]),
            (SqlTypeKind.TimestampWithTimeZone, SqlTypeKind.Timestamp) =>
                DbValue.FromText(RemoveCanonicalTimeZone(text)),
            _ => value,
        };
    }

    private static string RemoveCanonicalTimeZone(string value)
    {
        if (value.Length >= 6 &&
            value[^6] is '+' or '-' &&
            value[^3] == ':')
        {
            return value[..^6];
        }

        if (value.EndsWith('Z') || value.EndsWith('z'))
            return value[..^1];

        throw new FormatException("A canonical timezone suffix is required.");
    }

    /// <summary>
    /// Compares values using the semantic ordering of a declared logical type.
    /// Most logical text types use their canonical storage spelling, but SQL
    /// interval spellings are not lexicographically sortable (for example,
    /// 10-00 sorts before 2-00 as text).
    /// </summary>
    internal static int Compare(
        DbValue left,
        DbValue right,
        SqlTypeDescriptor? declaredType,
        string? collation = null)
    {
        if (left.IsNull || right.IsNull)
            return CollationSupport.Compare(left, right, collation);

        return declaredType?.Kind switch
        {
            SqlTypeKind.IntervalYearToMonth =>
                ParseYearMonthInterval(left).CompareTo(ParseYearMonthInterval(right)),
            SqlTypeKind.IntervalDayToSecond =>
                ParseDaySecondInterval(left).CompareTo(ParseDaySecondInterval(right)),
            _ => CollationSupport.Compare(left, right, collation),
        };
    }

    internal static bool IsInterval(SqlTypeDescriptor? declaredType) =>
        declaredType?.Kind is
            SqlTypeKind.IntervalYearToMonth or
            SqlTypeKind.IntervalDayToSecond;

    private static DbValue CoerceLegacy(DbValue value, DbType targetType)
    {
        if (value.Type == targetType)
            return value;
        if (targetType == DbType.Real && value.Type == DbType.Integer)
            return DbValue.FromReal(value.AsInteger);
        if (targetType == DbType.Real && value.Type == DbType.Decimal)
            return DbValue.FromReal((double)value.AsDecimal);

        throw new InvalidOperationException(
            $"Expected physical type {targetType}, received {value.Type}.");
    }

    private static DbValue Coerce(
        DbValue value,
        SqlTypeDescriptor targetType,
        bool explicitCast) => targetType.Kind switch
    {
        SqlTypeKind.Boolean => CoerceBoolean(value, explicitCast),
        SqlTypeKind.TinyInt => DbValue.FromInteger(
            RequireRange(GetExactInteger(value, explicitCast), byte.MinValue, byte.MaxValue, targetType)),
        SqlTypeKind.SmallInt => DbValue.FromInteger(
            RequireRange(GetExactInteger(value, explicitCast), short.MinValue, short.MaxValue, targetType)),
        SqlTypeKind.Integer => DbValue.FromInteger(
            RequireRange(GetExactInteger(value, explicitCast), int.MinValue, int.MaxValue, targetType)),
        SqlTypeKind.BigInt => DbValue.FromInteger(GetExactInteger(value, explicitCast)),
        SqlTypeKind.Real => CoerceReal(value, singlePrecision: true, explicitCast),
        SqlTypeKind.Double => CoerceReal(value, singlePrecision: false, explicitCast),
        SqlTypeKind.Decimal => CoerceDecimal(value, targetType, explicitCast),
        SqlTypeKind.Char => CoerceCharacter(value, targetType, fixedLength: true, explicitCast),
        SqlTypeKind.VarChar => CoerceCharacter(value, targetType, fixedLength: false, explicitCast),
        SqlTypeKind.Text => DbValue.FromText(GetText(value, explicitCast)),
        SqlTypeKind.Binary => CoerceBinary(value, targetType, fixedLength: true, explicitCast),
        SqlTypeKind.VarBinary => CoerceBinary(value, targetType, fixedLength: false, explicitCast),
        SqlTypeKind.Blob => DbValue.FromBlob(GetBlob(value, explicitCast)),
        SqlTypeKind.Uuid => CoerceUuid(value),
        SqlTypeKind.Date => CoerceDate(value),
        SqlTypeKind.Time => CoerceTime(value, targetType),
        SqlTypeKind.Timestamp => CoerceTimestamp(value, targetType),
        SqlTypeKind.TimestampWithTimeZone => CoerceTimestampWithTimeZone(value, targetType),
        SqlTypeKind.IntervalYearToMonth => CoerceYearMonthInterval(value, explicitCast),
        SqlTypeKind.IntervalDayToSecond => CoerceDaySecondInterval(value, targetType, explicitCast),
        SqlTypeKind.Json => DbValue.FromText(CanonicalizeJson(GetText(value, explicitCast))),
        SqlTypeKind.Xml => DbValue.FromText(CanonicalizeXml(GetText(value, explicitCast))),
        SqlTypeKind.Bit => CoerceBits(value, targetType, fixedLength: true),
        SqlTypeKind.VarBit => CoerceBits(value, targetType, fixedLength: false),
        _ => throw new InvalidOperationException($"Unsupported SQL type {targetType.Kind}."),
    };

    private static DbValue CoerceBoolean(DbValue value, bool explicitCast)
    {
        if (value.Type == DbType.Integer)
            return DbValue.FromInteger(value.AsInteger == 0 ? 0 : 1);
        if (value.Type == DbType.Decimal)
            return DbValue.FromInteger(value.AsDecimal == 0m ? 0 : 1);
        if (value.Type == DbType.Real)
        {
            if (!double.IsFinite(value.AsReal))
                throw new OverflowException("Non-finite floating-point values cannot be converted to BOOLEAN.");

            return DbValue.FromInteger(value.AsReal == 0d ? 0 : 1);
        }
        if (value.Type == DbType.Text)
        {
            string text = value.AsText.Trim();
            if (text.Equals("TRUE", StringComparison.OrdinalIgnoreCase) || text == "1")
                return DbValue.FromInteger(1);
            if (text.Equals("FALSE", StringComparison.OrdinalIgnoreCase) || text == "0")
                return DbValue.FromInteger(0);
        }

        throw new InvalidOperationException(
            explicitCast
                ? "BOOLEAN accepts TRUE, FALSE, or a finite numeric value."
                : "BOOLEAN assignments must resolve to TRUE/FALSE or a finite numeric value.");
    }

    private static long GetExactInteger(DbValue value, bool explicitCast)
    {
        if (value.Type == DbType.Integer)
            return value.AsInteger;
        if (value.Type == DbType.Decimal)
        {
            decimal number = value.AsDecimal;
            if (decimal.Truncate(number) == number)
                return decimal.ToInt64(number);
        }
        if (value.Type == DbType.Real)
        {
            double number = value.AsReal;
            if (double.IsFinite(number) &&
                Math.Truncate(number) == number &&
                number >= long.MinValue &&
                number < 9_223_372_036_854_775_808d)
            {
                return checked((long)number);
            }
        }
        if (explicitCast && value.Type == DbType.Text &&
            long.TryParse(value.AsText, NumberStyles.Integer, CultureInfo.InvariantCulture, out long parsed))
        {
            return parsed;
        }

        throw new InvalidOperationException("An exact integral value is required.");
    }

    private static long RequireRange(
        long value,
        long minimum,
        long maximum,
        SqlTypeDescriptor targetType)
    {
        if (value < minimum || value > maximum)
        {
            throw new OverflowException(
                $"{value} is outside the {targetType.ToSql()} range {minimum} through {maximum}.");
        }

        return value;
    }

    private static DbValue CoerceReal(DbValue value, bool singlePrecision, bool explicitCast)
    {
        // Preserve exact integer tags in REAL/DOUBLE columns. The record model
        // intentionally supports this representation so values beyond the
        // exactly representable IEEE-754 range are never rounded silently;
        // index creation can then reject an unsafe numeric key.
        if (value.Type == DbType.Integer)
            return value;
        if (value.Type == DbType.Real)
        {
            if (!double.IsFinite(value.AsReal))
                throw new OverflowException("Non-finite floating-point values are not supported.");
            return value;
        }

        double number = value.Type switch
        {
            DbType.Decimal => (double)value.AsDecimal,
            DbType.Text when explicitCast =>
                double.Parse(value.AsText, NumberStyles.Float, CultureInfo.InvariantCulture),
            _ => throw new InvalidOperationException("A numeric value is required."),
        };

        if (!double.IsFinite(number))
            throw new OverflowException("Non-finite floating-point values are not supported.");
        _ = singlePrecision; // Both logical spellings use the stable IEEE-754 double payload.
        return DbValue.FromReal(number);
    }

    private static DbValue CoerceDecimal(
        DbValue value,
        SqlTypeDescriptor targetType,
        bool explicitCast)
    {
        decimal number = value.Type switch
        {
            DbType.Decimal => value.AsDecimal,
            DbType.Integer => value.AsInteger,
            DbType.Real when double.IsFinite(value.AsReal) => checked((decimal)value.AsReal),
            DbType.Text when explicitCast =>
                decimal.Parse(value.AsText, NumberStyles.Number, CultureInfo.InvariantCulture),
            _ => throw new InvalidOperationException("A finite numeric value is required."),
        };

        (int precision, int scale) = CSharpDbDecimalCodec.ResolveFacets(
            targetType.Precision,
            targetType.Scale);
        long coefficient = CSharpDbDecimalCodec.ToScaledInt64(number, precision, scale);
        return DbValue.FromDecimalParts(coefficient, scale);
    }

    private static DbValue CoerceCharacter(
        DbValue value,
        SqlTypeDescriptor targetType,
        bool fixedLength,
        bool explicitCast)
    {
        string text = GetText(value, explicitCast);
        if (targetType.Length is not int length)
            return DbValue.FromText(text);

        int runeCount = text.EnumerateRunes().Count();
        if (runeCount > length)
        {
            throw new OverflowException(
                $"The value contains {runeCount} Unicode characters; {targetType.ToSql()} permits {length}.");
        }
        if (fixedLength && runeCount < length)
            text += new string(' ', length - runeCount);

        return DbValue.FromText(text);
    }

    private static string GetText(DbValue value, bool explicitCast)
    {
        if (value.Type == DbType.Text)
            return value.AsText;
        if (!explicitCast)
            throw new InvalidOperationException("A character value is required.");

        return value.Type switch
        {
            DbType.Integer => value.AsInteger.ToString(CultureInfo.InvariantCulture),
            DbType.Real => value.AsReal.ToString("R", CultureInfo.InvariantCulture),
            DbType.Decimal => value.AsDecimal.ToString(CultureInfo.InvariantCulture),
            DbType.Blob when value.IsBitString => value.AsBitString,
            DbType.Blob => Convert.ToHexString(value.AsBlob),
            _ => throw new InvalidOperationException($"{value.Type} cannot be converted to text."),
        };
    }

    private static DbValue CoerceBinary(
        DbValue value,
        SqlTypeDescriptor targetType,
        bool fixedLength,
        bool explicitCast)
    {
        byte[] bytes = GetBlob(value, explicitCast);
        if (targetType.Length is not int length)
            return DbValue.FromBlob(bytes);
        if (bytes.Length > length)
        {
            throw new OverflowException(
                $"The value contains {bytes.Length} bytes; {targetType.ToSql()} permits {length}.");
        }
        if (!fixedLength || bytes.Length == length)
            return DbValue.FromBlob(bytes);

        byte[] padded = new byte[length];
        bytes.CopyTo(padded, 0);
        return DbValue.FromBlob(padded);
    }

    private static byte[] GetBlob(DbValue value, bool explicitCast)
    {
        if (value.Type == DbType.Blob)
            return value.AsBlob;
        if (explicitCast && value.Type == DbType.Text)
            return StrictUtf8.GetBytes(value.AsText);

        throw new InvalidOperationException("A binary value is required.");
    }

    private static DbValue CoerceUuid(DbValue value)
    {
        if (value.Type == DbType.Blob)
        {
            if (value.AsBlob.Length != 16)
                throw new InvalidOperationException("UUID binary values must contain exactly 16 bytes.");
            return DbValue.FromBlob(value.AsBlob);
        }

        if (value.Type != DbType.Text)
            throw new InvalidOperationException("UUID requires canonical text or a 16-byte binary value.");

        Guid guid = CSharpDbTextCodec.ParseGuid(value.AsText);
        byte[] bytes = new byte[16];
        if (!guid.TryWriteBytes(bytes, bigEndian: true, out int bytesWritten) || bytesWritten != bytes.Length)
            throw new InvalidOperationException("UUID could not be encoded.");
        return DbValue.FromBlob(bytes);
    }

    private static DbValue CoerceDate(DbValue value)
    {
        string text = RequireText(value, "DATE");
        DateOnly parsed = DateOnly.Parse(text, CultureInfo.InvariantCulture);
        return DbValue.FromText(CSharpDbTextCodec.FormatDate(parsed));
    }

    private static DbValue CoerceTime(DbValue value, SqlTypeDescriptor targetType)
    {
        string text = RequireText(value, "TIME");
        string normalized = NormalizeTime(text, targetType.FractionalSecondsPrecision, out _);
        return DbValue.FromText(normalized);
    }

    private static DbValue CoerceTimestamp(DbValue value, SqlTypeDescriptor targetType)
    {
        string text = RequireText(value, "DATETIME2");
        SplitTimestamp(text, out DateOnly date, out string timeText);
        string time = NormalizeTime(timeText, targetType.FractionalSecondsPrecision, out _);
        return DbValue.FromText($"{CSharpDbTextCodec.FormatDate(date)} {time}");
    }

    private static DbValue CoerceTimestampWithTimeZone(
        DbValue value,
        SqlTypeDescriptor targetType)
    {
        string text = RequireText(value, "DATETIMEOFFSET").Trim();
        TimeSpan offset;
        string localTimestamp;
        if (text.EndsWith('Z') || text.EndsWith('z'))
        {
            offset = TimeSpan.Zero;
            localTimestamp = text[..^1];
        }
        else
        {
            int offsetStart = Math.Max(text.LastIndexOf('+'), text.LastIndexOf('-'));
            if (offsetStart <= 10 ||
                !TimeSpan.TryParseExact(text[offsetStart..], @"hh\:mm", CultureInfo.InvariantCulture, out TimeSpan magnitude))
            {
                // TryParseExact does not accept the sign as part of hh:mm.
                if (offsetStart <= 10 ||
                    !TimeSpan.TryParseExact(text[(offsetStart + 1)..], @"hh\:mm", CultureInfo.InvariantCulture, out magnitude))
                {
                    throw new FormatException("A timezone suffix Z or ±HH:mm is required.");
                }
            }
            offset = text[offsetStart] == '-' ? -magnitude : magnitude;
            localTimestamp = text[..offsetStart];
        }

        if (offset < TimeSpan.FromHours(-14) || offset > TimeSpan.FromHours(14))
            throw new FormatException("Timezone offset must be between -14:00 and +14:00.");

        SplitTimestamp(localTimestamp, out DateOnly date, out string timeText);
        string time = NormalizeTime(
            timeText,
            targetType.FractionalSecondsPrecision,
            out string fraction);
        string integralTime = time.Length >= 8 ? time[..8] : time;
        TimeOnly clock = TimeOnly.ParseExact(integralTime, "HH:mm:ss", CultureInfo.InvariantCulture);
        DateTime local = date.ToDateTime(clock, DateTimeKind.Unspecified);
        DateTimeOffset utc = new DateTimeOffset(local, offset).ToUniversalTime();
        return DbValue.FromText(
            $"{utc:yyyy-MM-dd HH:mm:ss}{fraction}+00:00");
    }

    private static string NormalizeTime(
        string input,
        int? maximumFractionalDigits,
        out string normalizedFraction)
    {
        string text = input.Trim();
        int separator = text.IndexOf('.');
        string integral = separator < 0 ? text : text[..separator];
        string fraction = separator < 0 ? string.Empty : text[(separator + 1)..];
        _ = TimeOnly.ParseExact(integral, "HH:mm:ss", CultureInfo.InvariantCulture);

        if (separator >= 0 &&
            (fraction.Length is < 1 or > SqlTypeDescriptor.MaximumFractionalSecondsPrecision ||
             fraction.Any(static ch => ch is < '0' or > '9')))
        {
            throw new FormatException(
                $"Fractional seconds must contain 1 through {SqlTypeDescriptor.MaximumFractionalSecondsPrecision} digits.");
        }
        if (maximumFractionalDigits is int maximum && fraction.Length > maximum)
        {
            throw new InvalidOperationException(
                $"The value has {fraction.Length} fractional digits; the declared precision is {maximum}.");
        }

        fraction = fraction.TrimEnd('0');
        normalizedFraction = fraction.Length == 0 ? string.Empty : $".{fraction}";
        return integral + normalizedFraction;
    }

    private static void SplitTimestamp(
        string input,
        out DateOnly date,
        out string timeText)
    {
        string text = input.Trim();
        if (text.Length < 19 || (text[10] is not (' ' or 'T' or 't')))
            throw new FormatException("Timestamp values must use YYYY-MM-DD HH:mm:ss[.fraction].");

        date = DateOnly.ParseExact(text[..10], "yyyy-MM-dd", CultureInfo.InvariantCulture);
        timeText = text[11..];
    }

    private static DbValue CoerceYearMonthInterval(DbValue value, bool explicitCast)
    {
        if (value.Type == DbType.Integer)
            return DbValue.FromText(FormatYearMonthInterval(value.AsInteger));
        if (value.Type != DbType.Text)
            throw new InvalidOperationException("INTERVAL YEAR TO MONTH requires total months or Y-M text.");

        string text = value.AsText.Trim();
        if (long.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out long totalMonths))
            return DbValue.FromText(FormatYearMonthInterval(totalMonths));

        bool negative = text.StartsWith('-');
        string unsigned = negative || text.StartsWith('+') ? text[1..] : text;
        string[] parts = unsigned.Split('-', StringSplitOptions.None);
        if (parts.Length != 2 ||
            !long.TryParse(parts[0], NumberStyles.None, CultureInfo.InvariantCulture, out long years) ||
            !int.TryParse(parts[1], NumberStyles.None, CultureInfo.InvariantCulture, out int months) ||
            months is < 0 or > 11)
        {
            throw new FormatException("INTERVAL YEAR TO MONTH must use [+-]years-months with months from 0 through 11.");
        }

        totalMonths = checked(years * 12 + months);
        if (negative)
            totalMonths = checked(-totalMonths);
        return DbValue.FromText(FormatYearMonthInterval(totalMonths));
    }

    private static string FormatYearMonthInterval(long totalMonths)
    {
        bool negative = totalMonths < 0;
        ulong magnitude = negative ? (ulong)(-(totalMonths + 1)) + 1 : (ulong)totalMonths;
        ulong years = magnitude / 12;
        ulong months = magnitude % 12;
        return $"{(negative ? "-" : string.Empty)}{years}-{months:D2}";
    }

    private static long ParseYearMonthInterval(DbValue value)
    {
        if (value.Type != DbType.Text)
            throw new CSharpDbException(
                ErrorCode.TypeMismatch,
                $"INTERVAL YEAR TO MONTH comparison requires text storage, received {value.Type}.");

        string canonical = CoerceYearMonthInterval(value, explicitCast: true).AsText;
        bool negative = canonical.StartsWith('-');
        string unsigned = negative ? canonical[1..] : canonical;
        int separator = unsigned.IndexOf('-');
        long years = long.Parse(unsigned[..separator], NumberStyles.None, CultureInfo.InvariantCulture);
        int months = int.Parse(unsigned[(separator + 1)..], NumberStyles.None, CultureInfo.InvariantCulture);
        long totalMonths = checked(years * 12 + months);
        return negative ? checked(-totalMonths) : totalMonths;
    }

    private static DbValue CoerceDaySecondInterval(
        DbValue value,
        SqlTypeDescriptor targetType,
        bool explicitCast)
    {
        string text;
        if (value.Type == DbType.Text)
            text = value.AsText;
        else if (value.Type == DbType.Integer)
            text = TimeSpan.FromSeconds(value.AsInteger).ToString("c", CultureInfo.InvariantCulture);
        else if (value.Type == DbType.Decimal)
            text = FormatSecondsInterval(value.AsDecimal);
        else if (value.Type == DbType.Real && explicitCast && double.IsFinite(value.AsReal))
            text = FormatSecondsInterval(checked((decimal)value.AsReal));
        else
            throw new InvalidOperationException("INTERVAL DAY TO SECOND requires interval text or a numeric second count.");

        if (!TimeSpan.TryParse(text, CultureInfo.InvariantCulture, out TimeSpan interval))
            throw new FormatException("INTERVAL DAY TO SECOND must use the invariant d.hh:mm:ss.fffffff format.");
        int precision = targetType.FractionalSecondsPrecision ?? 7;
        long quantum = precision >= 7 ? 1 : Pow10(7 - precision);
        if (interval.Ticks % quantum != 0)
            throw new InvalidOperationException($"The interval exceeds fractional-seconds precision {precision}.");
        return DbValue.FromText(interval.ToString("c", CultureInfo.InvariantCulture));
    }

    private static string FormatSecondsInterval(decimal seconds)
    {
        decimal ticks = checked(seconds * TimeSpan.TicksPerSecond);
        if (decimal.Truncate(ticks) != ticks)
            throw new InvalidOperationException("The interval exceeds the engine's 100-nanosecond resolution.");
        return TimeSpan.FromTicks(decimal.ToInt64(ticks)).ToString("c", CultureInfo.InvariantCulture);
    }

    private static long ParseDaySecondInterval(DbValue value)
    {
        if (value.Type != DbType.Text ||
            !TimeSpan.TryParse(value.AsText, CultureInfo.InvariantCulture, out TimeSpan interval))
        {
            throw new CSharpDbException(
                ErrorCode.TypeMismatch,
                $"INTERVAL DAY TO SECOND comparison requires canonical interval text, received {value.Type}.");
        }

        return interval.Ticks;
    }

    private static DbValue CoerceBits(
        DbValue value,
        SqlTypeDescriptor targetType,
        bool fixedLength)
    {
        byte[] bytes;
        int suppliedBits;
        if (value.Type == DbType.Blob)
        {
            bytes = value.AsBlob;
            suppliedBits = value.IsBitString
                ? value.BitLength
                : checked(bytes.Length * 8);
        }
        else if (value.Type == DbType.Text)
        {
            string bits = value.AsText.Trim();
            if (bits.Length == 0 || bits.Any(static ch => ch is not ('0' or '1')))
                throw new FormatException("Bit strings contain only 0 and 1.");
            suppliedBits = bits.Length;
            bytes = PackBits(bits);
        }
        else
        {
            throw new InvalidOperationException("BIT values require a bit string or binary value.");
        }

        if (targetType.Length is not int declaredBits)
            return DbValue.FromBitString(bytes, suppliedBits);
        if (suppliedBits > declaredBits)
            throw new OverflowException($"The value has {suppliedBits} bits; {targetType.ToSql()} permits {declaredBits}.");
        if (!fixedLength || suppliedBits == declaredBits)
            return DbValue.FromBitString(bytes, suppliedBits);

        return DbValue.FromBitString(
            PadPackedBits(bytes, suppliedBits, declaredBits),
            declaredBits);
    }

    private static byte[] PackBits(string bits)
    {
        byte[] bytes = new byte[(bits.Length + 7) / 8];
        for (int i = 0; i < bits.Length; i++)
        {
            if (bits[i] == '1')
                bytes[i / 8] |= (byte)(1 << (7 - (i % 8)));
        }
        return bytes;
    }

    private static byte[] PadPackedBits(byte[] bytes, int suppliedBits, int declaredBits)
    {
        byte[] padded = new byte[(declaredBits + 7) / 8];
        for (int bit = 0; bit < suppliedBits; bit++)
        {
            if ((bytes[bit / 8] & (1 << (7 - (bit % 8)))) != 0)
                padded[bit / 8] |= (byte)(1 << (7 - (bit % 8)));
        }
        return padded;
    }

    private static string CanonicalizeJson(string text)
    {
        using JsonDocument document = JsonDocument.Parse(text);
        var buffer = new ArrayBufferWriter<byte>();
        using (var writer = new Utf8JsonWriter(buffer))
            document.RootElement.WriteTo(writer);
        return StrictUtf8.GetString(buffer.WrittenSpan);
    }

    private static string CanonicalizeXml(string text) =>
        CSharpDbXmlCodec.Canonicalize(text);

    private static string RequireText(DbValue value, string sqlType)
    {
        if (value.Type != DbType.Text)
            throw new InvalidOperationException($"{sqlType} requires a character value.");
        return value.AsText;
    }

    private static long Pow10(int exponent)
    {
        long value = 1;
        for (int i = 0; i < exponent; i++)
            value *= 10;
        return value;
    }
}
