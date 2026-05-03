using System.Globalization;

namespace CSharpDB.Primitives;

public static class DbBuiltInScalarFunctions
{
    public static bool IsBuiltInFunctionName(string functionName)
        => functionName.ToUpperInvariant() switch
        {
            "TEXT" or
            "NZ" or "ISNULL" or "ISEMPTY" or "IIF" or "SWITCH" or "CHOOSE" or
            "COALESCE" or "IFNULL" or "NULLIF" or
            "LEN" or "LENGTH" or "LEFT" or "RIGHT" or "MID" or "SUBSTR" or "SUBSTRING" or
            "TRIM" or "LTRIM" or "RTRIM" or "UCASE" or "LCASE" or "UPPER" or "LOWER" or
            "INSTR" or "REPLACE" or "STRCOMP" or "VAL" or
            "DATE" or "TIME" or "NOW" or "DATETIME" or
            "YEAR" or "MONTH" or "DAY" or "HOUR" or "MINUTE" or "SECOND" or
            "DATEADD" or "DATEDIFF" or "DATEPART" or "DATESERIAL" or "TIMESERIAL" or
            "WEEKDAY" or "MONTHNAME" or
            "ABS" or "ROUND" or "INT" or "FIX" or "SGN" or
            "CSTR" or "CINT" or "CLNG" or "CDBL" or "CBOOL" or "CDATE" or "FORMAT" => true,
            _ => false,
        };

    public static bool TryGetReturnType(string functionName, out DbType type)
    {
        type = functionName.ToUpperInvariant() switch
        {
            "TEXT" or "CSTR" or
            "LEFT" or "RIGHT" or "MID" or "SUBSTR" or "SUBSTRING" or
            "TRIM" or "LTRIM" or "RTRIM" or "UCASE" or "LCASE" or "UPPER" or "LOWER" or "REPLACE" or
            "DATE" or "TIME" or "NOW" or "DATETIME" or "DATEADD" or "DATESERIAL" or "TIMESERIAL" or
            "MONTHNAME" or "CDATE" or "FORMAT" => DbType.Text,
            "ISNULL" or "ISEMPTY" or "CBOOL" or
            "LEN" or "LENGTH" or "INSTR" or "STRCOMP" or
            "YEAR" or "MONTH" or "DAY" or "HOUR" or "MINUTE" or "SECOND" or
            "DATEDIFF" or "DATEPART" or "WEEKDAY" or "SGN" or "CINT" or "CLNG" => DbType.Integer,
            "ABS" or "ROUND" or "INT" or "FIX" or "CDBL" or "VAL" => DbType.Real,
            _ => DbType.Null,
        };

        return type != DbType.Null;
    }

    public static bool TryEvaluate(string functionName, IReadOnlyList<DbValue> args, out DbValue value)
    {
        value = DbValue.Null;
        string name = functionName.ToUpperInvariant();

        switch (name)
        {
            case "TEXT":
                RequireArgumentCount(name, args, 1);
                value = DbValue.FromText(ToDisplayText(args[0]));
                return true;
            case "NZ":
                RequireArgumentCount(name, args, 1, 2);
                value = IsNullOrEmpty(args[0])
                    ? args.Count == 2 ? args[1] : DbValue.FromText(string.Empty)
                    : args[0];
                return true;
            case "ISNULL":
                RequireArgumentCount(name, args, 1);
                value = FromBoolean(args[0].IsNull);
                return true;
            case "ISEMPTY":
                RequireArgumentCount(name, args, 1);
                value = FromBoolean(IsNullOrEmpty(args[0]));
                return true;
            case "IIF":
                RequireArgumentCount(name, args, 3);
                value = IsTruthy(args[0]) ? args[1] : args[2];
                return true;
            case "SWITCH":
                if (args.Count == 0 || args.Count % 2 != 0)
                    throw WrongArgumentCount(name, "an even number of arguments.");
                for (int i = 0; i < args.Count; i += 2)
                {
                    if (IsTruthy(args[i]))
                    {
                        value = args[i + 1];
                        return true;
                    }
                }

                return true;
            case "CHOOSE":
                if (args.Count < 2)
                    throw WrongArgumentCount(name, "at least two arguments.");
                value = TryConvertLong(args[0], out long chooseIndex) && chooseIndex >= 1 && chooseIndex < args.Count
                    ? args[(int)chooseIndex]
                    : DbValue.Null;
                return true;
            case "COALESCE":
                if (args.Count == 0)
                    throw WrongArgumentCount(name, "at least one argument.");
                value = args.FirstOrDefault(static argument => !argument.IsNull);
                return true;
            case "IFNULL":
                RequireArgumentCount(name, args, 2);
                value = args[0].IsNull ? args[1] : args[0];
                return true;
            case "NULLIF":
                RequireArgumentCount(name, args, 2);
                value = DbValue.Compare(args[0], args[1]) == 0 ? DbValue.Null : args[0];
                return true;
            case "LEN":
            case "LENGTH":
                RequireArgumentCount(name, args, 1);
                value = args[0].IsNull ? DbValue.Null : DbValue.FromInteger(ToScalarString(args[0]).Length);
                return true;
            case "LEFT":
                RequireArgumentCount(name, args, 2);
                value = !args[0].IsNull && TryConvertLong(args[1], out long leftCount)
                    ? DbValue.FromText(Left(ToScalarString(args[0]), leftCount))
                    : DbValue.Null;
                return true;
            case "RIGHT":
                RequireArgumentCount(name, args, 2);
                value = !args[0].IsNull && TryConvertLong(args[1], out long rightCount)
                    ? DbValue.FromText(Right(ToScalarString(args[0]), rightCount))
                    : DbValue.Null;
                return true;
            case "MID":
            case "SUBSTR":
            case "SUBSTRING":
                RequireArgumentCount(name, args, 2, 3);
                value = EvaluateMid(args);
                return true;
            case "TRIM":
                RequireArgumentCount(name, args, 1);
                value = args[0].IsNull ? DbValue.Null : DbValue.FromText(ToScalarString(args[0]).Trim());
                return true;
            case "LTRIM":
                RequireArgumentCount(name, args, 1);
                value = args[0].IsNull ? DbValue.Null : DbValue.FromText(ToScalarString(args[0]).TrimStart());
                return true;
            case "RTRIM":
                RequireArgumentCount(name, args, 1);
                value = args[0].IsNull ? DbValue.Null : DbValue.FromText(ToScalarString(args[0]).TrimEnd());
                return true;
            case "UCASE":
            case "UPPER":
                RequireArgumentCount(name, args, 1);
                value = args[0].IsNull ? DbValue.Null : DbValue.FromText(ToScalarString(args[0]).ToUpperInvariant());
                return true;
            case "LCASE":
            case "LOWER":
                RequireArgumentCount(name, args, 1);
                value = args[0].IsNull ? DbValue.Null : DbValue.FromText(ToScalarString(args[0]).ToLowerInvariant());
                return true;
            case "INSTR":
                RequireArgumentCount(name, args, 2, 3);
                value = EvaluateInStr(args);
                return true;
            case "REPLACE":
                RequireArgumentCount(name, args, 3);
                value = !args[0].IsNull && !args[1].IsNull
                    ? DbValue.FromText(ToScalarString(args[0]).Replace(ToScalarString(args[1]), ToScalarString(args[2]), StringComparison.Ordinal))
                    : DbValue.Null;
                return true;
            case "STRCOMP":
                RequireArgumentCount(name, args, 2, 3);
                value = EvaluateStrComp(args);
                return true;
            case "VAL":
                RequireArgumentCount(name, args, 1);
                value = DbValue.FromReal(ParseLeadingNumber(ToScalarString(args[0])));
                return true;
            case "DATE":
                RequireArgumentCount(name, args, 0);
                value = DbValue.FromText(DateTime.Now.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture));
                return true;
            case "TIME":
                RequireArgumentCount(name, args, 0);
                value = DbValue.FromText(DateTime.Now.ToString("HH:mm:ss", CultureInfo.InvariantCulture));
                return true;
            case "NOW":
            case "DATETIME":
                RequireArgumentCount(name, args, 0);
                value = DbValue.FromText(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));
                return true;
            case "YEAR":
                RequireArgumentCount(name, args, 1);
                value = TryConvertDateTime(args[0], out DateTime yearDate) ? DbValue.FromInteger(yearDate.Year) : DbValue.Null;
                return true;
            case "MONTH":
                RequireArgumentCount(name, args, 1);
                value = TryConvertDateTime(args[0], out DateTime monthDate) ? DbValue.FromInteger(monthDate.Month) : DbValue.Null;
                return true;
            case "DAY":
                RequireArgumentCount(name, args, 1);
                value = TryConvertDateTime(args[0], out DateTime dayDate) ? DbValue.FromInteger(dayDate.Day) : DbValue.Null;
                return true;
            case "HOUR":
                RequireArgumentCount(name, args, 1);
                value = TryConvertTime(args[0], out TimeSpan hourTime) ? DbValue.FromInteger(hourTime.Hours) : DbValue.Null;
                return true;
            case "MINUTE":
                RequireArgumentCount(name, args, 1);
                value = TryConvertTime(args[0], out TimeSpan minuteTime) ? DbValue.FromInteger(minuteTime.Minutes) : DbValue.Null;
                return true;
            case "SECOND":
                RequireArgumentCount(name, args, 1);
                value = TryConvertTime(args[0], out TimeSpan secondTime) ? DbValue.FromInteger(secondTime.Seconds) : DbValue.Null;
                return true;
            case "DATEADD":
                RequireArgumentCount(name, args, 3);
                value = EvaluateDateAdd(args);
                return true;
            case "DATEDIFF":
                RequireArgumentCount(name, args, 3);
                value = EvaluateDateDiff(args);
                return true;
            case "DATEPART":
                RequireArgumentCount(name, args, 2);
                value = EvaluateDatePart(args);
                return true;
            case "DATESERIAL":
                RequireArgumentCount(name, args, 3);
                value = EvaluateDateSerial(args);
                return true;
            case "TIMESERIAL":
                RequireArgumentCount(name, args, 3);
                value = EvaluateTimeSerial(args);
                return true;
            case "WEEKDAY":
                RequireArgumentCount(name, args, 1);
                value = TryConvertDateTime(args[0], out DateTime weekdayDate)
                    ? DbValue.FromInteger(((int)weekdayDate.DayOfWeek) + 1)
                    : DbValue.Null;
                return true;
            case "MONTHNAME":
                RequireArgumentCount(name, args, 1, 2);
                value = EvaluateMonthName(args);
                return true;
            case "ABS":
                RequireArgumentCount(name, args, 1);
                value = TryConvertDouble(args[0], out double absValue) ? FromReal(Math.Abs(absValue)) : DbValue.Null;
                return true;
            case "ROUND":
                RequireArgumentCount(name, args, 1, 2);
                value = EvaluateRound(args);
                return true;
            case "INT":
                RequireArgumentCount(name, args, 1);
                value = TryConvertDouble(args[0], out double intValue) ? FromReal(Math.Floor(intValue)) : DbValue.Null;
                return true;
            case "FIX":
                RequireArgumentCount(name, args, 1);
                value = TryConvertDouble(args[0], out double fixValue) ? FromReal(Math.Truncate(fixValue)) : DbValue.Null;
                return true;
            case "SGN":
                RequireArgumentCount(name, args, 1);
                value = TryConvertDouble(args[0], out double sgnValue) ? DbValue.FromInteger(Math.Sign(sgnValue)) : DbValue.Null;
                return true;
            case "CSTR":
                RequireArgumentCount(name, args, 1);
                value = DbValue.FromText(ToScalarString(args[0]));
                return true;
            case "CINT":
            case "CLNG":
                RequireArgumentCount(name, args, 1);
                value = TryConvertLong(args[0], out long integerValue) ? DbValue.FromInteger(integerValue) : DbValue.Null;
                return true;
            case "CDBL":
                RequireArgumentCount(name, args, 1);
                value = TryConvertDouble(args[0], out double doubleValue) ? FromReal(doubleValue) : DbValue.Null;
                return true;
            case "CBOOL":
                RequireArgumentCount(name, args, 1);
                value = TryConvertBoolean(args[0], out bool boolValue) ? FromBoolean(boolValue) : DbValue.Null;
                return true;
            case "CDATE":
                RequireArgumentCount(name, args, 1);
                value = TryConvertDateTime(args[0], out DateTime dateValue)
                    ? DbValue.FromText(FormatDateTime(dateValue))
                    : DbValue.Null;
                return true;
            case "FORMAT":
                RequireArgumentCount(name, args, 2);
                value = args[1].IsNull ? DbValue.Null : FormatValue(args[0], ToScalarString(args[1]));
                return true;
            default:
                return false;
        }
    }

    public static string ToDisplayText(DbValue value) => value.Type switch
    {
        DbType.Null => "NULL",
        DbType.Integer => value.AsInteger.ToString(CultureInfo.InvariantCulture),
        DbType.Real => value.AsReal.ToString(CultureInfo.InvariantCulture),
        DbType.Text => value.AsText,
        DbType.Blob => $"[{value.AsBlob.Length} bytes]",
        _ => throw new CSharpDbException(ErrorCode.Unknown, $"Unsupported DbValue type '{value.Type}'."),
    };

    private static DbValue EvaluateMid(IReadOnlyList<DbValue> args)
    {
        if (args[0].IsNull || !TryConvertLong(args[1], out long start))
            return DbValue.Null;

        string text = ToScalarString(args[0]);
        int zeroBasedStart = ClampToStringStart(start - 1, text.Length);
        if (zeroBasedStart >= text.Length)
            return DbValue.FromText(string.Empty);

        if (args.Count == 2)
            return DbValue.FromText(text[zeroBasedStart..]);

        if (!TryConvertLong(args[2], out long count))
            return DbValue.Null;

        int length = ClampLength(count, text.Length - zeroBasedStart);
        return DbValue.FromText(text.Substring(zeroBasedStart, length));
    }

    private static DbValue EvaluateInStr(IReadOnlyList<DbValue> args)
    {
        long start = 1;
        DbValue source = args[0];
        DbValue search = args[1];
        if (args.Count == 3)
        {
            if (!TryConvertLong(args[0], out start))
                return DbValue.Null;

            source = args[1];
            search = args[2];
        }

        if (source.IsNull || search.IsNull)
            return DbValue.Null;

        string sourceText = ToScalarString(source);
        string searchText = ToScalarString(search);
        int zeroBasedStart = ClampToStringStart(start - 1, sourceText.Length);
        int index = sourceText.IndexOf(searchText, zeroBasedStart, StringComparison.OrdinalIgnoreCase);
        return DbValue.FromInteger(index < 0 ? 0 : index + 1);
    }

    private static DbValue EvaluateStrComp(IReadOnlyList<DbValue> args)
    {
        if (args[0].IsNull || args[1].IsNull)
            return DbValue.Null;

        StringComparison comparison = StringComparison.Ordinal;
        if (args.Count == 3)
        {
            string mode = ToScalarString(args[2]);
            if (string.Equals(mode, "text", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(mode, "1", StringComparison.OrdinalIgnoreCase))
            {
                comparison = StringComparison.OrdinalIgnoreCase;
            }
        }

        int result = string.Compare(ToScalarString(args[0]), ToScalarString(args[1]), comparison);
        return DbValue.FromInteger(result < 0 ? -1 : result > 0 ? 1 : 0);
    }

    private static DbValue EvaluateDateAdd(IReadOnlyList<DbValue> args)
    {
        if (!TryConvertLong(args[1], out long amount) ||
            !TryConvertDateTime(args[2], out DateTime date))
        {
            return DbValue.Null;
        }

        DateTime? result = AddDateInterval(ToScalarString(args[0]), date, amount);
        return result.HasValue ? DbValue.FromText(FormatDateTime(result.Value)) : DbValue.Null;
    }

    private static DbValue EvaluateDateDiff(IReadOnlyList<DbValue> args)
    {
        if (!TryConvertDateTime(args[1], out DateTime start) ||
            !TryConvertDateTime(args[2], out DateTime end))
        {
            return DbValue.Null;
        }

        long? result = DiffDateInterval(ToScalarString(args[0]), start, end);
        return result.HasValue ? DbValue.FromInteger(result.Value) : DbValue.Null;
    }

    private static DbValue EvaluateDatePart(IReadOnlyList<DbValue> args)
    {
        if (!TryConvertDateTime(args[1], out DateTime date))
            return DbValue.Null;

        long? result = GetDatePart(ToScalarString(args[0]), date);
        return result.HasValue ? DbValue.FromInteger(result.Value) : DbValue.Null;
    }

    private static DbValue EvaluateDateSerial(IReadOnlyList<DbValue> args)
    {
        if (!TryConvertLong(args[0], out long year) ||
            !TryConvertLong(args[1], out long month) ||
            !TryConvertLong(args[2], out long day) ||
            year is < 1 or > 9999 ||
            month is < int.MinValue or > int.MaxValue ||
            day is < int.MinValue or > int.MaxValue)
        {
            return DbValue.Null;
        }

        try
        {
            var date = new DateTime((int)year, 1, 1).AddMonths((int)month - 1).AddDays((int)day - 1);
            return DbValue.FromText(FormatDateTime(date));
        }
        catch
        {
            return DbValue.Null;
        }
    }

    private static DbValue EvaluateTimeSerial(IReadOnlyList<DbValue> args)
    {
        if (!TryConvertLong(args[0], out long hour) ||
            !TryConvertLong(args[1], out long minute) ||
            !TryConvertLong(args[2], out long second))
        {
            return DbValue.Null;
        }

        try
        {
            TimeSpan time = TimeSpan.FromHours(hour) + TimeSpan.FromMinutes(minute) + TimeSpan.FromSeconds(second);
            return DbValue.FromText(time.ToString(@"hh\:mm\:ss", CultureInfo.InvariantCulture));
        }
        catch
        {
            return DbValue.Null;
        }
    }

    private static DbValue EvaluateMonthName(IReadOnlyList<DbValue> args)
    {
        if (!TryConvertLong(args[0], out long month) || month is < 1 or > 12)
            return DbValue.Null;

        bool abbreviate = args.Count == 2 && TryConvertBoolean(args[1], out bool abbreviated) && abbreviated;
        DateTimeFormatInfo format = CultureInfo.InvariantCulture.DateTimeFormat;
        return DbValue.FromText(abbreviate ? format.GetAbbreviatedMonthName((int)month) : format.GetMonthName((int)month));
    }

    private static DbValue EvaluateRound(IReadOnlyList<DbValue> args)
    {
        if (!TryConvertDouble(args[0], out double value))
            return DbValue.Null;

        int digits = 0;
        if (args.Count == 2)
        {
            if (!TryConvertLong(args[1], out long parsedDigits) || parsedDigits is < 0 or > 15)
                return DbValue.Null;

            digits = (int)parsedDigits;
        }

        return FromReal(Math.Round(value, digits, MidpointRounding.ToEven));
    }

    private static DbValue FormatValue(DbValue value, string format)
    {
        if (value.IsNull)
            return DbValue.Null;

        try
        {
            if (TryConvertDouble(value, out double number))
                return DbValue.FromText(number.ToString(format, CultureInfo.InvariantCulture));
        }
        catch
        {
            return DbValue.FromText(ToScalarString(value));
        }

        return DbValue.FromText(ToScalarString(value));
    }

    private static DateTime? AddDateInterval(string interval, DateTime date, long amount)
    {
        try
        {
            return NormalizeInterval(interval) switch
            {
                "yyyy" => date.AddYears(checked((int)amount)),
                "q" => date.AddMonths(checked((int)amount * 3)),
                "m" => date.AddMonths(checked((int)amount)),
                "y" or "d" or "w" => date.AddDays(amount),
                "ww" => date.AddDays(checked(amount * 7)),
                "h" => date.AddHours(amount),
                "n" => date.AddMinutes(amount),
                "s" => date.AddSeconds(amount),
                _ => null,
            };
        }
        catch
        {
            return null;
        }
    }

    private static long? DiffDateInterval(string interval, DateTime start, DateTime end)
        => NormalizeInterval(interval) switch
        {
            "yyyy" => end.Year - start.Year,
            "q" => ((end.Year - start.Year) * 4) + ((end.Month - 1) / 3) - ((start.Month - 1) / 3),
            "m" => ((end.Year - start.Year) * 12) + end.Month - start.Month,
            "y" or "d" or "w" => (long)(end.Date - start.Date).TotalDays,
            "ww" => (long)Math.Floor((end.Date - start.Date).TotalDays / 7),
            "h" => (long)(end - start).TotalHours,
            "n" => (long)(end - start).TotalMinutes,
            "s" => (long)(end - start).TotalSeconds,
            _ => null,
        };

    private static long? GetDatePart(string interval, DateTime date)
        => NormalizeInterval(interval) switch
        {
            "yyyy" => date.Year,
            "q" => ((date.Month - 1) / 3) + 1,
            "m" => date.Month,
            "y" => date.DayOfYear,
            "d" => date.Day,
            "w" => ((int)date.DayOfWeek) + 1,
            "ww" => ISOWeek.GetWeekOfYear(date),
            "h" => date.Hour,
            "n" => date.Minute,
            "s" => date.Second,
            _ => null,
        };

    private static string NormalizeInterval(string interval)
        => interval.Trim().Trim('"', '\'').ToLowerInvariant();

    private static string Left(string text, long count)
        => text[..ClampLength(count, text.Length)];

    private static string Right(string text, long count)
    {
        int length = ClampLength(count, text.Length);
        return text[(text.Length - length)..];
    }

    private static double ParseLeadingNumber(string text)
    {
        text = text.TrimStart();
        if (text.Length == 0)
            return 0;

        int index = 0;
        if (text[index] is '+' or '-')
            index++;

        bool hasDigit = false;
        bool hasDecimal = false;
        while (index < text.Length)
        {
            char ch = text[index];
            if (char.IsDigit(ch))
            {
                hasDigit = true;
                index++;
                continue;
            }

            if (ch == '.' && !hasDecimal)
            {
                hasDecimal = true;
                index++;
                continue;
            }

            break;
        }

        if (!hasDigit)
            return 0;

        if (index < text.Length && text[index] is 'e' or 'E')
        {
            int exponentStart = index;
            index++;
            if (index < text.Length && text[index] is '+' or '-')
                index++;

            bool hasExponentDigit = false;
            while (index < text.Length && char.IsDigit(text[index]))
            {
                hasExponentDigit = true;
                index++;
            }

            if (!hasExponentDigit)
                index = exponentStart;
        }

        return double.TryParse(text[..index], NumberStyles.Float, CultureInfo.InvariantCulture, out double result)
            ? result
            : 0;
    }

    private static bool TryConvertDouble(DbValue value, out double result)
    {
        switch (value.Type)
        {
            case DbType.Integer:
                result = value.AsInteger;
                return true;
            case DbType.Real:
                result = value.AsReal;
                return true;
            case DbType.Text:
                return double.TryParse(
                    value.AsText,
                    NumberStyles.Float | NumberStyles.AllowThousands,
                    CultureInfo.InvariantCulture,
                    out result);
            default:
                result = 0;
                return false;
        }
    }

    private static bool TryConvertLong(DbValue value, out long result)
    {
        switch (value.Type)
        {
            case DbType.Integer:
                result = value.AsInteger;
                return true;
            case DbType.Real:
                return TryRoundToLong(value.AsReal, out result);
            case DbType.Text when long.TryParse(value.AsText, NumberStyles.Integer, CultureInfo.InvariantCulture, out long integer):
                result = integer;
                return true;
            case DbType.Text when double.TryParse(value.AsText, NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture, out double real):
                return TryRoundToLong(real, out result);
            default:
                result = 0;
                return false;
        }
    }

    private static bool TryRoundToLong(double value, out long result)
    {
        if (double.IsNaN(value) || double.IsInfinity(value))
        {
            result = 0;
            return false;
        }

        double rounded = Math.Round(value, MidpointRounding.ToEven);
        if (rounded < long.MinValue || rounded > long.MaxValue)
        {
            result = 0;
            return false;
        }

        result = Convert.ToInt64(rounded, CultureInfo.InvariantCulture);
        return true;
    }

    private static bool TryConvertBoolean(DbValue value, out bool result)
    {
        switch (value.Type)
        {
            case DbType.Integer:
                result = value.AsInteger != 0;
                return true;
            case DbType.Real:
                result = Math.Abs(value.AsReal) > double.Epsilon;
                return true;
            case DbType.Text:
                string text = value.AsText;
                if (bool.TryParse(text, out result))
                    return true;
                if (string.Equals(text, "yes", StringComparison.OrdinalIgnoreCase) ||
                    string.Equals(text, "y", StringComparison.OrdinalIgnoreCase))
                {
                    result = true;
                    return true;
                }
                if (string.Equals(text, "no", StringComparison.OrdinalIgnoreCase) ||
                    string.Equals(text, "n", StringComparison.OrdinalIgnoreCase))
                {
                    result = false;
                    return true;
                }
                if (TryConvertDouble(value, out double numericText))
                {
                    result = Math.Abs(numericText) > double.Epsilon;
                    return true;
                }
                break;
        }

        result = false;
        return false;
    }

    private static bool TryConvertDateTime(DbValue value, out DateTime result)
    {
        switch (value.Type)
        {
            case DbType.Text:
                if (DateTime.TryParse(value.AsText, CultureInfo.InvariantCulture, DateTimeStyles.AllowWhiteSpaces, out result) ||
                    DateTime.TryParse(value.AsText, CultureInfo.CurrentCulture, DateTimeStyles.AllowWhiteSpaces, out result))
                {
                    return true;
                }
                break;
            case DbType.Integer:
            case DbType.Real:
                if (TryConvertDouble(value, out double numeric))
                {
                    try
                    {
                        result = DateTime.FromOADate(numeric);
                        return true;
                    }
                    catch
                    {
                    }
                }
                break;
        }

        result = default;
        return false;
    }

    private static bool TryConvertTime(DbValue value, out TimeSpan result)
    {
        switch (value.Type)
        {
            case DbType.Text:
                if (TimeSpan.TryParse(value.AsText, CultureInfo.InvariantCulture, out result))
                    return true;
                if (DateTime.TryParse(value.AsText, CultureInfo.InvariantCulture, DateTimeStyles.AllowWhiteSpaces, out DateTime parsedDate))
                {
                    result = parsedDate.TimeOfDay;
                    return true;
                }
                break;
        }

        result = default;
        return false;
    }

    private static string ToScalarString(DbValue value) => value.Type switch
    {
        DbType.Null => string.Empty,
        DbType.Integer => value.AsInteger.ToString(CultureInfo.InvariantCulture),
        DbType.Real => value.AsReal.ToString(CultureInfo.InvariantCulture),
        DbType.Text => value.AsText,
        DbType.Blob => Convert.ToBase64String(value.AsBlob),
        _ => string.Empty,
    };

    private static string FormatDateTime(DateTime dateTime)
        => dateTime.TimeOfDay == TimeSpan.Zero
            ? dateTime.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture)
            : dateTime.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);

    private static DbValue FromBoolean(bool value)
        => DbValue.FromInteger(value ? 1 : 0);

    private static DbValue FromReal(double value)
        => double.IsNaN(value) || double.IsInfinity(value)
            ? DbValue.Null
            : DbValue.FromReal(value);

    private static bool IsTruthy(DbValue value)
    {
        if (value.IsNull)
            return false;

        if (TryConvertDouble(value, out double number))
            return Math.Abs(number) > double.Epsilon;

        string text = ToScalarString(value);
        if (bool.TryParse(text, out bool parsed))
            return parsed;

        return !string.IsNullOrWhiteSpace(text);
    }

    private static bool IsNullOrEmpty(DbValue value)
        => value.IsNull || value.Type == DbType.Text && value.AsText.Length == 0;

    private static int ClampToStringStart(long value, int textLength)
    {
        if (value <= 0)
            return 0;
        if (value >= textLength)
            return textLength;
        return (int)value;
    }

    private static int ClampLength(long value, int maxLength)
    {
        if (value <= 0)
            return 0;
        if (value >= maxLength)
            return maxLength;
        return (int)value;
    }

    private static void RequireArgumentCount(string functionName, IReadOnlyList<DbValue> args, int count)
    {
        if (args.Count != count)
            throw WrongArgumentCount(functionName, count == 1 ? "exactly one argument." : $"exactly {count} arguments.");
    }

    private static void RequireArgumentCount(string functionName, IReadOnlyList<DbValue> args, int count1, int count2)
    {
        if (args.Count != count1 && args.Count != count2)
            throw WrongArgumentCount(functionName, $"{count1} or {count2} arguments.");
    }

    private static CSharpDbException WrongArgumentCount(string functionName, string expectation)
        => new(ErrorCode.SyntaxError, $"{functionName}() requires {expectation}");
}
