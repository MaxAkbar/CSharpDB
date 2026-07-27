using System.Globalization;

namespace CSharpDB.Migration.Files.Csv;

internal static class CsvCulturePolicy
{
    public static string ComputeDigest(CultureInfo culture)
    {
        NumberFormatInfo number = culture.NumberFormat;
        DateTimeFormatInfo date = culture.DateTimeFormat;
        Calendar calendar = date.Calendar;
        var components = new List<string?>
        {
            "csharpdb-csv-culture-v1",
            culture.Name,
            culture.UseUserOverride ? "user-override" : "no-user-override",
            number.CurrencyDecimalDigits.ToString(CultureInfo.InvariantCulture),
            number.CurrencyDecimalSeparator,
            number.CurrencyGroupSeparator,
            number.CurrencyNegativePattern.ToString(CultureInfo.InvariantCulture),
            number.CurrencyPositivePattern.ToString(CultureInfo.InvariantCulture),
            number.CurrencySymbol,
            ((int)number.DigitSubstitution).ToString(CultureInfo.InvariantCulture),
            number.NaNSymbol,
            number.NegativeInfinitySymbol,
            number.NegativeSign,
            number.NumberDecimalDigits.ToString(CultureInfo.InvariantCulture),
            number.NumberDecimalSeparator,
            number.NumberGroupSeparator,
            number.NumberNegativePattern.ToString(CultureInfo.InvariantCulture),
            number.PercentDecimalDigits.ToString(CultureInfo.InvariantCulture),
            number.PercentDecimalSeparator,
            number.PercentGroupSeparator,
            number.PercentNegativePattern.ToString(CultureInfo.InvariantCulture),
            number.PercentPositivePattern.ToString(CultureInfo.InvariantCulture),
            number.PercentSymbol,
            number.PerMilleSymbol,
            number.PositiveInfinitySymbol,
            number.PositiveSign,
            date.AMDesignator,
            date.PMDesignator,
            date.DateSeparator,
            date.TimeSeparator,
            ((int)date.FirstDayOfWeek).ToString(CultureInfo.InvariantCulture),
            ((int)date.CalendarWeekRule).ToString(CultureInfo.InvariantCulture),
            date.ShortDatePattern,
            date.LongDatePattern,
            date.ShortTimePattern,
            date.LongTimePattern,
            date.FullDateTimePattern,
            date.MonthDayPattern,
            date.YearMonthPattern,
            date.SortableDateTimePattern,
            date.UniversalSortableDateTimePattern,
            date.RFC1123Pattern,
            calendar.GetType().FullName,
            ((int)calendar.AlgorithmType).ToString(CultureInfo.InvariantCulture),
            calendar.TwoDigitYearMax.ToString(CultureInfo.InvariantCulture),
            calendar.MinSupportedDateTime.Ticks.ToString(CultureInfo.InvariantCulture),
            calendar.MaxSupportedDateTime.Ticks.ToString(CultureInfo.InvariantCulture),
        };

        AddValues(components, "currency-group-sizes", number.CurrencyGroupSizes.Select(Invariant));
        AddValues(components, "number-group-sizes", number.NumberGroupSizes.Select(Invariant));
        AddValues(components, "percent-group-sizes", number.PercentGroupSizes.Select(Invariant));
        AddValues(components, "native-digits", number.NativeDigits);
        AddValues(components, "abbreviated-day-names", date.AbbreviatedDayNames);
        AddValues(components, "day-names", date.DayNames);
        AddValues(components, "shortest-day-names", date.ShortestDayNames);
        AddValues(components, "abbreviated-month-names", date.AbbreviatedMonthNames);
        AddValues(
            components,
            "abbreviated-month-genitive-names",
            date.AbbreviatedMonthGenitiveNames);
        AddValues(components, "month-names", date.MonthNames);
        AddValues(components, "month-genitive-names", date.MonthGenitiveNames);
        AddValues(
            components,
            "all-date-time-patterns",
            date.GetAllDateTimePatterns().Order(StringComparer.Ordinal));

        int[] eras = calendar.Eras.Order().ToArray();
        components.Add("eras");
        components.Add(eras.Length.ToString(CultureInfo.InvariantCulture));
        foreach (int era in eras)
        {
            components.Add(era.ToString(CultureInfo.InvariantCulture));
            components.Add(date.GetEraName(era));
            components.Add(date.GetAbbreviatedEraName(era));
        }

        return CsvStableDigest.Compute(components.ToArray());
    }

    private static void AddValues(
        List<string?> components,
        string name,
        IEnumerable<string> values)
    {
        string[] materialized = values.ToArray();
        components.Add(name);
        components.Add(materialized.Length.ToString(CultureInfo.InvariantCulture));
        components.AddRange(materialized);
    }

    private static string Invariant(int value) =>
        value.ToString(CultureInfo.InvariantCulture);
}
