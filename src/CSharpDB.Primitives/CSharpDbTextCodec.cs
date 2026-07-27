using System.Globalization;

namespace CSharpDB.Primitives;

/// <summary>
/// Versioned logical text encodings shared by provider and migration paths.
/// Formats preserve the established CSharpDB EF provider wire representation
/// so existing stored values and new migration values remain interoperable.
/// </summary>
public static class CSharpDbTextCodec
{
    public const int Version = 1;
    public const string GuidFormat = "D";
    public const string DateFormat = "yyyy-MM-dd";
    public const string TimeFormat = "HH:mm:ss";
    public const string TimeFractionalFormat = "o";
    public const string DateTimeFormat = "yyyy-MM-dd HH:mm:ss.FFFFFFF";
    public const string DateTimeOffsetFormat = "yyyy-MM-dd HH:mm:ss.FFFFFFFzzz";

    public static string FormatGuid(Guid value) =>
        value.ToString(GuidFormat, CultureInfo.InvariantCulture).ToLowerInvariant();

    public static Guid ParseGuid(string value)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value);
        return Guid.Parse(value);
    }

    public static string FormatDate(DateOnly value) =>
        value.ToString(DateFormat, CultureInfo.InvariantCulture);

    public static DateOnly ParseDate(string value)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value);
        return DateOnly.Parse(value, CultureInfo.InvariantCulture);
    }

    public static string FormatTime(TimeOnly value) =>
        value.Ticks % TimeSpan.TicksPerSecond == 0
            ? value.ToString(TimeFormat, CultureInfo.InvariantCulture)
            : value.ToString(TimeFractionalFormat, CultureInfo.InvariantCulture);

    public static TimeOnly ParseTime(string value)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value);
        return TimeOnly.Parse(value, CultureInfo.InvariantCulture);
    }

    public static string FormatDateTime(DateTime value) =>
        value.ToString(DateTimeFormat, CultureInfo.InvariantCulture);

    /// <remarks>
    /// The established relational representation is wall-clock text and does
    /// not persist <see cref="DateTime.Kind"/>. Parsed values are unspecified.
    /// </remarks>
    public static DateTime ParseDateTime(string value)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value);
        return DateTime.Parse(
            value,
            CultureInfo.InvariantCulture,
            DateTimeStyles.None);
    }

    public static string FormatDateTimeOffset(DateTimeOffset value) =>
        value.ToString(DateTimeOffsetFormat, CultureInfo.InvariantCulture);

    public static DateTimeOffset ParseDateTimeOffset(string value)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value);
        return DateTimeOffset.Parse(
            value,
            CultureInfo.InvariantCulture,
            DateTimeStyles.None);
    }
}
