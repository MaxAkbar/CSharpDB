using System.Globalization;

namespace CSharpDB.Primitives;

public static class DbCommandArguments
{
    public static Dictionary<string, DbValue> FromObjectDictionary(
        IReadOnlyDictionary<string, object?>? first,
        IReadOnlyDictionary<string, object?>? second = null)
    {
        var arguments = new Dictionary<string, DbValue>(StringComparer.OrdinalIgnoreCase);
        AddDictionary(arguments, first);
        AddDictionary(arguments, second);
        return arguments;
    }

    public static DbValue FromObject(object? value) => value switch
    {
        null => DbValue.Null,
        DbValue dbValue => dbValue,
        bool boolValue => DbValue.FromInteger(boolValue ? 1 : 0),
        byte or sbyte or short or ushort or int or uint or long => DbValue.FromInteger(Convert.ToInt64(value, CultureInfo.InvariantCulture)),
        float floatValue when IsIntegerLike(floatValue) => DbValue.FromInteger((long)floatValue),
        double doubleValue when IsIntegerLike(doubleValue) => DbValue.FromInteger((long)doubleValue),
        decimal decimalValue when IsIntegerLike(decimalValue) => DbValue.FromInteger((long)decimalValue),
        float or double or decimal => DbValue.FromReal(Convert.ToDouble(value, CultureInfo.InvariantCulture)),
        string text => DbValue.FromText(text),
        Guid guid => DbValue.FromText(guid.ToString("D")),
        DateOnly date => DbValue.FromText(date.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture)),
        DateTime dateTime => DbValue.FromText(dateTime.ToString("O", CultureInfo.InvariantCulture)),
        DateTimeOffset dateTimeOffset => DbValue.FromText(dateTimeOffset.ToString("O", CultureInfo.InvariantCulture)),
        byte[] bytes => DbValue.FromBlob(bytes),
        _ => DbValue.FromText(Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty),
    };

    private static bool IsIntegerLike(double value)
        => !double.IsNaN(value)
            && !double.IsInfinity(value)
            && value >= long.MinValue
            && value <= long.MaxValue
            && Math.Truncate(value) == value;

    private static bool IsIntegerLike(decimal value)
        => value >= long.MinValue
            && value <= long.MaxValue
            && decimal.Truncate(value) == value;

    private static void AddDictionary(
        Dictionary<string, DbValue> arguments,
        IReadOnlyDictionary<string, object?>? source)
    {
        if (source is null)
            return;

        foreach ((string key, object? value) in source)
        {
            if (!string.IsNullOrWhiteSpace(key))
                arguments[key] = FromObject(value);
        }
    }
}
