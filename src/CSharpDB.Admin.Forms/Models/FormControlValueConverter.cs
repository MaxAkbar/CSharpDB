using System.Globalization;
using System.Text.Json;

namespace CSharpDB.Admin.Forms.Models;

public static class FormControlValueConverter
{
    public static bool ToBoolean(object? value)
        => TryConvertToBoolean(value, out bool result) && result;

    public static object? ConvertCheckboxValue(bool isChecked, FormFieldDefinition? fieldDefinition)
        => fieldDefinition?.DataType switch
        {
            FieldDataType.String => isChecked ? "true" : "false",
            FieldDataType.Int32 => isChecked ? 1 : 0,
            FieldDataType.Int64 => isChecked ? 1L : 0L,
            FieldDataType.Decimal => isChecked ? 1m : 0m,
            FieldDataType.Double => isChecked ? 1d : 0d,
            FieldDataType.Boolean => isChecked,
            _ => isChecked,
        };

    public static object? ConvertChoiceValue(string? rawValue, FormFieldDefinition? fieldDefinition)
    {
        if (rawValue is null)
            return null;

        string trimmed = rawValue.Trim();
        if (fieldDefinition is null)
            return trimmed;

        return fieldDefinition.DataType switch
        {
            FieldDataType.Boolean => TryConvertToBoolean(trimmed, out bool boolValue) ? boolValue : trimmed,
            FieldDataType.Int32 => TryConvertToInt32(trimmed, out int intValue) ? intValue : trimmed,
            FieldDataType.Int64 => TryConvertToInt64(trimmed, out long longValue) ? longValue : trimmed,
            FieldDataType.Decimal => TryConvertToDecimal(trimmed, out decimal decimalValue) ? decimalValue : trimmed,
            FieldDataType.Double => TryConvertToDouble(trimmed, out double doubleValue) ? doubleValue : trimmed,
            FieldDataType.String => trimmed,
            _ => trimmed,
        };
    }

    public static bool ChoiceMatchesValue(object? currentValue, string? rawChoiceValue, FormFieldDefinition? fieldDefinition)
    {
        if (rawChoiceValue is null)
            return currentValue is null;

        object? choiceValue = ConvertChoiceValue(rawChoiceValue, fieldDefinition);

        if (TryConvertToBoolean(currentValue, out bool currentBool) && TryConvertToBoolean(choiceValue, out bool choiceBool))
            return currentBool == choiceBool;

        if (TryConvertToDecimal(currentValue, out decimal currentDecimal) && TryConvertToDecimal(choiceValue, out decimal choiceDecimal))
            return currentDecimal == choiceDecimal;

        return string.Equals(ToText(currentValue), ToText(choiceValue), StringComparison.OrdinalIgnoreCase);
    }

    private static bool TryConvertToBoolean(object? value, out bool result)
    {
        switch (value)
        {
            case null:
                result = false;
                return false;
            case bool boolean:
                result = boolean;
                return true;
            case string text:
                return TryConvertTextToBoolean(text, out result);
            case JsonElement json:
                return TryConvertJsonElementToBoolean(json, out result);
            case sbyte or byte or short or ushort or int or uint or long or ulong:
                result = Convert.ToInt64(value, CultureInfo.InvariantCulture) != 0L;
                return true;
            case float or double or decimal:
                result = Math.Abs(Convert.ToDouble(value, CultureInfo.InvariantCulture)) > double.Epsilon;
                return true;
            default:
                return TryConvertTextToBoolean(Convert.ToString(value, CultureInfo.InvariantCulture), out result);
        }
    }

    private static bool TryConvertToInt32(string value, out int result)
    {
        if (int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out result))
            return true;

        if (TryConvertTextToBoolean(value, out bool boolValue))
        {
            result = boolValue ? 1 : 0;
            return true;
        }

        result = default;
        return false;
    }

    private static bool TryConvertToInt64(string value, out long result)
    {
        if (long.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out result))
            return true;

        if (TryConvertTextToBoolean(value, out bool boolValue))
        {
            result = boolValue ? 1L : 0L;
            return true;
        }

        result = default;
        return false;
    }

    private static bool TryConvertToDecimal(object? value, out decimal result)
    {
        switch (value)
        {
            case null:
                result = default;
                return false;
            case decimal decimalValue:
                result = decimalValue;
                return true;
            case JsonElement json:
                return TryConvertJsonElementToDecimal(json, out result);
            case bool boolean:
                result = boolean ? 1m : 0m;
                return true;
            case sbyte or byte or short or ushort or int or uint or long or ulong:
                result = Convert.ToDecimal(value, CultureInfo.InvariantCulture);
                return true;
            case float or double:
                result = Convert.ToDecimal(value, CultureInfo.InvariantCulture);
                return true;
            case string text:
                return TryConvertTextToDecimal(text, out result);
            default:
                return TryConvertTextToDecimal(Convert.ToString(value, CultureInfo.InvariantCulture), out result);
        }
    }

    private static bool TryConvertToDouble(string value, out double result)
    {
        if (double.TryParse(value, NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture, out result))
            return true;

        if (TryConvertTextToBoolean(value, out bool boolValue))
        {
            result = boolValue ? 1d : 0d;
            return true;
        }

        result = default;
        return false;
    }

    private static bool TryConvertTextToBoolean(string? value, out bool result)
    {
        string trimmed = value?.Trim() ?? string.Empty;
        if (bool.TryParse(trimmed, out result))
            return true;

        switch (trimmed.ToLowerInvariant())
        {
            case "1":
            case "y":
            case "yes":
            case "on":
                result = true;
                return true;
            case "0":
            case "n":
            case "no":
            case "off":
                result = false;
                return true;
            default:
                result = false;
                return false;
        }
    }

    private static bool TryConvertTextToDecimal(string? value, out decimal result)
    {
        string trimmed = value?.Trim() ?? string.Empty;
        if (decimal.TryParse(trimmed, NumberStyles.Number, CultureInfo.InvariantCulture, out result))
            return true;

        if (TryConvertTextToBoolean(trimmed, out bool boolValue))
        {
            result = boolValue ? 1m : 0m;
            return true;
        }

        result = default;
        return false;
    }

    private static bool TryConvertJsonElementToBoolean(JsonElement value, out bool result)
    {
        switch (value.ValueKind)
        {
            case JsonValueKind.True:
                result = true;
                return true;
            case JsonValueKind.False:
                result = false;
                return true;
            case JsonValueKind.Number when value.TryGetInt64(out long integer):
                result = integer != 0L;
                return true;
            case JsonValueKind.Number:
                result = Math.Abs(value.GetDouble()) > double.Epsilon;
                return true;
            case JsonValueKind.String:
                return TryConvertTextToBoolean(value.GetString(), out result);
            default:
                result = false;
                return false;
        }
    }

    private static bool TryConvertJsonElementToDecimal(JsonElement value, out decimal result)
    {
        switch (value.ValueKind)
        {
            case JsonValueKind.Number:
                result = value.GetDecimal();
                return true;
            case JsonValueKind.True:
                result = 1m;
                return true;
            case JsonValueKind.False:
                result = 0m;
                return true;
            case JsonValueKind.String:
                return TryConvertTextToDecimal(value.GetString(), out result);
            default:
                result = default;
                return false;
        }
    }

    private static string ToText(object? value)
        => value switch
        {
            null => string.Empty,
            JsonElement json when json.ValueKind == JsonValueKind.String => json.GetString() ?? string.Empty,
            JsonElement json => json.ToString(),
            _ => Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty,
        };
}
