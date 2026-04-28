using System.Globalization;
using CSharpDB.Admin.Forms.Models;
using CSharpDB.Primitives;

namespace CSharpDB.Admin.Forms.Services;

internal static class FormCommandInvocation
{
    public static Dictionary<string, DbValue> BuildArguments(
        IReadOnlyDictionary<string, object?>? record,
        IReadOnlyDictionary<string, object?>? configuredArguments)
    {
        var arguments = new Dictionary<string, DbValue>(StringComparer.OrdinalIgnoreCase);

        if (record is not null)
        {
            foreach ((string key, object? value) in record)
                arguments[key] = ToDbValue(value);
        }

        if (configuredArguments is not null)
        {
            foreach ((string key, object? value) in configuredArguments)
            {
                if (!string.IsNullOrWhiteSpace(key))
                    arguments[key] = ToDbValue(value);
            }
        }

        return arguments;
    }

    public static Dictionary<string, string> BuildMetadata(FormDefinition form)
    {
        ArgumentNullException.ThrowIfNull(form);

        return new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["surface"] = "AdminForms",
            ["formId"] = form.FormId,
            ["formName"] = form.Name,
            ["tableName"] = form.TableName,
        };
    }

    public static IReadOnlyDictionary<string, object?>? ReadArgumentsProperty(object? value)
    {
        if (value is null)
            return null;

        if (value is IReadOnlyDictionary<string, object?> readOnly)
            return readOnly;

        if (value is Dictionary<string, object?> dictionary)
            return dictionary;

        if (value is System.Text.Json.JsonElement { ValueKind: System.Text.Json.JsonValueKind.Object } json)
        {
            var result = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
            foreach (System.Text.Json.JsonProperty property in json.EnumerateObject())
                result[property.Name] = ReadJsonValue(property.Value);
            return result;
        }

        return null;
    }

    private static object? ReadJsonValue(System.Text.Json.JsonElement value)
    {
        return value.ValueKind switch
        {
            System.Text.Json.JsonValueKind.Null => null,
            System.Text.Json.JsonValueKind.True => true,
            System.Text.Json.JsonValueKind.False => false,
            System.Text.Json.JsonValueKind.Number => value.TryGetInt64(out long longValue) ? longValue : value.GetDouble(),
            System.Text.Json.JsonValueKind.String => value.GetString(),
            System.Text.Json.JsonValueKind.Object => ReadArgumentsProperty(value),
            System.Text.Json.JsonValueKind.Array => value.EnumerateArray().Select(ReadJsonValue).ToArray(),
            _ => value.ToString(),
        };
    }

    private static DbValue ToDbValue(object? value) => value switch
    {
        null => DbValue.Null,
        DbValue dbValue => dbValue,
        bool boolValue => DbValue.FromInteger(boolValue ? 1 : 0),
        byte or sbyte or short or ushort or int or uint or long => DbValue.FromInteger(Convert.ToInt64(value, CultureInfo.InvariantCulture)),
        float or double or decimal => DbValue.FromReal(Convert.ToDouble(value, CultureInfo.InvariantCulture)),
        string text => DbValue.FromText(text),
        Guid guid => DbValue.FromText(guid.ToString("D")),
        DateOnly date => DbValue.FromText(date.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture)),
        DateTime dateTime => DbValue.FromText(dateTime.ToString("O", CultureInfo.InvariantCulture)),
        byte[] bytes => DbValue.FromBlob(bytes),
        _ => DbValue.FromText(Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty),
    };
}
