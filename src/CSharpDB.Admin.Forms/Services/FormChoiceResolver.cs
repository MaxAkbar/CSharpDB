using System.Globalization;
using System.Text.Json;
using CSharpDB.Admin.Forms.Models;

namespace CSharpDB.Admin.Forms.Services;

public static class FormChoiceResolver
{
    private static readonly HashSet<string> s_choiceControlTypes = new(StringComparer.OrdinalIgnoreCase)
    {
        "select",
        "lookup",
        "comboBox",
        "listBox",
        "optionGroup",
        "radio",
    };

    public static bool IsChoiceControl(ControlDefinition control)
        => s_choiceControlTypes.Contains(control.ControlType);

    public static bool UsesLookupChoices(ControlDefinition control)
        => IsChoiceControl(control)
           && TryGetString(control.Props.Values, "lookupTable", out _)
           && TryGetString(control.Props.Values, "valueField", out _)
           && TryGetString(control.Props.Values, "displayField", out _);

    public static IReadOnlyList<EnumChoice> ResolveChoices(
        ControlDefinition control,
        string? fieldName,
        IReadOnlyDictionary<string, IReadOnlyList<EnumChoice>>? runtimeChoices,
        FormFieldDefinition? fieldDefinition = null)
    {
        IReadOnlyList<EnumChoice> staticChoices = ReadOptions(control.Props.Values.TryGetValue("options", out object? options) ? options : null);
        if (staticChoices.Count > 0)
            return staticChoices;

        if (runtimeChoices is not null &&
            runtimeChoices.TryGetValue(control.ControlId, out IReadOnlyList<EnumChoice>? controlChoices) &&
            controlChoices is not null)
        {
            return controlChoices;
        }

        if (!string.IsNullOrWhiteSpace(fieldName) &&
            runtimeChoices is not null &&
            runtimeChoices.TryGetValue(fieldName, out IReadOnlyList<EnumChoice>? fieldChoices) &&
            fieldChoices is not null)
        {
            return fieldChoices;
        }

        return fieldDefinition?.Choices is { Count: > 0 } schemaChoices
            ? schemaChoices
            : [];
    }

    public static IReadOnlyList<EnumChoice> BuildLookupChoices(
        IEnumerable<IReadOnlyDictionary<string, object?>> rows,
        string valueField,
        string displayField,
        IReadOnlyList<string>? displayFields = null)
    {
        string[] effectiveDisplayFields = displayFields is { Count: > 0 }
            ? displayFields.Where(field => !string.IsNullOrWhiteSpace(field)).ToArray()
            : [displayField];

        return rows
            .Select(row => new EnumChoice(
                LookupField(row, valueField),
                BuildDisplayLabel(row, effectiveDisplayFields)))
            .Where(choice => !string.IsNullOrWhiteSpace(choice.Value))
            .ToArray();
    }

    public static IReadOnlyList<EnumChoice> ReadOptions(object? value)
    {
        value = NormalizeJsonValue(value);
        if (value is null)
            return [];

        if (value is JsonElement json)
            return ReadOptionsFromJson(json);

        if (value is IEnumerable<EnumChoice> enumChoices)
            return enumChoices.ToArray();

        if (value is IEnumerable<object?> items)
        {
            return items
                .Select(ReadOption)
                .Where(choice => choice is not null)
                .Select(choice => choice!)
                .ToArray();
        }

        return [];
    }

    public static IReadOnlyList<string> ReadStringList(object? value)
    {
        value = NormalizeJsonValue(value);
        if (value is null)
            return [];

        if (value is JsonElement json && json.ValueKind == JsonValueKind.Array)
            return json.EnumerateArray()
                .Select(element => element.ValueKind == JsonValueKind.String ? element.GetString() : element.ToString())
                .Where(text => !string.IsNullOrWhiteSpace(text))
                .Select(text => text!)
                .ToArray();

        if (value is IEnumerable<object?> items)
            return items
                .Select(item => NormalizeJsonValue(item)?.ToString())
                .Where(text => !string.IsNullOrWhiteSpace(text))
                .Select(text => text!)
                .ToArray();

        string? single = value.ToString();
        return string.IsNullOrWhiteSpace(single)
            ? []
            : single.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
    }

    public static int ReadInt(IReadOnlyDictionary<string, object?> props, string key, int fallback)
    {
        if (!props.TryGetValue(key, out object? value) || value is null)
            return fallback;

        value = NormalizeJsonValue(value);
        return value switch
        {
            int i => i,
            long l => checked((int)l),
            double d => checked((int)d),
            decimal m => checked((int)m),
            JsonElement json when json.ValueKind == JsonValueKind.Number && json.TryGetInt32(out int i) => i,
            _ when int.TryParse(value?.ToString(), NumberStyles.Integer, CultureInfo.InvariantCulture, out int parsed) => parsed,
            _ => fallback,
        };
    }

    public static bool TryGetString(IReadOnlyDictionary<string, object?> props, string key, out string value)
    {
        value = string.Empty;
        if (!props.TryGetValue(key, out object? raw) || raw is null)
            return false;

        raw = NormalizeJsonValue(raw);
        value = raw?.ToString() ?? string.Empty;
        return !string.IsNullOrWhiteSpace(value);
    }

    private static EnumChoice? ReadOption(object? value)
    {
        value = NormalizeJsonValue(value);
        if (value is null)
            return null;

        if (value is JsonElement json)
            return ReadOptionFromJson(json);

        if (value is IReadOnlyDictionary<string, object?> readOnly)
            return ReadOptionFromDictionary(readOnly);

        if (value is IDictionary<string, object?> dictionary)
            return ReadOptionFromDictionary(dictionary.ToDictionary(pair => pair.Key, pair => pair.Value, StringComparer.OrdinalIgnoreCase));

        string? scalar = value.ToString();
        return string.IsNullOrWhiteSpace(scalar)
            ? null
            : new EnumChoice(scalar, scalar);
    }

    private static EnumChoice? ReadOptionFromDictionary(IReadOnlyDictionary<string, object?> dictionary)
    {
        string optionValue = ReadDictionaryText(dictionary, "value");
        if (string.IsNullOrWhiteSpace(optionValue))
            return null;

        string label = ReadDictionaryText(dictionary, "label");
        return new EnumChoice(optionValue, string.IsNullOrWhiteSpace(label) ? optionValue : label);
    }

    private static string ReadDictionaryText(IReadOnlyDictionary<string, object?> dictionary, string key)
    {
        if (!dictionary.TryGetValue(key, out object? value) || value is null)
            return string.Empty;

        return NormalizeJsonValue(value)?.ToString() ?? string.Empty;
    }

    private static IReadOnlyList<EnumChoice> ReadOptionsFromJson(JsonElement json)
    {
        if (json.ValueKind != JsonValueKind.Array)
            return [];

        return json.EnumerateArray()
            .Select(ReadOptionFromJson)
            .Where(choice => choice is not null)
            .Select(choice => choice!)
            .ToArray();
    }

    private static EnumChoice? ReadOptionFromJson(JsonElement json)
    {
        if (json.ValueKind == JsonValueKind.Object)
        {
            string optionValue = json.TryGetProperty("value", out JsonElement valueElement)
                ? ReadJsonText(valueElement)
                : string.Empty;
            if (string.IsNullOrWhiteSpace(optionValue))
                return null;

            string label = json.TryGetProperty("label", out JsonElement labelElement)
                ? ReadJsonText(labelElement)
                : optionValue;
            return new EnumChoice(optionValue, string.IsNullOrWhiteSpace(label) ? optionValue : label);
        }

        string scalar = ReadJsonText(json);
        return string.IsNullOrWhiteSpace(scalar)
            ? null
            : new EnumChoice(scalar, scalar);
    }

    private static string ReadJsonText(JsonElement value)
        => value.ValueKind == JsonValueKind.String
            ? value.GetString() ?? string.Empty
            : value.ToString();

    private static string LookupField(IReadOnlyDictionary<string, object?> row, string fieldName)
    {
        if (row.TryGetValue(fieldName, out object? value) && value is not null)
            return value.ToString() ?? string.Empty;

        string? actualKey = row.Keys.FirstOrDefault(candidate => string.Equals(candidate, fieldName, StringComparison.OrdinalIgnoreCase));
        return actualKey is not null && row.TryGetValue(actualKey, out value) && value is not null
            ? value.ToString() ?? string.Empty
            : string.Empty;
    }

    private static string BuildDisplayLabel(IReadOnlyDictionary<string, object?> row, IReadOnlyList<string> displayFields)
    {
        string label = string.Join(" - ", displayFields.Select(field => LookupField(row, field)).Where(value => value.Length > 0));
        return label.Length == 0 && displayFields.Count > 0
            ? LookupField(row, displayFields[0])
            : label;
    }

    private static object? NormalizeJsonValue(object? value)
        => value is JsonElement json ? NormalizeJsonElement(json) : value;

    private static object? NormalizeJsonElement(JsonElement json)
    {
        return json.ValueKind switch
        {
            JsonValueKind.Null => null,
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.String => json.GetString(),
            JsonValueKind.Number when json.TryGetInt64(out long integer) => integer,
            JsonValueKind.Number => json.GetDouble(),
            _ => json,
        };
    }
}
