namespace CSharpDB.Admin.Forms.Models;

public static class ChildRelationResolver
{
    public static string? GetPanelParentField(IReadOnlyList<ChildTabConfig> tabs)
        => tabs
            .Select(tab => tab.ParentKeyField)
            .FirstOrDefault(field => !string.IsNullOrWhiteSpace(field));

    public static object? GetPanelParentValue(
        IReadOnlyList<ChildTabConfig> tabs,
        IReadOnlyDictionary<string, object?>? record,
        FormTableDefinition? parentTableDefinition = null)
    {
        if (record is null)
            return null;

        string? configuredParentField = GetPanelParentField(tabs);
        if (!string.IsNullOrWhiteSpace(configuredParentField) &&
            TryGetFieldValue(record, configuredParentField, out object? configuredValue) &&
            configuredValue is not null)
        {
            return configuredValue;
        }

        if (parentTableDefinition?.PrimaryKey is { Count: > 0 } parentPrimaryKey)
        {
            string pkField = parentPrimaryKey[0];
            if (TryGetFieldValue(record, pkField, out object? pkValue) && pkValue is not null)
                return pkValue;
        }

        if (TryGetFieldValue(record, "id", out object? idValue) && idValue is not null)
            return idValue;

        if (TryGetFieldValue(record, "Id", out object? pascalIdValue) && pascalIdValue is not null)
            return pascalIdValue;

        return null;
    }

    public static bool TryGetFieldValue(IReadOnlyDictionary<string, object?> record, string fieldName, out object? value)
    {
        if (record.TryGetValue(fieldName, out value))
            return true;

        string? key = record.Keys.FirstOrDefault(candidate => string.Equals(candidate, fieldName, StringComparison.OrdinalIgnoreCase));
        if (key is not null && record.TryGetValue(key, out value))
            return true;

        value = null;
        return false;
    }
}
