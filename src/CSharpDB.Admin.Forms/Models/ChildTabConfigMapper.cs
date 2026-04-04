using System.Text.Json;

namespace CSharpDB.Admin.Forms.Models;

/// <summary>
/// Converts between PropertyBag's untyped representation (Dictionary/object?[])
/// and strongly-typed ChildTabConfig records.
/// Handles both in-memory (object?[]/Dictionary) and JSON-deserialized (JsonElement) formats.
/// </summary>
public static class ChildTabConfigMapper
{
    /// <summary>Deserialize from PropertyBag value to typed list.</summary>
    public static List<ChildTabConfig> FromPropertyBag(object? tabsValue)
    {
        if (tabsValue is object?[] arr)
            return arr.OfType<Dictionary<string, object?>>().Select(FromDictionary).ToList();

        if (tabsValue is JsonElement je && je.ValueKind == JsonValueKind.Array)
            return je.EnumerateArray().Select(FromJsonElement).ToList();

        return [];
    }

    /// <summary>Serialize typed list back to PropertyBag-compatible format.</summary>
    public static object?[] ToPropertyBag(IReadOnlyList<ChildTabConfig> tabs)
    {
        return tabs.Select(t => (object?)ToDictionary(t)).ToArray();
    }

    /// <summary>Recursively collect all unique child table names from a tab hierarchy.</summary>
    public static void CollectChildTableNames(IReadOnlyList<ChildTabConfig> tabs, HashSet<string> names)
    {
        foreach (var tab in tabs)
        {
            if (!string.IsNullOrEmpty(tab.ChildTable))
                names.Add(tab.ChildTable);
            if (tab.ChildTabs.Count > 0)
                CollectChildTableNames(tab.ChildTabs, names);
        }
    }

    // ===== From Dictionary (in-memory PropertyBag) =====

    private static ChildTabConfig FromDictionary(Dictionary<string, object?> dict)
    {
        return new ChildTabConfig(
            Id: GetString(dict, "id"),
            Label: GetString(dict, "label"),
            ChildTable: GetString(dict, "childTable"),
            ForeignKeyField: GetString(dict, "foreignKeyField"),
            ParentKeyField: GetString(dict, "parentKeyField"),
            VisibleColumns: GetStringList(dict, "visibleColumns"),
            AllowAdd: GetBool(dict, "allowAdd", true),
            AllowEdit: GetBool(dict, "allowEdit", true),
            AllowDelete: GetBool(dict, "allowDelete", true),
            ChildTabs: GetChildTabs(dict, "childTabs"));
    }

    private static string GetString(Dictionary<string, object?> dict, string key)
    {
        return dict.TryGetValue(key, out var v) ? v?.ToString() ?? "" : "";
    }

    private static bool GetBool(Dictionary<string, object?> dict, string key, bool fallback)
    {
        if (!dict.TryGetValue(key, out var v)) return fallback;
        return v switch
        {
            bool b => b,
            string s => bool.TryParse(s, out var sb) && sb,
            _ => fallback
        };
    }

    private static List<string> GetStringList(Dictionary<string, object?> dict, string key)
    {
        if (!dict.TryGetValue(key, out var v)) return [];
        if (v is object?[] arr)
            return arr.Where(x => x is not null).Select(x => x!.ToString()!).ToList();
        if (v is JsonElement je && je.ValueKind == JsonValueKind.Array)
            return je.EnumerateArray().Select(el => el.GetString() ?? "").Where(s => s.Length > 0).ToList();
        return [];
    }

    private static List<ChildTabConfig> GetChildTabs(Dictionary<string, object?> dict, string key)
    {
        if (!dict.TryGetValue(key, out var v)) return [];
        return FromPropertyBag(v);
    }

    // ===== From JsonElement (server deserialized) =====

    private static ChildTabConfig FromJsonElement(JsonElement el)
    {
        return new ChildTabConfig(
            Id: el.TryGetProperty("id", out var id) ? id.GetString() ?? "" : "",
            Label: el.TryGetProperty("label", out var label) ? label.GetString() ?? "" : "",
            ChildTable: el.TryGetProperty("childTable", out var ct) ? ct.GetString() ?? "" : "",
            ForeignKeyField: el.TryGetProperty("foreignKeyField", out var fk) ? fk.GetString() ?? "" : "",
            ParentKeyField: el.TryGetProperty("parentKeyField", out var pk) ? pk.GetString() ?? "" : "",
            VisibleColumns: el.TryGetProperty("visibleColumns", out var vc) && vc.ValueKind == JsonValueKind.Array
                ? vc.EnumerateArray().Select(x => x.GetString() ?? "").Where(s => s.Length > 0).ToList()
                : [],
            AllowAdd: el.TryGetProperty("allowAdd", out var aa) && aa.ValueKind == JsonValueKind.True,
            AllowEdit: el.TryGetProperty("allowEdit", out var ae) && ae.ValueKind == JsonValueKind.True,
            AllowDelete: el.TryGetProperty("allowDelete", out var ad) && ad.ValueKind == JsonValueKind.True,
            ChildTabs: el.TryGetProperty("childTabs", out var cts) && cts.ValueKind == JsonValueKind.Array
                ? cts.EnumerateArray().Select(FromJsonElement).ToList()
                : []);
    }

    // ===== To Dictionary (for PropertyBag storage) =====

    private static Dictionary<string, object?> ToDictionary(ChildTabConfig tab)
    {
        return new Dictionary<string, object?>
        {
            ["id"] = tab.Id,
            ["label"] = tab.Label,
            ["childTable"] = tab.ChildTable,
            ["foreignKeyField"] = tab.ForeignKeyField,
            ["parentKeyField"] = tab.ParentKeyField,
            ["visibleColumns"] = tab.VisibleColumns.Select(x => (object?)x).ToArray(),
            ["allowAdd"] = tab.AllowAdd,
            ["allowEdit"] = tab.AllowEdit,
            ["allowDelete"] = tab.AllowDelete,
            ["childTabs"] = ToPropertyBag(tab.ChildTabs)
        };
    }
}
