using System.Text.Json;
using Microsoft.AspNetCore.Components;

namespace CSharpDB.Admin.Forms.Models;

public sealed record FormControlDescriptor
{
    public required string ControlType { get; init; }
    public required string DisplayName { get; init; }
    public string ToolboxGroup { get; init; } = "Custom";
    public string IconText { get; init; } = "?";
    public string? Description { get; init; }
    public double DefaultWidth { get; init; } = 320;
    public double DefaultHeight { get; init; } = 34;
    public bool SupportsBinding { get; init; } = true;
    public bool ParticipatesInTabOrder { get; init; } = true;
    public bool ShowInToolbox { get; init; } = true;
    public int ToolboxGroupOrder { get; init; } = 100;
    public int ToolboxOrder { get; init; } = 100;
    public IReadOnlyDictionary<string, object?> DefaultProps { get; init; } =
        new Dictionary<string, object?>();
    public IReadOnlyList<FormControlPropertyDescriptor> PropertyDescriptors { get; init; } =
        [];
    public Type? DesignerPreviewComponentType { get; init; }
    public Type? RuntimeComponentType { get; init; }
    public Type? PropertyEditorComponentType { get; init; }
    public bool ReplaceBuiltInRuntime { get; init; }
    public bool IsBuiltIn { get; init; }

    public Dictionary<string, object?> CreateDefaultProps()
        => DefaultProps.ToDictionary(pair => pair.Key, pair => CloneDefaultValue(pair.Value), StringComparer.Ordinal);

    private static object? CloneDefaultValue(object? value)
    {
        if (value is null)
            return null;

        if (value is JsonElement json)
            return json.Clone();

        if (value is object?[] array)
            return array.Select(CloneDefaultValue).ToArray();

        if (value is IReadOnlyDictionary<string, object?> readOnlyDictionary)
            return readOnlyDictionary.ToDictionary(pair => pair.Key, pair => CloneDefaultValue(pair.Value), StringComparer.Ordinal);

        if (value is IDictionary<string, object?> dictionary)
            return dictionary.ToDictionary(pair => pair.Key, pair => CloneDefaultValue(pair.Value), StringComparer.Ordinal);

        return value;
    }

    public static void ValidateComponentType(Type? componentType, string propertyName)
    {
        if (componentType is not null && !typeof(IComponent).IsAssignableFrom(componentType))
            throw new ArgumentException($"{propertyName} must implement {nameof(IComponent)}.", propertyName);
    }
}

public sealed record FormControlPropertyDescriptor
{
    public required string Name { get; init; }
    public required string Label { get; init; }
    public FormControlPropertyEditor Editor { get; init; } = FormControlPropertyEditor.Text;
    public object? DefaultValue { get; init; }
    public string? Placeholder { get; init; }
    public string? HelpText { get; init; }
    public IReadOnlyList<FormControlPropertyOption> Options { get; init; } = [];
}

public sealed record FormControlPropertyOption(string Value, string Label);

public enum FormControlPropertyEditor
{
    Text,
    TextArea,
    Number,
    Checkbox,
    Select,
}
