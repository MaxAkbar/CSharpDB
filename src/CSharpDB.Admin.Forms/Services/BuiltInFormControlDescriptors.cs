using CSharpDB.Admin.Forms.Models;

namespace CSharpDB.Admin.Forms.Services;

internal static class BuiltInFormControlDescriptors
{
    public static void AddTo(FormControlRegistryBuilder builder)
    {
        builder
            .Add(BuiltIn("label", "Label", "Layout", "A", 180, 34, supportsBinding: false, participatesInTabOrder: false, 10, 10, "Static text label",
                new Dictionary<string, object?> { ["text"] = "Label" }))
            .Add(BuiltIn("text", "Text", "Input Controls", "\u2328", 320, 34, supportsBinding: true, participatesInTabOrder: true, 20, 10, "Text input field"))
            .Add(BuiltIn("textarea", "Textarea", "Input Controls", "\u2263", 320, 80, supportsBinding: true, participatesInTabOrder: true, 20, 20, "Multi-line text area"))
            .Add(BuiltIn("number", "Number", "Input Controls", "#", 320, 34, supportsBinding: true, participatesInTabOrder: true, 20, 30, "Number input field"))
            .Add(BuiltIn("date", "Date", "Input Controls", "\U0001F4C5", 320, 34, supportsBinding: true, participatesInTabOrder: true, 20, 40, "Date picker"))
            .Add(BuiltIn("checkbox", "Checkbox", "Input Controls", "\u2611", 200, 34, supportsBinding: true, participatesInTabOrder: true, 20, 50, "Checkbox",
                new Dictionary<string, object?> { ["text"] = "Checkbox" }))
            .Add(BuiltIn("radio", "Radio", "Input Controls", "\u25C9", 200, 80, supportsBinding: true, participatesInTabOrder: true, 20, 60, "Radio button group"))
            .Add(BuiltIn("select", "Select", "Input Controls", "\u25BE", 320, 34, supportsBinding: true, participatesInTabOrder: true, 20, 70, "Dropdown select",
                ChoiceDefaults()))
            .Add(BuiltIn("comboBox", "Combo Box", "Input Controls", "\u2327", 320, 34, supportsBinding: true, participatesInTabOrder: true, 20, 80, "Searchable single-select with optional custom entry",
                ChoiceDefaults(new Dictionary<string, object?> { ["placeholder"] = "Search or select", ["allowCustomValue"] = false })))
            .Add(BuiltIn("listBox", "List Box", "Input Controls", "\u2630", 260, 120, supportsBinding: true, participatesInTabOrder: true, 20, 90, "Always-visible single-select list",
                ChoiceDefaults(new Dictionary<string, object?> { ["visibleRows"] = 5, ["multiSelect"] = false, ["multiValueDelimiter"] = ";" })))
            .Add(BuiltIn("lookup", "Lookup", "Input Controls", "\U0001F50D", 320, 34, supportsBinding: true, participatesInTabOrder: true, 20, 100, "Lookup combo box loaded from a table",
                new Dictionary<string, object?> { ["lookupTable"] = "", ["displayField"] = "", ["valueField"] = "", ["placeholder"] = "-- Select --" }))
            .Add(BuiltIn("optionGroup", "Option Group", "Input Controls", "\u25C9", 220, 100, supportsBinding: true, participatesInTabOrder: true, 20, 110, "Bound scalar option group",
                ChoiceDefaults(new Dictionary<string, object?> { ["orientation"] = "vertical", ["buttonStyle"] = false })))
            .Add(BuiltIn("toggleButton", "Toggle", "Input Controls", "\u25FC", 160, 34, supportsBinding: true, participatesInTabOrder: true, 20, 120, "Boolean or scalar toggle button",
                new Dictionary<string, object?> { ["text"] = "Toggle", ["trueValue"] = true, ["falseValue"] = false }))
            .Add(BuiltIn("computed", "Computed", "Input Controls", "\u03A3", 320, 34, supportsBinding: true, participatesInTabOrder: true, 20, 130, "Computed field",
                new Dictionary<string, object?> { ["formula"] = "", ["format"] = "" }))
            .Add(BuiltIn("datagrid", "DataGrid", "Data", "\u2637", 560, 200, supportsBinding: false, participatesInTabOrder: false, 30, 10, "Table data grid",
                new Dictionary<string, object?>
                {
                    ["dataGridMode"] = "standalone",
                    ["childTable"] = "",
                    ["foreignKeyField"] = "",
                    ["parentKeyField"] = "",
                    ["foreignKeyName"] = "",
                    ["visibleColumns"] = Array.Empty<object?>(),
                    ["allowAdd"] = true,
                    ["allowDelete"] = true,
                    ["allowEdit"] = true,
                }))
            .Add(BuiltIn("childtabs", "Child Tabs", "Data", "\u2630", 600, 280, supportsBinding: false, participatesInTabOrder: false, 30, 20, "Tab-based child forms with nesting",
                new Dictionary<string, object?> { ["tabs"] = Array.Empty<object?>() }))
            .Add(BuiltIn("tabControl", "Tab Control", "Data", "\u25AB", 600, 300, supportsBinding: false, participatesInTabOrder: false, 30, 30, "General tab container for form controls",
                new Dictionary<string, object?>
                {
                    ["tabs"] = new object?[]
                    {
                        new Dictionary<string, object?> { ["id"] = "page1", ["label"] = "Page 1" },
                        new Dictionary<string, object?> { ["id"] = "page2", ["label"] = "Page 2" },
                    },
                }))
            .Add(BuiltIn("subform", "Subform", "Data", "\u25A3", 640, 320, supportsBinding: false, participatesInTabOrder: false, 30, 40, "Embedded saved form linked to the parent record",
                new Dictionary<string, object?> { ["formId"] = "", ["parentKeyField"] = "", ["foreignKeyField"] = "", ["showToolbar"] = true, ["showRecordList"] = true }))
            .Add(BuiltIn("attachment", "Attachment", "Data", "\u2398", 360, 74, supportsBinding: true, participatesInTabOrder: true, 30, 50, "BLOB attachment upload",
                AttachmentDefaults("attachment")))
            .Add(BuiltIn("image", "Image", "Data", "\u25A7", 360, 240, supportsBinding: true, participatesInTabOrder: true, 30, 60, "BLOB image upload and preview",
                AttachmentDefaults("image")))
            .Add(BuiltIn("commandButton", "Button", "Automation", "\u25B6", 160, 34, supportsBinding: false, participatesInTabOrder: true, 40, 10, "Runs a trusted command",
                new Dictionary<string, object?> { ["text"] = "Button", ["commandName"] = "" }));
    }

    private static FormControlDescriptor BuiltIn(
        string controlType,
        string displayName,
        string toolboxGroup,
        string iconText,
        double defaultWidth,
        double defaultHeight,
        bool supportsBinding,
        bool participatesInTabOrder,
        int groupOrder,
        int order,
        string description,
        IReadOnlyDictionary<string, object?>? defaultProps = null)
        => new()
        {
            ControlType = controlType,
            DisplayName = displayName,
            ToolboxGroup = toolboxGroup,
            IconText = iconText,
            Description = description,
            DefaultWidth = defaultWidth,
            DefaultHeight = defaultHeight,
            SupportsBinding = supportsBinding,
            ParticipatesInTabOrder = participatesInTabOrder,
            ToolboxGroupOrder = groupOrder,
            ToolboxOrder = order,
            DefaultProps = defaultProps ?? new Dictionary<string, object?>(),
            IsBuiltIn = true,
        };

    private static Dictionary<string, object?> ChoiceDefaults(Dictionary<string, object?>? extra = null)
    {
        var props = new Dictionary<string, object?>
        {
            ["options"] = new object?[]
            {
                new Dictionary<string, object?> { ["value"] = "1", ["label"] = "Option 1" },
                new Dictionary<string, object?> { ["value"] = "2", ["label"] = "Option 2" },
            },
        };

        if (extra is not null)
        {
            foreach (KeyValuePair<string, object?> pair in extra)
                props[pair.Key] = pair.Value;
        }

        return props;
    }

    private static Dictionary<string, object?> AttachmentDefaults(string controlType)
        => new()
        {
            ["storageMode"] = "blobField",
            ["fileNameField"] = "",
            ["contentTypeField"] = "",
            ["fileSizeField"] = "",
            ["attachmentTable"] = "",
            ["attachmentForeignKeyField"] = "",
            ["attachmentBlobField"] = "",
            ["attachmentFileNameField"] = "",
            ["attachmentContentTypeField"] = "",
            ["attachmentFileSizeField"] = "",
            ["attachmentControlIdField"] = "",
            ["accept"] = controlType == "image" ? "image/*" : "",
        };
}
