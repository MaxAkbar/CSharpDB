using CSharpDB.Admin.Forms.Contracts;
using CSharpDB.Admin.Forms.Models;

namespace CSharpDB.Admin.Forms.Services;

public sealed class DefaultFormGenerator : IFormGenerator
{
    public FormDefinition GenerateDefault(FormTableDefinition table)
    {
        var controls = new List<ControlDefinition>();

        const double xLabel = 24;
        const double xControl = 220;
        const double rowH = 34;
        const double labelW = 180;
        const double controlW = 320;
        const double spacing = 12;

        double y = 24;

        foreach (var field in table.Fields)
        {
            controls.Add(new ControlDefinition(
                ControlId: NewId(),
                ControlType: "label",
                Rect: new Rect(xLabel, y, labelW, rowH),
                Binding: null,
                Props: new PropertyBag(new Dictionary<string, object?>
                {
                    ["text"] = field.DisplayName ?? field.Name,
                    ["forField"] = field.Name,
                }),
                ValidationOverride: null));

            string controlType = PickControlType(field);
            controls.Add(new ControlDefinition(
                ControlId: NewId(),
                ControlType: controlType,
                Rect: new Rect(xControl, y, controlW, rowH),
                Binding: new BindingDefinition(field.Name, field.IsReadOnly ? "OneWay" : "TwoWay"),
                Props: BuildProps(field, controlType),
                ValidationOverride: new ValidationOverride(false, [], [])));

            y += rowH + spacing;
        }

        return new FormDefinition(
            FormId: NewId(),
            Name: $"{table.TableName} Form",
            TableName: table.TableName,
            DefinitionVersion: 1,
            SourceSchemaSignature: table.SourceSchemaSignature,
            Layout: new LayoutDefinition("absolute", 8, true, [new Breakpoint("md", 0, null)]),
            Controls: controls);
    }

    private static string PickControlType(FormFieldDefinition field)
    {
        if (field.Choices is { Count: > 0 })
            return "select";

        return field.DataType switch
        {
            FieldDataType.Boolean => "checkbox",
            FieldDataType.Date or FieldDataType.DateTime => "date",
            FieldDataType.Blob => "attachment",
            FieldDataType.Int32 or FieldDataType.Int64 or FieldDataType.Decimal or FieldDataType.Double => "number",
            _ => "text",
        };
    }

    private static PropertyBag BuildProps(FormFieldDefinition field, string controlType)
    {
        var props = new Dictionary<string, object?>
        {
            ["readOnly"] = field.IsReadOnly,
            ["placeholder"] = field.Description,
        };

        if (controlType == "text")
        {
            if (field.MaxLength is not null)
                props["maxLength"] = field.MaxLength;
            if (field.Regex is not null)
                props["pattern"] = field.Regex;
        }

        if (controlType == "number")
        {
            if (field.Min is not null)
                props["min"] = field.Min;
            if (field.Max is not null)
                props["max"] = field.Max;
        }

        if (controlType == "select" && field.Choices is not null)
        {
            props["options"] = field.Choices.Select(choice => new Dictionary<string, object?>
            {
                ["value"] = choice.Value,
                ["label"] = choice.Label,
            }).ToArray();
        }

        return new PropertyBag(props);
    }

    private static string NewId() => Guid.NewGuid().ToString("N");
}
