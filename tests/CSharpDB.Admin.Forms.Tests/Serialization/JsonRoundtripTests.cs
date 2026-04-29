using System.Text.Json;
using CSharpDB.Admin.Forms.Models;
using CSharpDB.Admin.Forms.Serialization;
using CSharpDB.Admin.Forms.Services;
using CSharpDB.Primitives;

namespace CSharpDB.Admin.Forms.Tests.Serialization;

public class JsonRoundtripTests
{
    private static readonly JsonSerializerOptions Options = JsonDefaults.Options;

    [Fact]
    public void FormTableDefinition_RoundTrips()
    {
        var table = CreateSampleTable();

        var json = JsonSerializer.Serialize(table, Options);
        var deserialized = JsonSerializer.Deserialize<FormTableDefinition>(json, Options)!;

        Assert.Equal(table.TableName, deserialized.TableName);
        Assert.Equal(table.SourceSchemaSignature, deserialized.SourceSchemaSignature);
        Assert.Equal(table.Fields.Count, deserialized.Fields.Count);
        Assert.Equal(table.Fields[0].Name, deserialized.Fields[0].Name);
        Assert.Equal(table.Fields[0].DataType, deserialized.Fields[0].DataType);
        Assert.Equal(table.PrimaryKey.Count, deserialized.PrimaryKey.Count);
    }

    [Fact]
    public void FormDefinition_RoundTrips()
    {
        var form = CreateSampleForm();

        var json = JsonSerializer.Serialize(form, Options);
        var deserialized = JsonSerializer.Deserialize<FormDefinition>(json, Options)!;

        Assert.Equal(form.FormId, deserialized.FormId);
        Assert.Equal(form.Name, deserialized.Name);
        Assert.Equal(form.DefinitionVersion, deserialized.DefinitionVersion);
        Assert.Equal(form.Layout.LayoutMode, deserialized.Layout.LayoutMode);
        Assert.Equal(form.Layout.GridSize, deserialized.Layout.GridSize);
        Assert.Equal(form.Controls.Count, deserialized.Controls.Count);
        Assert.NotNull(deserialized.EventBindings);
        Assert.Single(deserialized.EventBindings);
        Assert.Equal(FormEventKind.AfterUpdate, deserialized.EventBindings[0].Event);
        Assert.Equal("AuditChange", deserialized.EventBindings[0].CommandName);
        Assert.Equal("manual", deserialized.EventBindings[0].Arguments!["reason"]);
        ControlEventBinding controlBinding = Assert.Single(deserialized.Controls[1].EventBindings!);
        Assert.Equal(ControlEventKind.OnChange, controlBinding.Event);
        Assert.Equal("NormalizeName", controlBinding.CommandName);
        Assert.Equal("control", controlBinding.Arguments!["source"]);
    }

    [Fact]
    public void CamelCase_IsAppliedInOutput()
    {
        var rect = new Rect(10, 20, 100, 50);
        var json = JsonSerializer.Serialize(rect, Options);

        Assert.Contains("\"x\":", json);
        Assert.Contains("\"y\":", json);
        Assert.Contains("\"width\":", json);
        Assert.Contains("\"height\":", json);
        Assert.DoesNotContain("\"X\":", json);
    }

    [Fact]
    public void NullOptionalFields_AreOmitted()
    {
        var field = new FormFieldDefinition("Name", FieldDataType.String, false, false);
        var json = JsonSerializer.Serialize(field, Options);

        Assert.DoesNotContain("\"displayName\"", json);
        Assert.DoesNotContain("\"maxLength\"", json);
        Assert.DoesNotContain("\"regex\"", json);
        Assert.DoesNotContain("\"choices\"", json);
    }

    [Fact]
    public void PropertyBag_WithMixedTypes_RoundTrips()
    {
        var bag = new PropertyBag(new Dictionary<string, object?>
        {
            ["text"] = "hello",
            ["count"] = 42L,
            ["rate"] = 3.14,
            ["enabled"] = true,
            ["nothing"] = null
        });

        var json = JsonSerializer.Serialize(bag, Options);
        var deserialized = JsonSerializer.Deserialize<PropertyBag>(json, Options)!;

        Assert.Equal("hello", deserialized.Values["text"]);
        Assert.Equal(42L, deserialized.Values["count"]);
        Assert.Equal(3.14, deserialized.Values["rate"]);
        Assert.Equal(true, deserialized.Values["enabled"]);
        // null values are omitted via WhenWritingNull, so "nothing" won't be in the output
    }

    [Fact]
    public void PropertyBag_WithNestedObject_RoundTrips()
    {
        var bag = new PropertyBag(new Dictionary<string, object?>
        {
            ["options"] = new Dictionary<string, object?>
            {
                ["min"] = 0L,
                ["max"] = 100L
            }
        });

        var json = JsonSerializer.Serialize(bag, Options);
        var deserialized = JsonSerializer.Deserialize<PropertyBag>(json, Options)!;

        var nested = deserialized.Values["options"] as Dictionary<string, object?>;
        Assert.NotNull(nested);
        Assert.Equal(0L, nested["min"]);
        Assert.Equal(100L, nested["max"]);
    }

    [Fact]
    public void PropertyBag_WithArray_RoundTrips()
    {
        var bag = new PropertyBag(new Dictionary<string, object?>
        {
            ["tags"] = new object?[] { "a", "b", "c" }
        });

        var json = JsonSerializer.Serialize(bag, Options);
        var deserialized = JsonSerializer.Deserialize<PropertyBag>(json, Options)!;

        var array = deserialized.Values["tags"] as object?[];
        Assert.NotNull(array);
        Assert.Equal(3, array.Length);
        Assert.Equal("b", array[1]);
    }

    [Fact]
    public void ControlDefinition_WithBinding_RoundTrips()
    {
        var control = new ControlDefinition(
            "c1", "text", new Rect(220, 24, 320, 34),
            new BindingDefinition("FirstName", "TwoWay"),
            new PropertyBag(new Dictionary<string, object?>
            {
                ["placeholder"] = "Enter first name",
                ["maxLength"] = 50L
            }),
            null);

        var json = JsonSerializer.Serialize(control, Options);
        var deserialized = JsonSerializer.Deserialize<ControlDefinition>(json, Options)!;

        Assert.Equal("c1", deserialized.ControlId);
        Assert.Equal("text", deserialized.ControlType);
        Assert.Equal(220, deserialized.Rect.X);
        Assert.NotNull(deserialized.Binding);
        Assert.Equal("FirstName", deserialized.Binding.FieldName);
        Assert.Equal("TwoWay", deserialized.Binding.Mode);
        Assert.Equal("Enter first name", deserialized.Props.Values["placeholder"]);
    }

    [Fact]
    public void FormEventBinding_WithActionSequence_RoundTrips()
    {
        var form = new FormDefinition(
            "f-actions",
            "Action Form",
            "Orders",
            1,
            "orders:v1",
            new LayoutDefinition("absolute", 8, true, []),
            [],
            EventBindings:
            [
                new FormEventBinding(
                    FormEventKind.OnLoad,
                    string.Empty,
                    ActionSequence: new DbActionSequence(
                    [
                        new DbActionStep(DbActionKind.SetFieldValue, Target: "Status", Value: "Ready"),
                        new DbActionStep(
                            DbActionKind.RunCommand,
                            CommandName: "AuditAction",
                            Arguments: new Dictionary<string, object?> { ["source"] = "roundtrip" },
                            Condition: "Status = 'Ready'"),
                        new DbActionStep(DbActionKind.GoToRecord, Value: 123L),
                        new DbActionStep(DbActionKind.SaveRecord),
                    ],
                    Name: "LoadActions")),
            ]);

        string json = JsonSerializer.Serialize(form, Options);
        FormDefinition deserialized = JsonSerializer.Deserialize<FormDefinition>(json, Options)!;

        DbActionSequence sequence = deserialized.EventBindings![0].ActionSequence!;
        Assert.Equal("LoadActions", sequence.Name);
        Assert.Equal(4, sequence.Steps.Count);
        Assert.Equal(DbActionKind.SetFieldValue, sequence.Steps[0].Kind);
        Assert.Equal("Status", sequence.Steps[0].Target);
        Assert.Equal("Ready", sequence.Steps[0].Value?.ToString());
        Assert.Equal(DbActionKind.RunCommand, sequence.Steps[1].Kind);
        Assert.Equal("AuditAction", sequence.Steps[1].CommandName);
        Assert.Equal("roundtrip", sequence.Steps[1].Arguments!["source"]);
        Assert.Equal("Status = 'Ready'", sequence.Steps[1].Condition);
        Assert.Equal(DbActionKind.GoToRecord, sequence.Steps[2].Kind);
        Assert.Equal("123", sequence.Steps[2].Value?.ToString());
        Assert.Equal(DbActionKind.SaveRecord, sequence.Steps[3].Kind);
    }

    [Fact]
    public void FormAutomationMetadata_NormalizeForExport_RoundTrips()
    {
        var form = new FormDefinition(
            "f-automation",
            "Automation Form",
            "Orders",
            1,
            "orders:v1",
            new LayoutDefinition("absolute", 8, true, []),
            [
                new ControlDefinition(
                    "ship",
                    "commandButton",
                    new Rect(0, 0, 120, 32),
                    null,
                    new PropertyBag(new Dictionary<string, object?> { ["commandName"] = "ShipOrder" }),
                    null),
                new ControlDefinition(
                    "score",
                    "computed",
                    new Rect(0, 40, 120, 32),
                    null,
                    new PropertyBag(new Dictionary<string, object?> { ["formula"] = "=BoostScore(Score)" }),
                    null,
                    EventBindings:
                    [
                        new ControlEventBinding(
                            ControlEventKind.OnChange,
                            "NormalizeScore",
                            ActionSequence: new DbActionSequence(
                            [
                                new DbActionStep(DbActionKind.RunCommand, CommandName: "AuditScore"),
                            ],
                            Name: "ScoreActions")),
                    ]),
            ],
            EventBindings:
            [
                new FormEventBinding(FormEventKind.BeforeInsert, "ValidateOrder"),
            ]);

        FormDefinition normalized = FormAutomationMetadata.NormalizeForExport(form);
        string json = JsonSerializer.Serialize(normalized, Options);
        FormDefinition deserialized = JsonSerializer.Deserialize<FormDefinition>(json, Options)!;

        Assert.NotNull(deserialized.Automation);
        Assert.Equal(DbAutomationMetadata.CurrentMetadataVersion, deserialized.Automation!.MetadataVersion);
        Assert.Contains(deserialized.Automation.Commands!, command => command.Name == "ShipOrder");
        Assert.Contains(deserialized.Automation.Commands!, command => command.Name == "NormalizeScore");
        Assert.Contains(deserialized.Automation.Commands!, command => command.Name == "AuditScore");
        Assert.Contains(deserialized.Automation.Commands!, command => command.Name == "ValidateOrder");
        DbAutomationScalarFunctionReference function = Assert.Single(deserialized.Automation.ScalarFunctions!);
        Assert.Equal("BoostScore", function.Name);
        Assert.Equal(1, function.Arity);
        Assert.Contains("\"automation\"", json);
    }

    [Fact]
    public void ControlDefinition_WithoutBinding_RoundTrips()
    {
        var control = new ControlDefinition(
            "lbl1", "label", new Rect(24, 24, 180, 34),
            Binding: null,
            Props: new PropertyBag(new Dictionary<string, object?> { ["text"] = "First Name" }),
            ValidationOverride: null);

        var json = JsonSerializer.Serialize(control, Options);
        var deserialized = JsonSerializer.Deserialize<ControlDefinition>(json, Options)!;

        Assert.Null(deserialized.Binding);
        Assert.Equal("First Name", deserialized.Props.Values["text"]);
    }

    [Fact]
    public void FormFieldDefinition_WithChoices_RoundTrips()
    {
        var field = new FormFieldDefinition(
            "Status", FieldDataType.String, false, false,
            Choices: [new EnumChoice("A", "Active"), new EnumChoice("I", "Inactive")]);

        var json = JsonSerializer.Serialize(field, Options);
        var deserialized = JsonSerializer.Deserialize<FormFieldDefinition>(json, Options)!;

        Assert.Equal(2, deserialized.Choices!.Count);
        Assert.Equal("A", deserialized.Choices[0].Value);
        Assert.Equal("Inactive", deserialized.Choices[1].Label);
    }

    [Fact]
    public void ControlDefinition_LookupType_RoundTrips()
    {
        var control = new ControlDefinition(
            "lk1", "lookup", new Rect(24, 100, 320, 34),
            new BindingDefinition("ProductId", "TwoWay"),
            new PropertyBag(new Dictionary<string, object?>
            {
                ["lookupTable"] = "Products",
                ["displayField"] = "ProductName",
                ["valueField"] = "ProductId",
                ["placeholder"] = "-- Select product --"
            }),
            null);

        var json = JsonSerializer.Serialize(control, Options);
        var deserialized = JsonSerializer.Deserialize<ControlDefinition>(json, Options)!;

        Assert.Equal("lk1", deserialized.ControlId);
        Assert.Equal("lookup", deserialized.ControlType);
        Assert.NotNull(deserialized.Binding);
        Assert.Equal("ProductId", deserialized.Binding.FieldName);
        Assert.Equal("Products", deserialized.Props.Values["lookupTable"]);
        Assert.Equal("ProductName", deserialized.Props.Values["displayField"]);
        Assert.Equal("ProductId", deserialized.Props.Values["valueField"]);
        Assert.Equal("-- Select product --", deserialized.Props.Values["placeholder"]);
    }

    [Fact]
    public void FieldDataType_SerializesAsCamelCaseString()
    {
        var field = new FormFieldDefinition("Id", FieldDataType.Int32, false, true);
        var json = JsonSerializer.Serialize(field, Options);

        Assert.Contains("\"dataType\":\"int32\"", json);
    }

    [Fact]
    public void ValidationRule_RoundTrips()
    {
        var rule = new ValidationRule("maxLength", "Must be at most {max} characters",
            new Dictionary<string, object?> { ["max"] = 100L });

        var json = JsonSerializer.Serialize(rule, Options);
        var deserialized = JsonSerializer.Deserialize<ValidationRule>(json, Options)!;

        Assert.Equal("maxLength", deserialized.RuleId);
        Assert.Equal(100L, deserialized.Parameters["max"]);
    }

    [Fact]
    public void ControlDefinition_ComputedType_RoundTrips()
    {
        var control = new ControlDefinition(
            "comp1", "computed", new Rect(24, 200, 320, 34),
            new BindingDefinition("LineTotal", "TwoWay"),
            new PropertyBag(new Dictionary<string, object?>
            {
                ["formula"] = "=Quantity * UnitPrice",
                ["format"] = "N2"
            }),
            null);

        var json = JsonSerializer.Serialize(control, Options);
        var deserialized = JsonSerializer.Deserialize<ControlDefinition>(json, Options)!;

        Assert.Equal("comp1", deserialized.ControlId);
        Assert.Equal("computed", deserialized.ControlType);
        Assert.NotNull(deserialized.Binding);
        Assert.Equal("LineTotal", deserialized.Binding.FieldName);
        Assert.Equal("=Quantity * UnitPrice", deserialized.Props.Values["formula"]);
        Assert.Equal("N2", deserialized.Props.Values["format"]);
    }

    private static FormTableDefinition CreateSampleTable()
    {
        return new FormTableDefinition(
            "Customers", "customers:v1",
            [
                new FormFieldDefinition("Id", FieldDataType.Int32, false, true),
                new FormFieldDefinition("FirstName", FieldDataType.String, false, false,
                    DisplayName: "First Name", MaxLength: 50),
                new FormFieldDefinition("Email", FieldDataType.String, true, false,
                    Regex: @"^.+@.+\..+$"),
                new FormFieldDefinition("IsActive", FieldDataType.Boolean, false, false),
                new FormFieldDefinition("CreatedDate", FieldDataType.DateTime, false, true)
            ],
            ["Id"],
            []);
    }

    private static FormDefinition CreateSampleForm()
    {
        var layout = new LayoutDefinition("absolute", 8, true, [new Breakpoint("md", 0, null)]);
        var controls = new List<ControlDefinition>
        {
            new("lbl1", "label", new Rect(24, 24, 180, 34), null,
                new PropertyBag(new Dictionary<string, object?> { ["text"] = "First Name" }), null),
            new("c1", "text", new Rect(220, 24, 320, 34),
                new BindingDefinition("FirstName", "TwoWay"),
                new PropertyBag(new Dictionary<string, object?> { ["placeholder"] = "Enter first name" }), null,
                EventBindings:
                [
                    new ControlEventBinding(
                        ControlEventKind.OnChange,
                        "NormalizeName",
                        new Dictionary<string, object?> { ["source"] = "control" }),
                ])
        };

        return new FormDefinition(
            "f1",
            "Customer Form",
            "Customers",
            1,
            "customers:v1",
            layout,
            controls,
            EventBindings:
            [
                new FormEventBinding(
                    FormEventKind.AfterUpdate,
                    "AuditChange",
                    new Dictionary<string, object?> { ["reason"] = "manual" }),
            ]);
    }
}
