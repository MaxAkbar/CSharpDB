using CSharpDB.Admin.Forms.Models;
using CSharpDB.Admin.Forms.Services;

namespace CSharpDB.Admin.Forms.Tests.Services;

public class DefaultFormGeneratorTests
{
    private readonly DefaultFormGenerator _generator = new();

    [Fact]
    public void GenerateDefault_ProducesTwoControlsPerField()
    {
        FormTableDefinition table = CreateTable(
            new FormFieldDefinition("Id", FieldDataType.Int32, false, true),
            new FormFieldDefinition("Name", FieldDataType.String, false, false));

        FormDefinition form = _generator.GenerateDefault(table);

        Assert.Equal(4, form.Controls.Count);
    }

    [Fact]
    public void GenerateDefault_SetsFormMetadata()
    {
        FormTableDefinition table = CreateTable(
            new FormFieldDefinition("Id", FieldDataType.Int32, false, true));

        FormDefinition form = _generator.GenerateDefault(table);

        Assert.Equal("TestTable Form", form.Name);
        Assert.Equal("TestTable", form.TableName);
        Assert.Equal(1, form.DefinitionVersion);
        Assert.Equal("sig:test-table", form.SourceSchemaSignature);
    }

    [Fact]
    public void GenerateDefault_SetsAbsoluteLayout()
    {
        FormTableDefinition table = CreateTable(
            new FormFieldDefinition("Id", FieldDataType.Int32, false, true));

        FormDefinition form = _generator.GenerateDefault(table);

        Assert.Equal("absolute", form.Layout.LayoutMode);
        Assert.Equal(8, form.Layout.GridSize);
        Assert.True(form.Layout.SnapToGrid);
    }

    [Theory]
    [InlineData(FieldDataType.Boolean, "checkbox")]
    [InlineData(FieldDataType.Date, "date")]
    [InlineData(FieldDataType.DateTime, "date")]
    [InlineData(FieldDataType.Int32, "number")]
    [InlineData(FieldDataType.Int64, "number")]
    [InlineData(FieldDataType.Decimal, "number")]
    [InlineData(FieldDataType.Double, "number")]
    [InlineData(FieldDataType.Blob, "attachment")]
    [InlineData(FieldDataType.String, "text")]
    [InlineData(FieldDataType.Guid, "text")]
    public void GenerateDefault_MapsFieldToCorrectControlType(FieldDataType dataType, string expectedControlType)
    {
        FormTableDefinition table = CreateTable(
            new FormFieldDefinition("Field1", dataType, false, false));

        FormDefinition form = _generator.GenerateDefault(table);

        ControlDefinition inputControl = Assert.Single(form.Controls, control => control.ControlType != "label");
        Assert.Equal(expectedControlType, inputControl.ControlType);
    }

    [Fact]
    public void GenerateDefault_FieldWithChoices_MapsToSelect()
    {
        FormTableDefinition table = CreateTable(
            new FormFieldDefinition(
                "Status",
                FieldDataType.String,
                false,
                false,
                Choices: [new EnumChoice("A", "Active"), new EnumChoice("I", "Inactive")]));

        FormDefinition form = _generator.GenerateDefault(table);

        ControlDefinition inputControl = Assert.Single(form.Controls, control => control.ControlType != "label");
        Assert.Equal("select", inputControl.ControlType);
    }

    [Fact]
    public void GenerateDefault_ReadOnlyField_UsesOneWayBinding()
    {
        FormTableDefinition table = CreateTable(
            new FormFieldDefinition("Id", FieldDataType.Int32, false, true));

        FormDefinition form = _generator.GenerateDefault(table);

        ControlDefinition inputControl = Assert.Single(form.Controls, control => control.Binding is not null);
        Assert.Equal("OneWay", inputControl.Binding!.Mode);
    }

    [Fact]
    public void GenerateDefault_EditableField_UsesTwoWayBinding()
    {
        FormTableDefinition table = CreateTable(
            new FormFieldDefinition("Name", FieldDataType.String, false, false));

        FormDefinition form = _generator.GenerateDefault(table);

        ControlDefinition inputControl = Assert.Single(form.Controls, control => control.Binding is not null);
        Assert.Equal("TwoWay", inputControl.Binding!.Mode);
    }

    [Fact]
    public void GenerateDefault_LabelControls_HaveNoBinding()
    {
        FormTableDefinition table = CreateTable(
            new FormFieldDefinition("Name", FieldDataType.String, false, false));

        FormDefinition form = _generator.GenerateDefault(table);

        ControlDefinition label = Assert.Single(form.Controls, control => control.ControlType == "label");
        Assert.Null(label.Binding);
    }

    [Fact]
    public void GenerateDefault_AllControlIds_AreUnique()
    {
        FormTableDefinition table = CreateTable(
            new FormFieldDefinition("Id", FieldDataType.Int32, false, true),
            new FormFieldDefinition("Name", FieldDataType.String, false, false),
            new FormFieldDefinition("Email", FieldDataType.String, true, false));

        FormDefinition form = _generator.GenerateDefault(table);

        List<string> ids = form.Controls.Select(control => control.ControlId).ToList();
        Assert.Equal(ids.Count, ids.Distinct(StringComparer.Ordinal).Count());
    }

    [Fact]
    public void GenerateDefault_UsesDisplayNameForLabelAndCopiesTextConstraints()
    {
        FormTableDefinition table = CreateTable(
            new FormFieldDefinition(
                "FirstName",
                FieldDataType.String,
                false,
                false,
                DisplayName: "First Name",
                Description: "Enter first name",
                MaxLength: 50,
                Regex: "^[A-Za-z]+$"));

        FormDefinition form = _generator.GenerateDefault(table);

        ControlDefinition label = Assert.Single(form.Controls, control => control.ControlType == "label");
        ControlDefinition input = Assert.Single(form.Controls, control => control.ControlType == "text");

        Assert.Equal("First Name", label.Props.Values["text"]);
        Assert.Equal("Enter first name", input.Props.Values["placeholder"]);
        Assert.Equal(50, input.Props.Values["maxLength"]);
        Assert.Equal("^[A-Za-z]+$", input.Props.Values["pattern"]);
    }

    private static FormTableDefinition CreateTable(params FormFieldDefinition[] fields)
        => new("TestTable", "sig:test-table", fields, ["Id"], []);
}
