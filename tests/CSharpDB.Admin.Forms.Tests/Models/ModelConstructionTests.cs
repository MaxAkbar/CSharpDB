using CSharpDB.Admin.Forms.Models;

namespace CSharpDB.Admin.Forms.Tests.Models;

public class ModelConstructionTests
{
    [Fact]
    public void FormFieldDefinition_WithRequiredFields_CreatesSuccessfully()
    {
        var field = new FormFieldDefinition("Name", FieldDataType.String, IsNullable: false, IsReadOnly: false);

        Assert.Equal("Name", field.Name);
        Assert.Equal(FieldDataType.String, field.DataType);
        Assert.False(field.IsNullable);
        Assert.False(field.IsReadOnly);
        Assert.Null(field.DisplayName);
        Assert.Null(field.MaxLength);
        Assert.Null(field.Choices);
    }

    [Fact]
    public void FormFieldDefinition_WithAllOptionalFields_CreatesSuccessfully()
    {
        var choices = new List<EnumChoice> { new("M", "Male"), new("F", "Female") };
        var field = new FormFieldDefinition(
            "Gender", FieldDataType.String, IsNullable: true, IsReadOnly: false,
            DisplayName: "Gender", Description: "Select gender",
            MaxLength: 1, Regex: "^[MF]$", Choices: choices);

        Assert.Equal("Gender", field.DisplayName);
        Assert.Equal(1, field.MaxLength);
        Assert.Equal("^[MF]$", field.Regex);
        Assert.Equal(2, field.Choices!.Count);
    }

    [Fact]
    public void FormTableDefinition_ConstructsWithFieldsAndKeys()
    {
        var fields = new[]
        {
            new FormFieldDefinition("Id", FieldDataType.Int32, false, true),
            new FormFieldDefinition("Name", FieldDataType.String, false, false, MaxLength: 100)
        };

        var table = new FormTableDefinition("Customers", "customers:v1", fields, ["Id"], []);

        Assert.Equal("Customers", table.TableName);
        Assert.Equal("customers:v1", table.SourceSchemaSignature);
        Assert.Equal(2, table.Fields.Count);
        Assert.Single(table.PrimaryKey);
        Assert.Empty(table.ForeignKeys);
    }

    [Fact]
    public void FormDefinition_ConstructsWithControlsAndLayout()
    {
        var layout = new LayoutDefinition("absolute", 8, true, [new Breakpoint("md", 0, null)]);
        var control = new ControlDefinition(
            "c1", "text", new Rect(0, 0, 200, 30),
            new BindingDefinition("Name", "TwoWay"),
            PropertyBag.Empty, null);

        var form = new FormDefinition("f1", "Test Form", "Customers", 1, "customers:v1", layout, [control]);

        Assert.Equal("f1", form.FormId);
        Assert.Equal("absolute", form.Layout.LayoutMode);
        Assert.Single(form.Controls);
        Assert.Equal("text", form.Controls[0].ControlType);
    }

    [Fact]
    public void ControlDefinition_WithNullBinding_ValidForLabels()
    {
        var control = new ControlDefinition(
            "lbl1", "label", new Rect(10, 10, 100, 20),
            Binding: null,
            Props: new PropertyBag(new Dictionary<string, object?> { ["text"] = "First Name" }),
            ValidationOverride: null);

        Assert.Null(control.Binding);
        Assert.Equal("First Name", control.Props.Values["text"]);
    }

    [Fact]
    public void Record_WithExpression_CreatesNewInstance()
    {
        var rect = new Rect(0, 0, 100, 50);
        var moved = rect with { X = 50, Y = 25 };

        Assert.Equal(0, rect.X);
        Assert.Equal(50, moved.X);
        Assert.Equal(25, moved.Y);
        Assert.Equal(100, moved.Width);
    }

    [Fact]
    public void Record_Equality_WorksCorrectly()
    {
        var rect1 = new Rect(10, 20, 100, 50);
        var rect2 = new Rect(10, 20, 100, 50);

        Assert.Equal(rect1, rect2);
    }

    [Fact]
    public void PropertyBag_Empty_ReturnsEmptyDictionary()
    {
        Assert.Empty(PropertyBag.Empty.Values);
    }

    [Fact]
    public void PropertyBag_WithMixedTypes_StoresCorrectly()
    {
        var bag = new PropertyBag(new Dictionary<string, object?>
        {
            ["text"] = "hello",
            ["count"] = 42,
            ["enabled"] = true,
            ["nothing"] = null
        });

        Assert.Equal("hello", bag.Values["text"]);
        Assert.Equal(42, bag.Values["count"]);
        Assert.Equal(true, bag.Values["enabled"]);
        Assert.Null(bag.Values["nothing"]);
    }

    [Fact]
    public void ValidationOverride_CanDisableAndAddRules()
    {
        var addedRule = new ValidationRule("custom:email", "Must be valid email",
            new Dictionary<string, object?> { ["pattern"] = @"^.+@.+\..+$" });

        var @override = new ValidationOverride(
            DisableInferredRules: false,
            AddRules: [addedRule],
            DisableRuleIds: ["maxLength"]);

        Assert.False(@override.DisableInferredRules);
        Assert.Single(@override.AddRules);
        Assert.Single(@override.DisableRuleIds);
        Assert.Equal("maxLength", @override.DisableRuleIds[0]);
    }

    [Fact]
    public void FormForeignKeyDefinition_ConstructsCorrectly()
    {
        var fk = new FormForeignKeyDefinition(
            "FK_Orders_Customers",
            ["CustomerId"],
            "Customers",
            ["Id"]);

        Assert.Equal("FK_Orders_Customers", fk.Name);
        Assert.Single(fk.LocalFields);
        Assert.Equal("Customers", fk.ReferencedTable);
    }

    // ========== ChildTabConfig ==========

    [Fact]
    public void ChildTabConfig_ConstructsWithAllFields()
    {
        var tab = new ChildTabConfig(
            "tab1", "Orders", "Orders", "CustomerId", "Id",
            ["OrderDate", "TotalAmount"], true, false, true, []);

        Assert.Equal("tab1", tab.Id);
        Assert.Equal("Orders", tab.Label);
        Assert.Equal("Orders", tab.ChildTable);
        Assert.Equal("CustomerId", tab.ForeignKeyField);
        Assert.Equal("Id", tab.ParentKeyField);
        Assert.Equal(2, tab.VisibleColumns.Count);
        Assert.True(tab.AllowAdd);
        Assert.False(tab.AllowEdit);
        Assert.True(tab.AllowDelete);
        Assert.Empty(tab.ChildTabs);
    }

    [Fact]
    public void ChildTabConfig_SupportsRecursiveNesting()
    {
        var leaf = new ChildTabConfig("leaf", "Notes", "DetailNotes", "DetailId", "DetailId",
            ["NoteText"], true, true, true, []);
        var mid = new ChildTabConfig("mid", "Details", "ItemDetails", "OrderItemId", "OrderItemId",
            ["BatchNumber"], true, true, true, [leaf]);
        var root = new ChildTabConfig("root", "Items", "OrderItems", "OrderId", "OrderId",
            ["ProductName"], true, true, true, [mid]);

        Assert.Single(root.ChildTabs);
        Assert.Single(root.ChildTabs[0].ChildTabs);
        Assert.Empty(root.ChildTabs[0].ChildTabs[0].ChildTabs);
        Assert.Equal("Notes", root.ChildTabs[0].ChildTabs[0].Label);
    }

    [Fact]
    public void ChildTabConfig_FiveLevelHierarchy_ConstructsSuccessfully()
    {
        // Customers -> Orders -> OrderItems -> ItemDetails -> DetailNotes
        var l5 = new ChildTabConfig("l5", "Detail Notes", "DetailNotes", "DetailId", "DetailId",
            ["NoteText", "Author"], true, true, false, []);
        var l4 = new ChildTabConfig("l4", "Item Details", "ItemDetails", "OrderItemId", "OrderItemId",
            ["BatchNumber", "SerialNumber", "WarehouseLocation"], true, true, true, [l5]);
        var l3 = new ChildTabConfig("l3", "Order Items", "OrderItems", "OrderId", "OrderId",
            ["ProductName", "Qty", "UnitPrice"], true, true, true, [l4]);
        var l2 = new ChildTabConfig("l2", "Orders", "Orders", "CustomerId", "Id",
            ["OrderDate", "TotalAmount", "Quantity"], true, true, true, [l3]);

        // Verify the full chain
        Assert.Equal("Orders", l2.Label);
        Assert.Equal("Order Items", l2.ChildTabs[0].Label);
        Assert.Equal("Item Details", l2.ChildTabs[0].ChildTabs[0].Label);
        Assert.Equal("Detail Notes", l2.ChildTabs[0].ChildTabs[0].ChildTabs[0].Label);
        Assert.Empty(l2.ChildTabs[0].ChildTabs[0].ChildTabs[0].ChildTabs);

        // Verify FK chain
        Assert.Equal("CustomerId", l2.ForeignKeyField);
        Assert.Equal("OrderId", l3.ForeignKeyField);
        Assert.Equal("OrderItemId", l4.ForeignKeyField);
        Assert.Equal("DetailId", l5.ForeignKeyField);
    }

    [Fact]
    public void ChildTabConfig_WithExpression_CreatesModifiedCopy()
    {
        var original = new ChildTabConfig("t1", "Orders", "Orders", "CustomerId", "Id",
            ["OrderDate"], true, true, true, []);
        var modified = original with { Label = "Updated Orders", AllowDelete = false };

        Assert.Equal("Orders", original.Label);
        Assert.True(original.AllowDelete);
        Assert.Equal("Updated Orders", modified.Label);
        Assert.False(modified.AllowDelete);
    }

    [Fact]
    public void ChildTabConfig_MultipleSiblingTabs()
    {
        var child1 = new ChildTabConfig("c1", "Items", "OrderItems", "OrderId", "OrderId",
            ["ProductName"], true, true, true, []);
        var child2 = new ChildTabConfig("c2", "Shipments", "Shipments", "OrderId", "OrderId",
            ["ShipDate"], true, false, false, []);
        var parent = new ChildTabConfig("p1", "Orders", "Orders", "CustomerId", "Id",
            ["OrderDate"], true, true, true, [child1, child2]);

        Assert.Equal(2, parent.ChildTabs.Count);
        Assert.Equal("Items", parent.ChildTabs[0].Label);
        Assert.Equal("Shipments", parent.ChildTabs[1].Label);
        Assert.True(parent.ChildTabs[0].AllowEdit);
        Assert.False(parent.ChildTabs[1].AllowEdit);
    }
}
