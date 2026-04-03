using CSharpDB.Admin.Forms.Models;

namespace CSharpDB.Admin.Forms.Tests.Models;

public class ChildRelationResolverTests
{
    [Fact]
    public void GetPanelParentField_ReturnsFirstConfiguredParentField()
    {
        List<ChildTabConfig> tabs =
        [
            new("tab1", "Orders", "Orders", "CustomerId", "", [], true, true, true, []),
            new("tab2", "Notes", "Notes", "AccountNumber", "AccountCode", [], true, true, true, [])
        ];

        string? field = ChildRelationResolver.GetPanelParentField(tabs);

        Assert.Equal("AccountCode", field);
    }

    [Fact]
    public void GetPanelParentValue_UsesConfiguredParentFieldCaseInsensitively()
    {
        List<ChildTabConfig> tabs =
        [
            new("tab1", "Orders", "Orders", "CustomerId", "CustomerCode", [], true, true, true, [])
        ];
        Dictionary<string, object?> record = new(StringComparer.OrdinalIgnoreCase)
        {
            ["customercode"] = "CUST-001"
        };

        object? value = ChildRelationResolver.GetPanelParentValue(tabs, record);

        Assert.Equal("CUST-001", value);
    }

    [Fact]
    public void GetPanelParentValue_FallsBackToPrimaryKeyWhenParentFieldIsUnset()
    {
        List<ChildTabConfig> tabs =
        [
            new("tab1", "Orders", "Orders", "CustomerId", "", [], true, true, true, [])
        ];
        Dictionary<string, object?> record = new(StringComparer.OrdinalIgnoreCase)
        {
            ["OrderId"] = 42L
        };
        FormTableDefinition parentTable = new(
            "Customers",
            "sig:customers",
            [new FormFieldDefinition("OrderId", FieldDataType.Int64, false, false)],
            ["OrderId"],
            []);

        object? value = ChildRelationResolver.GetPanelParentValue(tabs, record, parentTable);

        Assert.Equal(42L, value);
    }

    [Fact]
    public void GetParentValue_UsesRequestedTabParentField_WhenSiblingTabsDiffer()
    {
        List<ChildTabConfig> tabs =
        [
            new("tab1", "Orders", "Orders", "CustomerId", "Id", [], true, true, true, []),
            new("tab2", "Payments", "Payments", "TenantId", "TenantId", [], true, true, true, [])
        ];
        Dictionary<string, object?> record = new(StringComparer.OrdinalIgnoreCase)
        {
            ["Id"] = 42L,
            ["TenantId"] = "tenant-0042"
        };

        object? value = ChildRelationResolver.GetParentValue(tabs[1], record);

        Assert.Equal("tenant-0042", value);
    }
}
