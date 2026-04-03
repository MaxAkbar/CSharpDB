using System.Text.Json;
using CSharpDB.Admin.Forms.Models;
using CSharpDB.Admin.Forms.Serialization;

namespace CSharpDB.Admin.Forms.Tests.Models;

public class ChildTabConfigMapperTests
{
    // ========== FromPropertyBag: Dictionary-based (in-memory) ==========

    [Fact]
    public void FromPropertyBag_NullInput_ReturnsEmptyList()
    {
        var result = ChildTabConfigMapper.FromPropertyBag(null);
        Assert.Empty(result);
    }

    [Fact]
    public void FromPropertyBag_EmptyArray_ReturnsEmptyList()
    {
        var result = ChildTabConfigMapper.FromPropertyBag(Array.Empty<object?>());
        Assert.Empty(result);
    }

    [Fact]
    public void FromPropertyBag_StringInput_ReturnsEmptyList()
    {
        var result = ChildTabConfigMapper.FromPropertyBag("not an array");
        Assert.Empty(result);
    }

    [Fact]
    public void FromPropertyBag_SingleTab_ParsesCorrectly()
    {
        var dict = new Dictionary<string, object?>
        {
            ["id"] = "tab1",
            ["label"] = "Orders",
            ["childTable"] = "Orders",
            ["foreignKeyField"] = "CustomerId",
            ["parentKeyField"] = "Id",
            ["visibleColumns"] = new object?[] { "OrderDate", "TotalAmount" },
            ["allowAdd"] = true,
            ["allowEdit"] = true,
            ["allowDelete"] = false,
            ["childTabs"] = Array.Empty<object?>()
        };

        var result = ChildTabConfigMapper.FromPropertyBag(new object?[] { dict });

        Assert.Single(result);
        var tab = result[0];
        Assert.Equal("tab1", tab.Id);
        Assert.Equal("Orders", tab.Label);
        Assert.Equal("Orders", tab.ChildTable);
        Assert.Equal("CustomerId", tab.ForeignKeyField);
        Assert.Equal("Id", tab.ParentKeyField);
        Assert.Equal(2, tab.VisibleColumns.Count);
        Assert.Equal("OrderDate", tab.VisibleColumns[0]);
        Assert.Equal("TotalAmount", tab.VisibleColumns[1]);
        Assert.True(tab.AllowAdd);
        Assert.True(tab.AllowEdit);
        Assert.False(tab.AllowDelete);
        Assert.Empty(tab.ChildTabs);
    }

    [Fact]
    public void FromPropertyBag_MultipleTabs_ParsesAll()
    {
        var tab1 = CreateTabDict("tab1", "Orders", "Orders");
        var tab2 = CreateTabDict("tab2", "Notes", "CustomerNotes");

        var result = ChildTabConfigMapper.FromPropertyBag(new object?[] { tab1, tab2 });

        Assert.Equal(2, result.Count);
        Assert.Equal("Orders", result[0].Label);
        Assert.Equal("Notes", result[1].Label);
    }

    [Fact]
    public void FromPropertyBag_TwoLevelNesting_ParsesRecursively()
    {
        var childDict = CreateTabDict("child1", "Order Items", "OrderItems");
        var parentDict = CreateTabDict("parent1", "Orders", "Orders");
        parentDict["childTabs"] = new object?[] { childDict };

        var result = ChildTabConfigMapper.FromPropertyBag(new object?[] { parentDict });

        Assert.Single(result);
        Assert.Single(result[0].ChildTabs);
        Assert.Equal("Order Items", result[0].ChildTabs[0].Label);
        Assert.Equal("OrderItems", result[0].ChildTabs[0].ChildTable);
    }

    [Fact]
    public void FromPropertyBag_ThreeLevelNesting_ParsesRecursively()
    {
        var level3 = CreateTabDict("l3", "Item Details", "ItemDetails");
        var level2 = CreateTabDict("l2", "Order Items", "OrderItems");
        level2["childTabs"] = new object?[] { level3 };
        var level1 = CreateTabDict("l1", "Orders", "Orders");
        level1["childTabs"] = new object?[] { level2 };

        var result = ChildTabConfigMapper.FromPropertyBag(new object?[] { level1 });

        Assert.Single(result);
        Assert.Equal("Orders", result[0].Label);
        Assert.Single(result[0].ChildTabs);
        Assert.Equal("Order Items", result[0].ChildTabs[0].Label);
        Assert.Single(result[0].ChildTabs[0].ChildTabs);
        Assert.Equal("Item Details", result[0].ChildTabs[0].ChildTabs[0].Label);
    }

    [Fact]
    public void FromPropertyBag_FiveLevelNesting_ParsesRecursively()
    {
        // Customers -> Orders -> OrderItems -> ItemDetails -> DetailNotes
        var level5 = CreateTabDict("l5", "Detail Notes", "DetailNotes");
        var level4 = CreateTabDict("l4", "Item Details", "ItemDetails");
        level4["childTabs"] = new object?[] { level5 };
        var level3 = CreateTabDict("l3", "Order Items", "OrderItems");
        level3["childTabs"] = new object?[] { level4 };
        var level2 = CreateTabDict("l2", "Orders", "Orders");
        level2["childTabs"] = new object?[] { level3 };

        var result = ChildTabConfigMapper.FromPropertyBag(new object?[] { level2 });

        Assert.Single(result);
        var l1 = result[0];
        Assert.Equal("Orders", l1.Label);

        var l2 = l1.ChildTabs[0];
        Assert.Equal("Order Items", l2.Label);

        var l3 = l2.ChildTabs[0];
        Assert.Equal("Item Details", l3.Label);

        var l4 = l3.ChildTabs[0];
        Assert.Equal("Detail Notes", l4.Label);
        Assert.Empty(l4.ChildTabs);
    }

    [Fact]
    public void FromPropertyBag_MissingFields_DefaultsGracefully()
    {
        var dict = new Dictionary<string, object?>();

        var result = ChildTabConfigMapper.FromPropertyBag(new object?[] { dict });

        Assert.Single(result);
        var tab = result[0];
        Assert.Equal("", tab.Id);
        Assert.Equal("", tab.Label);
        Assert.Equal("", tab.ChildTable);
        Assert.Empty(tab.VisibleColumns);
        Assert.True(tab.AllowAdd);    // default = true
        Assert.True(tab.AllowEdit);   // default = true
        Assert.True(tab.AllowDelete); // default = true
        Assert.Empty(tab.ChildTabs);
    }

    // ========== FromPropertyBag: JsonElement-based (API deserialized) ==========

    [Fact]
    public void FromPropertyBag_JsonElement_SingleTab_ParsesCorrectly()
    {
        var json = """
        [
          {
            "id": "tab1",
            "label": "Orders",
            "childTable": "Orders",
            "foreignKeyField": "CustomerId",
            "parentKeyField": "Id",
            "visibleColumns": ["OrderDate", "TotalAmount"],
            "allowAdd": true,
            "allowEdit": true,
            "allowDelete": false,
            "childTabs": []
          }
        ]
        """;

        var element = JsonSerializer.Deserialize<JsonElement>(json);
        var result = ChildTabConfigMapper.FromPropertyBag(element);

        Assert.Single(result);
        var tab = result[0];
        Assert.Equal("tab1", tab.Id);
        Assert.Equal("Orders", tab.Label);
        Assert.Equal("Orders", tab.ChildTable);
        Assert.Equal(2, tab.VisibleColumns.Count);
        Assert.True(tab.AllowAdd);
        Assert.False(tab.AllowDelete);
    }

    [Fact]
    public void FromPropertyBag_JsonElement_FiveLevelNesting_ParsesCorrectly()
    {
        var json = """
        [
          {
            "id": "l1", "label": "Orders", "childTable": "Orders",
            "foreignKeyField": "CustomerId", "parentKeyField": "Id",
            "visibleColumns": ["OrderDate"], "allowAdd": true, "allowEdit": true, "allowDelete": true,
            "childTabs": [
              {
                "id": "l2", "label": "Order Items", "childTable": "OrderItems",
                "foreignKeyField": "OrderId", "parentKeyField": "OrderId",
                "visibleColumns": ["ProductName", "Qty"], "allowAdd": true, "allowEdit": true, "allowDelete": true,
                "childTabs": [
                  {
                    "id": "l3", "label": "Item Details", "childTable": "ItemDetails",
                    "foreignKeyField": "OrderItemId", "parentKeyField": "OrderItemId",
                    "visibleColumns": ["BatchNumber"], "allowAdd": true, "allowEdit": true, "allowDelete": true,
                    "childTabs": [
                      {
                        "id": "l4", "label": "Detail Notes", "childTable": "DetailNotes",
                        "foreignKeyField": "DetailId", "parentKeyField": "DetailId",
                        "visibleColumns": ["NoteText", "Author"], "allowAdd": true, "allowEdit": true, "allowDelete": false,
                        "childTabs": []
                      }
                    ]
                  }
                ]
              }
            ]
          }
        ]
        """;

        var element = JsonSerializer.Deserialize<JsonElement>(json);
        var result = ChildTabConfigMapper.FromPropertyBag(element);

        Assert.Single(result);
        Assert.Equal("Orders", result[0].Label);
        Assert.Equal("Order Items", result[0].ChildTabs[0].Label);
        Assert.Equal("Item Details", result[0].ChildTabs[0].ChildTabs[0].Label);
        Assert.Equal("Detail Notes", result[0].ChildTabs[0].ChildTabs[0].ChildTabs[0].Label);
        Assert.Empty(result[0].ChildTabs[0].ChildTabs[0].ChildTabs[0].ChildTabs);
    }

    // ========== ToPropertyBag ==========

    [Fact]
    public void ToPropertyBag_EmptyList_ReturnsEmptyArray()
    {
        var result = ChildTabConfigMapper.ToPropertyBag([]);
        Assert.Empty(result);
    }

    [Fact]
    public void ToPropertyBag_SingleTab_ProducesCorrectDictionary()
    {
        var tab = new ChildTabConfig("t1", "Orders", "Orders", "CustomerId", "Id",
            ["OrderDate", "TotalAmount"], true, true, false, []);

        var result = ChildTabConfigMapper.ToPropertyBag([tab]);

        Assert.Single(result);
        var dict = result[0] as Dictionary<string, object?>;
        Assert.NotNull(dict);
        Assert.Equal("t1", dict["id"]);
        Assert.Equal("Orders", dict["label"]);
        Assert.Equal("Orders", dict["childTable"]);
        Assert.Equal("CustomerId", dict["foreignKeyField"]);
        Assert.Equal("Id", dict["parentKeyField"]);
        Assert.True((bool)dict["allowAdd"]!);
        Assert.True((bool)dict["allowEdit"]!);
        Assert.False((bool)dict["allowDelete"]!);

        var columns = dict["visibleColumns"] as object?[];
        Assert.NotNull(columns);
        Assert.Equal(2, columns.Length);
        Assert.Equal("OrderDate", columns[0]);
        Assert.Equal("TotalAmount", columns[1]);
    }

    [Fact]
    public void ToPropertyBag_DeepNesting_PreservesAllLevels()
    {
        var l4 = new ChildTabConfig("l4", "Notes", "DetailNotes", "DetailId", "DetailId",
            ["NoteText"], true, true, true, []);
        var l3 = new ChildTabConfig("l3", "Details", "ItemDetails", "OrderItemId", "OrderItemId",
            ["BatchNumber"], true, true, true, [l4]);
        var l2 = new ChildTabConfig("l2", "Items", "OrderItems", "OrderId", "OrderId",
            ["ProductName"], true, true, true, [l3]);
        var l1 = new ChildTabConfig("l1", "Orders", "Orders", "CustomerId", "Id",
            ["OrderDate"], true, true, true, [l2]);

        var result = ChildTabConfigMapper.ToPropertyBag([l1]);

        // Traverse the dictionary tree
        var d1 = result[0] as Dictionary<string, object?>;
        Assert.Equal("Orders", d1!["label"]);

        var childTabs1 = d1["childTabs"] as object?[];
        var d2 = childTabs1![0] as Dictionary<string, object?>;
        Assert.Equal("Items", d2!["label"]);

        var childTabs2 = d2["childTabs"] as object?[];
        var d3 = childTabs2![0] as Dictionary<string, object?>;
        Assert.Equal("Details", d3!["label"]);

        var childTabs3 = d3["childTabs"] as object?[];
        var d4 = childTabs3![0] as Dictionary<string, object?>;
        Assert.Equal("Notes", d4!["label"]);

        var childTabs4 = d4["childTabs"] as object?[];
        Assert.Empty(childTabs4!);
    }

    // ========== Roundtrip: ToPropertyBag -> FromPropertyBag ==========

    [Fact]
    public void Roundtrip_DictionaryBased_PreservesData()
    {
        var original = new ChildTabConfig("t1", "Orders", "Orders", "CustomerId", "Id",
            ["OrderDate", "TotalAmount"], true, false, true, []);

        var propBag = ChildTabConfigMapper.ToPropertyBag([original]);
        var restored = ChildTabConfigMapper.FromPropertyBag(propBag);

        Assert.Single(restored);
        Assert.Equal(original.Id, restored[0].Id);
        Assert.Equal(original.Label, restored[0].Label);
        Assert.Equal(original.ChildTable, restored[0].ChildTable);
        Assert.Equal(original.ForeignKeyField, restored[0].ForeignKeyField);
        Assert.Equal(original.ParentKeyField, restored[0].ParentKeyField);
        Assert.Equal(original.VisibleColumns, restored[0].VisibleColumns);
        Assert.Equal(original.AllowAdd, restored[0].AllowAdd);
        Assert.Equal(original.AllowEdit, restored[0].AllowEdit);
        Assert.Equal(original.AllowDelete, restored[0].AllowDelete);
    }

    [Fact]
    public void Roundtrip_FiveLevelHierarchy_PreservesAllData()
    {
        var l4 = new ChildTabConfig("l4", "Detail Notes", "DetailNotes", "DetailId", "DetailId",
            ["NoteText", "Author"], true, true, false, []);
        var l3 = new ChildTabConfig("l3", "Item Details", "ItemDetails", "OrderItemId", "OrderItemId",
            ["BatchNumber", "SerialNumber", "WarehouseLocation"], true, true, true, [l4]);
        var l2 = new ChildTabConfig("l2", "Order Items", "OrderItems", "OrderId", "OrderId",
            ["ProductName", "Qty", "UnitPrice"], true, true, true, [l3]);
        var l1 = new ChildTabConfig("l1", "Orders", "Orders", "CustomerId", "Id",
            ["OrderDate", "TotalAmount", "Quantity"], true, true, true, [l2]);

        var propBag = ChildTabConfigMapper.ToPropertyBag([l1]);
        var restored = ChildTabConfigMapper.FromPropertyBag(propBag);

        Assert.Single(restored);
        AssertTabEquals(l1, restored[0]);
        AssertTabEquals(l2, restored[0].ChildTabs[0]);
        AssertTabEquals(l3, restored[0].ChildTabs[0].ChildTabs[0]);
        AssertTabEquals(l4, restored[0].ChildTabs[0].ChildTabs[0].ChildTabs[0]);
    }

    [Fact]
    public void Roundtrip_JsonSerialization_PreservesFiveLevelHierarchy()
    {
        var l4 = new ChildTabConfig("l4", "Detail Notes", "DetailNotes", "DetailId", "DetailId",
            ["NoteText", "Author"], true, true, false, []);
        var l3 = new ChildTabConfig("l3", "Item Details", "ItemDetails", "OrderItemId", "OrderItemId",
            ["BatchNumber", "SerialNumber"], true, true, true, [l4]);
        var l2 = new ChildTabConfig("l2", "Order Items", "OrderItems", "OrderId", "OrderId",
            ["ProductName", "Qty", "UnitPrice"], true, true, true, [l3]);
        var l1 = new ChildTabConfig("l1", "Orders", "Orders", "CustomerId", "Id",
            ["OrderDate", "TotalAmount"], true, true, true, [l2]);

        // Serialize to PropertyBag, then to JSON, then back
        var propBag = ChildTabConfigMapper.ToPropertyBag([l1]);
        var bag = new PropertyBag(new Dictionary<string, object?> { ["tabs"] = propBag });
        var json = JsonSerializer.Serialize(bag, JsonDefaults.Options);
        var deserialized = JsonSerializer.Deserialize<PropertyBag>(json, JsonDefaults.Options)!;
        var tabsValue = deserialized.Values["tabs"];
        var restored = ChildTabConfigMapper.FromPropertyBag(tabsValue);

        Assert.Single(restored);
        AssertTabEquals(l1, restored[0]);
        AssertTabEquals(l2, restored[0].ChildTabs[0]);
        AssertTabEquals(l3, restored[0].ChildTabs[0].ChildTabs[0]);
        AssertTabEquals(l4, restored[0].ChildTabs[0].ChildTabs[0].ChildTabs[0]);
    }

    // ========== CollectChildTableNames ==========

    [Fact]
    public void CollectChildTableNames_EmptyList_ReturnsEmpty()
    {
        var names = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        ChildTabConfigMapper.CollectChildTableNames([], names);
        Assert.Empty(names);
    }

    [Fact]
    public void CollectChildTableNames_SingleLevel_CollectsOne()
    {
        var tabs = new List<ChildTabConfig>
        {
            new("t1", "Orders", "Orders", "CustomerId", "Id", ["OrderDate"], true, true, true, [])
        };
        var names = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        ChildTabConfigMapper.CollectChildTableNames(tabs, names);

        Assert.Single(names);
        Assert.Contains("Orders", names);
    }

    [Fact]
    public void CollectChildTableNames_MultipleSiblingTabs_CollectsAll()
    {
        var tabs = new List<ChildTabConfig>
        {
            new("t1", "Orders", "Orders", "CustomerId", "Id", [], true, true, true, []),
            new("t2", "Notes", "CustomerNotes", "CustomerId", "Id", [], true, true, true, [])
        };
        var names = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        ChildTabConfigMapper.CollectChildTableNames(tabs, names);

        Assert.Equal(2, names.Count);
        Assert.Contains("Orders", names);
        Assert.Contains("CustomerNotes", names);
    }

    [Fact]
    public void CollectChildTableNames_FiveLevelHierarchy_CollectsAll()
    {
        var l4 = new ChildTabConfig("l4", "Notes", "DetailNotes", "DetailId", "DetailId", [], true, true, true, []);
        var l3 = new ChildTabConfig("l3", "Details", "ItemDetails", "OrderItemId", "OrderItemId", [], true, true, true, [l4]);
        var l2 = new ChildTabConfig("l2", "Items", "OrderItems", "OrderId", "OrderId", [], true, true, true, [l3]);
        var l1 = new ChildTabConfig("l1", "Orders", "Orders", "CustomerId", "Id", [], true, true, true, [l2]);

        var names = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        ChildTabConfigMapper.CollectChildTableNames([l1], names);

        Assert.Equal(4, names.Count);
        Assert.Contains("Orders", names);
        Assert.Contains("OrderItems", names);
        Assert.Contains("ItemDetails", names);
        Assert.Contains("DetailNotes", names);
    }

    [Fact]
    public void CollectChildTableNames_DuplicateTableNames_DeduplicatesViaHashSet()
    {
        var childA = new ChildTabConfig("a", "Items A", "OrderItems", "OrderId", "OrderId", [], true, true, true, []);
        var childB = new ChildTabConfig("b", "Items B", "OrderItems", "OrderId", "OrderId", [], true, true, true, []);
        var parent = new ChildTabConfig("p", "Orders", "Orders", "CustomerId", "Id", [], true, true, true, [childA, childB]);

        var names = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        ChildTabConfigMapper.CollectChildTableNames([parent], names);

        // Orders + OrderItems = 2 unique tables (not 3)
        Assert.Equal(2, names.Count);
    }

    [Fact]
    public void CollectChildTableNames_SkipsEmptyTableNames()
    {
        var tabs = new List<ChildTabConfig>
        {
            new("t1", "Empty", "", "fk", "pk", [], true, true, true, [])
        };
        var names = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        ChildTabConfigMapper.CollectChildTableNames(tabs, names);

        Assert.Empty(names);
    }

    // ========== Multiple siblings at each level ==========

    [Fact]
    public void FromPropertyBag_MultipleSiblingsAtMultipleLevels_ParsesAll()
    {
        // Level 1: Orders has two children: OrderItems AND OrderShipments
        var items = CreateTabDict("items", "Order Items", "OrderItems");
        var shipments = CreateTabDict("ships", "Shipments", "Shipments");
        var orders = CreateTabDict("orders", "Orders", "Orders");
        orders["childTabs"] = new object?[] { items, shipments };

        var result = ChildTabConfigMapper.FromPropertyBag(new object?[] { orders });

        Assert.Single(result);
        Assert.Equal(2, result[0].ChildTabs.Count);
        Assert.Equal("Order Items", result[0].ChildTabs[0].Label);
        Assert.Equal("Shipments", result[0].ChildTabs[1].Label);
    }

    // ========== Helpers ==========

    private static void AssertTabEquals(ChildTabConfig expected, ChildTabConfig actual)
    {
        Assert.Equal(expected.Id, actual.Id);
        Assert.Equal(expected.Label, actual.Label);
        Assert.Equal(expected.ChildTable, actual.ChildTable);
        Assert.Equal(expected.ForeignKeyField, actual.ForeignKeyField);
        Assert.Equal(expected.ParentKeyField, actual.ParentKeyField);
        Assert.Equal(expected.VisibleColumns.Count, actual.VisibleColumns.Count);
        for (int i = 0; i < expected.VisibleColumns.Count; i++)
            Assert.Equal(expected.VisibleColumns[i], actual.VisibleColumns[i]);
        Assert.Equal(expected.AllowAdd, actual.AllowAdd);
        Assert.Equal(expected.AllowEdit, actual.AllowEdit);
        Assert.Equal(expected.AllowDelete, actual.AllowDelete);
        Assert.Equal(expected.ChildTabs.Count, actual.ChildTabs.Count);
    }

    private static Dictionary<string, object?> CreateTabDict(string id, string label, string childTable)
    {
        return new Dictionary<string, object?>
        {
            ["id"] = id,
            ["label"] = label,
            ["childTable"] = childTable,
            ["foreignKeyField"] = "FK",
            ["parentKeyField"] = "PK",
            ["visibleColumns"] = new object?[] { "Col1" },
            ["allowAdd"] = true,
            ["allowEdit"] = true,
            ["allowDelete"] = true,
            ["childTabs"] = Array.Empty<object?>()
        };
    }
}
