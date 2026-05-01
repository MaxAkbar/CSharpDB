using System.Text.Json;
using CSharpDB.Admin.Forms.Models;
using CSharpDB.Admin.Forms.Serialization;

namespace CSharpDB.Admin.Forms.Tests.Serialization;

public sealed class CustomControlRoundtripTests
{
    [Fact]
    public void CustomControl_MetadataRoundTripsWithoutSchemaMigration()
    {
        var form = new FormDefinition(
            "custom-form",
            "Custom Form",
            "Orders",
            1,
            "orders:v1",
            new LayoutDefinition("absolute", 8, true, [new Breakpoint("md", 0, null)]),
            [
                new ControlDefinition(
                    "rating1",
                    "rating",
                    new Rect(10, 20, 180, 42),
                    new BindingDefinition("Rating", "TwoWay"),
                    new PropertyBag(new Dictionary<string, object?>
                    {
                        ["displayMode"] = "star",
                        ["max"] = 5L,
                        ["thresholds"] = new object?[]
                        {
                            new Dictionary<string, object?> { ["value"] = 3L, ["label"] = "Review" },
                        },
                        ["parentControlId"] = "tabs",
                        ["parentTabId"] = "details",
                    }),
                    null),
            ]);

        string json = JsonSerializer.Serialize(form, JsonDefaults.Options);
        FormDefinition deserialized = JsonSerializer.Deserialize<FormDefinition>(json, JsonDefaults.Options)!;

        ControlDefinition control = Assert.Single(deserialized.Controls);
        Assert.Equal("rating", control.ControlType);
        Assert.Equal("Rating", control.Binding!.FieldName);
        Assert.Equal("star", control.Props.Values["displayMode"]);
        Assert.Equal(5L, control.Props.Values["max"]);
        Assert.Equal("tabs", control.Props.Values["parentControlId"]);
        Assert.Equal("details", control.Props.Values["parentTabId"]);
        object?[] thresholds = Assert.IsType<object?[]>(control.Props.Values["thresholds"]);
        var threshold = Assert.IsType<Dictionary<string, object?>>(thresholds[0]);
        Assert.Equal(3L, threshold["value"]);
        Assert.Equal("Review", threshold["label"]);
    }
}
