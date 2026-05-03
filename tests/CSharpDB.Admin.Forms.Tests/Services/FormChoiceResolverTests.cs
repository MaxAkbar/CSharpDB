using System.Text.Json;
using CSharpDB.Admin.Forms.Models;
using CSharpDB.Admin.Forms.Services;
using CSharpDB.Primitives;

namespace CSharpDB.Admin.Forms.Tests.Services;

public sealed class FormChoiceResolverTests
{
    [Fact]
    public void ResolveChoices_PrefersStaticOptionsFromPropertyBag()
    {
        var control = new ControlDefinition(
            "status",
            "comboBox",
            new Rect(0, 0, 200, 32),
            new BindingDefinition("Status", "TwoWay"),
            new PropertyBag(new Dictionary<string, object?>
            {
                ["options"] = new object?[]
                {
                    new Dictionary<string, object?> { ["value"] = "A", ["label"] = "Active" },
                },
            }),
            null);
        var runtimeChoices = new Dictionary<string, IReadOnlyList<EnumChoice>>
        {
            ["Status"] = [new EnumChoice("I", "Inactive")],
        };

        IReadOnlyList<EnumChoice> choices = FormChoiceResolver.ResolveChoices(control, "Status", runtimeChoices);

        EnumChoice choice = Assert.Single(choices);
        Assert.Equal("A", choice.Value);
        Assert.Equal("Active", choice.Label);
    }

    [Fact]
    public void BuildLookupChoices_UsesConfiguredDisplayFields()
    {
        var rows = new[]
        {
            new Dictionary<string, object?>
            {
                ["Id"] = 7L,
                ["Code"] = "ALP",
                ["Name"] = "Alpha",
            },
        };

        IReadOnlyList<EnumChoice> choices = FormChoiceResolver.BuildLookupChoices(rows, "Id", "Name", ["Code", "Name"]);

        EnumChoice choice = Assert.Single(choices);
        Assert.Equal("7", choice.Value);
        Assert.Equal("ALP - Alpha", choice.Label);
    }

    [Fact]
    public void ReadOptions_HandlesJsonElementArrays()
    {
        using JsonDocument document = JsonDocument.Parse(
            """
            [{"value":"1","label":"One"}]
            """);

        IReadOnlyList<EnumChoice> choices = FormChoiceResolver.ReadOptions(document.RootElement);

        EnumChoice choice = Assert.Single(choices);
        Assert.Equal("1", choice.Value);
        Assert.Equal("One", choice.Label);
    }
}
