using CSharpDB.Admin.Forms.Components.Designer;
using CSharpDB.Admin.Forms.Models;

namespace CSharpDB.Admin.Forms.Tests.Components.Designer;

public sealed class DesignerStateTests
{
    [Fact]
    public void ToFormDefinition_PreservesEventBindings()
    {
        var state = new DesignerState();
        var form = CreateForm() with
        {
            EventBindings =
            [
                new FormEventBinding(
                    FormEventKind.AfterUpdate,
                    "AuditChange",
                    new Dictionary<string, object?> { ["reason"] = "manual" }),
            ],
        };

        state.LoadForm(form);

        FormDefinition saved = state.ToFormDefinition();

        Assert.NotNull(saved.EventBindings);
        FormEventBinding binding = Assert.Single(saved.EventBindings);
        Assert.Equal(FormEventKind.AfterUpdate, binding.Event);
        Assert.Equal("AuditChange", binding.CommandName);
        Assert.Equal("manual", binding.Arguments!["reason"]);
    }

    [Fact]
    public void UpdateEventBindings_ReplacesFormLevelBindings()
    {
        var state = new DesignerState();
        state.LoadForm(CreateForm());

        state.UpdateEventBindings(
        [
            new FormEventBinding(FormEventKind.BeforeDelete, "ConfirmDelete", StopOnFailure: false),
        ]);

        FormDefinition saved = state.ToFormDefinition();

        FormEventBinding binding = Assert.Single(saved.EventBindings!);
        Assert.Equal(FormEventKind.BeforeDelete, binding.Event);
        Assert.Equal("ConfirmDelete", binding.CommandName);
        Assert.False(binding.StopOnFailure);
    }

    [Fact]
    public void UpdateControlEventBindings_ReplacesSelectedControlBindings()
    {
        var state = new DesignerState();
        ControlDefinition textControl = new(
            "name",
            "text",
            new Rect(0, 0, 120, 24),
            new BindingDefinition("Name", "TwoWay"),
            PropertyBag.Empty,
            null);
        state.LoadForm(CreateForm() with { Controls = [textControl] });

        state.UpdateControlEventBindings(
            "name",
            [
                new ControlEventBinding(
                    ControlEventKind.OnChange,
                    "NormalizeName",
                    new Dictionary<string, object?> { ["source"] = "designer" },
                    StopOnFailure: false),
            ]);

        FormDefinition saved = state.ToFormDefinition();

        ControlEventBinding binding = Assert.Single(saved.Controls[0].EventBindings!);
        Assert.Equal(ControlEventKind.OnChange, binding.Event);
        Assert.Equal("NormalizeName", binding.CommandName);
        Assert.Equal("designer", binding.Arguments!["source"]);
        Assert.False(binding.StopOnFailure);
    }

    private static FormDefinition CreateForm()
        => new(
            "customers-form",
            "Customers",
            "Customers",
            1,
            "sig:customers",
            new LayoutDefinition("absolute", 8, true, [new Breakpoint("md", 0, null)]),
            []);
}
