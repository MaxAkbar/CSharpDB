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
