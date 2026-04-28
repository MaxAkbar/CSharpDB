using CSharpDB.Admin.Forms.Components.Designer;
using CSharpDB.Admin.Forms.Models;
using CSharpDB.Primitives;

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
    public void ToFormDefinition_PreservesFormActionSequences()
    {
        var state = new DesignerState();
        var form = CreateForm() with
        {
            EventBindings =
            [
                new FormEventBinding(
                    FormEventKind.BeforeInsert,
                    string.Empty,
                    ActionSequence: new DbActionSequence(
                    [
                        new DbActionStep(DbActionKind.SetFieldValue, Target: "Status", Value: "Ready"),
                        new DbActionStep(DbActionKind.RunCommand, CommandName: "AuditChange"),
                    ],
                    Name: "PrepareRecord")),
            ],
        };

        state.LoadForm(form);

        FormDefinition saved = state.ToFormDefinition();

        FormEventBinding binding = Assert.Single(saved.EventBindings!);
        Assert.NotNull(binding.ActionSequence);
        Assert.Equal("PrepareRecord", binding.ActionSequence!.Name);
        Assert.Equal(DbActionKind.SetFieldValue, binding.ActionSequence.Steps[0].Kind);
        Assert.Equal("Status", binding.ActionSequence.Steps[0].Target);
        Assert.Equal("Ready", binding.ActionSequence.Steps[0].Value);
        Assert.Equal(DbActionKind.RunCommand, binding.ActionSequence.Steps[1].Kind);
        Assert.Equal("AuditChange", binding.ActionSequence.Steps[1].CommandName);
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

    [Fact]
    public void UpdateControlEventBindings_ReplacesActionSequences()
    {
        var state = new DesignerState();
        ControlDefinition button = new(
            "ship",
            "commandButton",
            new Rect(0, 0, 120, 32),
            null,
            PropertyBag.Empty,
            null);
        state.LoadForm(CreateForm() with { Controls = [button] });

        state.UpdateControlEventBindings(
            "ship",
            [
                new ControlEventBinding(
                    ControlEventKind.OnClick,
                    string.Empty,
                    ActionSequence: new DbActionSequence(
                    [
                        new DbActionStep(DbActionKind.ShowMessage, Message: "Queued."),
                    ],
                    Name: "NotifyClick")),
            ]);

        FormDefinition saved = state.ToFormDefinition();

        ControlEventBinding binding = Assert.Single(saved.Controls[0].EventBindings!);
        Assert.NotNull(binding.ActionSequence);
        Assert.Equal("NotifyClick", binding.ActionSequence!.Name);
        DbActionStep step = Assert.Single(binding.ActionSequence.Steps);
        Assert.Equal(DbActionKind.ShowMessage, step.Kind);
        Assert.Equal("Queued.", step.Message);
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
