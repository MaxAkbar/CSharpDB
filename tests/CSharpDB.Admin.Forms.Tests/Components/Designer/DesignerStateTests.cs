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
    public void ToFormDefinition_PreservesReusableActionSequences()
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
                        new DbActionStep(DbActionKind.RunActionSequence, SequenceName: "PrepareRecord"),
                    ])),
            ],
            ActionSequences =
            [
                new DbActionSequence(
                [
                    new DbActionStep(DbActionKind.SetFieldValue, Target: "Status", Value: "Ready"),
                    new DbActionStep(DbActionKind.RunCommand, CommandName: "AuditPrepared"),
                ],
                Name: "PrepareRecord"),
            ],
        };

        state.LoadForm(form);

        FormDefinition saved = state.ToFormDefinition();

        DbActionSequence reusable = Assert.Single(saved.ActionSequences!);
        Assert.Equal("PrepareRecord", reusable.Name);
        Assert.Equal(DbActionKind.SetFieldValue, reusable.Steps[0].Kind);
        Assert.Equal("Status", reusable.Steps[0].Target);
        Assert.Equal("Ready", reusable.Steps[0].Value);
        Assert.Equal(DbActionKind.RunCommand, reusable.Steps[1].Kind);
        Assert.Equal("AuditPrepared", reusable.Steps[1].CommandName);

        FormEventBinding binding = Assert.Single(saved.EventBindings!);
        Assert.Equal(DbActionKind.RunActionSequence, binding.ActionSequence!.Steps[0].Kind);
        Assert.Equal("PrepareRecord", binding.ActionSequence.Steps[0].SequenceName);
    }

    [Fact]
    public void UpdateActionSequences_ReplacesReusableActionSequences()
    {
        var state = new DesignerState();
        state.LoadForm(CreateForm());

        state.UpdateActionSequences(
        [
            new DbActionSequence(
            [
                new DbActionStep(DbActionKind.ShowMessage, Message: "Ready."),
            ],
            Name: "NotifyReady"),
        ]);

        FormDefinition saved = state.ToFormDefinition();

        DbActionSequence sequence = Assert.Single(saved.ActionSequences!);
        Assert.Equal("NotifyReady", sequence.Name);
        DbActionStep step = Assert.Single(sequence.Steps);
        Assert.Equal(DbActionKind.ShowMessage, step.Kind);
        Assert.Equal("Ready.", step.Message);
    }

    [Fact]
    public void ToFormDefinition_PreservesControlRules()
    {
        var state = new DesignerState();
        var form = CreateForm() with
        {
            Rules =
            [
                new ControlRuleDefinition(
                    "closed-state",
                    "[Status] = 'Closed'",
                    [new ControlRuleEffect("status", "visible", false)]),
            ],
        };

        state.LoadForm(form);

        FormDefinition saved = state.ToFormDefinition();

        ControlRuleDefinition rule = Assert.Single(saved.Rules!);
        Assert.Equal("closed-state", rule.RuleId);
        Assert.Equal("[Status] = 'Closed'", rule.Condition);
        ControlRuleEffect effect = Assert.Single(rule.Effects);
        Assert.Equal("status", effect.ControlId);
        Assert.Equal("visible", effect.Property);
        Assert.False(Assert.IsType<bool>(effect.Value));
    }

    [Fact]
    public void UpdateRules_ReplacesControlRules()
    {
        var state = new DesignerState();
        state.LoadForm(CreateForm());

        state.UpdateRules(
        [
            new ControlRuleDefinition(
                "readonly-closed",
                "[Status] = 'Closed'",
                [new ControlRuleEffect("status", "readOnly", true)]),
        ]);

        FormDefinition saved = state.ToFormDefinition();

        ControlRuleDefinition rule = Assert.Single(saved.Rules!);
        Assert.Equal("readonly-closed", rule.RuleId);
        Assert.Equal("readOnly", Assert.Single(rule.Effects).Property);
    }

    [Fact]
    public void SetLayoutMode_UpdatesSavedLayout()
    {
        var state = new DesignerState();
        state.LoadForm(CreateForm());

        state.SetLayoutMode("elastic");

        FormDefinition saved = state.ToFormDefinition();
        Assert.Equal("elastic", saved.Layout.LayoutMode);
    }

    [Fact]
    public void SetFormName_TrimsAndPersistsName()
    {
        var state = new DesignerState();
        state.LoadForm(CreateForm());

        state.SetFormName("  Customer Entry  ");

        FormDefinition saved = state.ToFormDefinition();
        Assert.Equal("Customer Entry", saved.Name);
    }

    [Fact]
    public void SetFormName_BlankNameFallsBackToUntitled()
    {
        var state = new DesignerState();
        state.LoadForm(CreateForm());

        state.SetFormName("   ");

        FormDefinition saved = state.ToFormDefinition();
        Assert.Equal("Untitled Form", saved.Name);
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

    [Fact]
    public void UpdateControlProps_UpdatesMultiplePropertiesWithOneUndoSnapshot()
    {
        var state = new DesignerState();
        ControlDefinition text = new(
            "status",
            "text",
            new Rect(0, 0, 160, 32),
            new BindingDefinition("Status", "TwoWay"),
            PropertyBag.Empty,
            null);
        state.LoadForm(CreateForm() with { Controls = [text] });

        state.UpdateControlProps(
            "status",
            new Dictionary<string, object?>
            {
                ["anchorLeft"] = true,
                ["anchorRight"] = true,
                ["minWidth"] = 120L,
            });

        ControlDefinition updated = Assert.Single(state.ToFormDefinition().Controls);
        Assert.Equal(true, updated.Props.Values["anchorLeft"]);
        Assert.Equal(true, updated.Props.Values["anchorRight"]);
        Assert.Equal(120L, updated.Props.Values["minWidth"]);

        state.Undo();

        ControlDefinition reverted = Assert.Single(state.ToFormDefinition().Controls);
        Assert.Empty(reverted.Props.Values);
    }

    [Fact]
    public void DeleteSelected_RemovesTabChildren()
    {
        var state = new DesignerState();
        state.LoadForm(CreateForm() with
        {
            Controls =
            [
                CreateTabControl("tabs"),
                CreateTabChild("child", "tabs", "main"),
            ],
        });

        state.SelectControl("tabs", addToSelection: false);
        state.DeleteSelected();

        Assert.Empty(state.ToFormDefinition().Controls);
    }

    [Fact]
    public void CopyPaste_RemapsCopiedTabChildrenToCopiedParent()
    {
        var state = new DesignerState();
        state.LoadForm(CreateForm() with
        {
            Controls =
            [
                CreateTabControl("tabs"),
                CreateTabChild("child", "tabs", "main"),
            ],
        });

        state.SelectControl("tabs", addToSelection: false);
        state.CopySelected();
        state.PasteClipboard();

        FormDefinition saved = state.ToFormDefinition();
        Assert.Equal(4, saved.Controls.Count);
        ControlDefinition pastedParent = Assert.Single(saved.Controls, control => control.ControlType == "tabControl" && control.ControlId != "tabs");
        ControlDefinition pastedChild = Assert.Single(saved.Controls, control => control.ControlId is not "child" && control.ControlType == "text");
        Assert.Equal(pastedParent.ControlId, pastedChild.Props.Values["parentControlId"]);
        Assert.Equal("main", pastedChild.Props.Values["parentTabId"]);
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

    private static ControlDefinition CreateTabControl(string controlId)
        => new(
            controlId,
            "tabControl",
            new Rect(0, 0, 400, 240),
            null,
            new PropertyBag(new Dictionary<string, object?>
            {
                ["tabs"] = new object?[]
                {
                    new Dictionary<string, object?> { ["id"] = "main", ["label"] = "Main" },
                },
            }),
            null);

    private static ControlDefinition CreateTabChild(string controlId, string parentControlId, string parentTabId)
        => new(
            controlId,
            "text",
            new Rect(16, 48, 160, 32),
            new BindingDefinition("Name", "TwoWay"),
            new PropertyBag(new Dictionary<string, object?>
            {
                ["parentControlId"] = parentControlId,
                ["parentTabId"] = parentTabId,
            }),
            null);
}
