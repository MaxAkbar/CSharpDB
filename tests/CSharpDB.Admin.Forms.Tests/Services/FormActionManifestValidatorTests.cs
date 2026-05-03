using CSharpDB.Admin.Forms.Contracts;
using CSharpDB.Admin.Forms.Models;
using CSharpDB.Admin.Forms.Services;
using CSharpDB.Primitives;

namespace CSharpDB.Admin.Forms.Tests.Services;

public sealed class FormActionManifestValidatorTests
{
    [Fact]
    public void Validate_ReturnsSuccessForSupportedPhase8Actions()
    {
        FormDefinition form = CreateForm([
            new DbActionStep(DbActionKind.OpenForm, Target: "orders-detail"),
            new DbActionStep(DbActionKind.ApplyFilter, Target: "ordersGrid", Value: "[Status] = 'Open'"),
            new DbActionStep(DbActionKind.ClearFilter, Target: "ordersGrid"),
            new DbActionStep(DbActionKind.RunSql, Value: "UPDATE Orders SET Status = @status"),
            new DbActionStep(DbActionKind.RunProcedure, Target: "RepriceOrder"),
            new DbActionStep(
                DbActionKind.SetControlProperty,
                Target: "ordersGrid",
                Value: false,
                Arguments: new Dictionary<string, object?> { ["property"] = "visible" }),
        ]);
        var capabilities = FormActionRuntimeCapabilities.RenderedForm with
        {
            RunSql = true,
            RunProcedure = true,
        };

        FormActionValidationResult result = FormActionManifestValidator.Validate(
            form,
            new FormActionValidationOptions(
                RuntimeCapabilities: capabilities,
                AvailableForms: ["orders-detail"],
                AvailableProcedures: ["RepriceOrder"]));

        Assert.True(result.Succeeded);
        Assert.Empty(result.Issues);
    }

    [Fact]
    public void Validate_ReportsActionReadinessIssues()
    {
        FormDefinition form = CreateForm(
            [
                new DbActionStep(DbActionKind.OpenForm, Target: "missing-form"),
                new DbActionStep(DbActionKind.ApplyFilter, Target: "missingGrid", Value: "[Status = 'Open'"),
                new DbActionStep(DbActionKind.RunSql),
                new DbActionStep(DbActionKind.RunProcedure, Target: "MissingProcedure"),
                new DbActionStep(DbActionKind.RunActionSequence, SequenceName: "MissingSequence"),
                new DbActionStep(DbActionKind.SetControlReadOnly, Target: "missingControl", Value: true),
            ],
            rules:
            [
                new ControlRuleDefinition(
                    "hide-internal",
                    "Status = 'Closed'",
                    [
                        new ControlRuleEffect("missingControl", "notAProperty", true),
                    ]),
            ]);

        FormActionValidationResult result = FormActionManifestValidator.Validate(
            form,
            new FormActionValidationOptions(
                RuntimeCapabilities: FormActionRuntimeCapabilities.None,
                AvailableForms: ["orders-detail"],
                AvailableProcedures: ["RepriceOrder"]));

        Assert.False(result.Succeeded);
        Assert.Contains(result.Issues, issue => issue.Severity == FormActionValidationSeverity.Warning && issue.ActionKind == DbActionKind.OpenForm);
        Assert.Contains(result.Issues, issue => issue.Message.Contains("OpenForm target 'missing-form'", StringComparison.Ordinal));
        Assert.Contains(result.Issues, issue => issue.Message.Contains("Unknown control 'missingGrid'", StringComparison.Ordinal));
        Assert.Contains(result.Issues, issue => issue.Message.Contains("malformed", StringComparison.Ordinal));
        Assert.Contains(result.Issues, issue => issue.Message.Contains("RunSql action requires SQL", StringComparison.Ordinal));
        Assert.Contains(result.Issues, issue => issue.Message.Contains("Procedure 'MissingProcedure'", StringComparison.Ordinal));
        Assert.Contains(result.Issues, issue => issue.Message.Contains("Unknown form action sequence 'MissingSequence'", StringComparison.Ordinal));
        Assert.Contains(result.Issues, issue => issue.Message.Contains("Control rule 'hide-internal' targets unknown control", StringComparison.Ordinal));
        Assert.Contains(result.Issues, issue => issue.Message.Contains("unsupported property 'notAProperty'", StringComparison.Ordinal));
    }

    [Fact]
    public void Validate_ReportsAmbiguousReusableSequences()
    {
        FormDefinition form = CreateForm(
            [new DbActionStep(DbActionKind.RunActionSequence, SequenceName: "Prepare")],
            actionSequences:
            [
                new DbActionSequence([], "Prepare"),
                new DbActionSequence([], "prepare"),
            ]);

        FormActionValidationResult result = FormActionManifestValidator.Validate(form);

        Assert.False(result.Succeeded);
        Assert.Contains(result.Issues, issue => issue.Message.Contains("ambiguous", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void Validate_ReportsUnknownFilterFieldsWhenSchemaIsAvailable()
    {
        FormDefinition form = CreateForm([
            new DbActionStep(DbActionKind.ApplyFilter, Target: "form", Value: "[MissingStatus] = 'Open'"),
        ]);
        var schema = new FormTableDefinition(
            "Orders",
            "orders:v1",
            [new FormFieldDefinition("Status", FieldDataType.String, IsNullable: false, IsReadOnly: false)],
            PrimaryKey: [],
            ForeignKeys: []);

        FormActionValidationResult result = FormActionManifestValidator.Validate(
            form,
            new FormActionValidationOptions(
                RuntimeCapabilities: FormActionRuntimeCapabilities.RenderedForm,
                Schema: schema));

        Assert.False(result.Succeeded);
        Assert.Contains(result.Issues, issue => issue.Message.Contains("unknown field 'MissingStatus'", StringComparison.OrdinalIgnoreCase));
    }

    private static FormDefinition CreateForm(
        IReadOnlyList<DbActionStep> steps,
        IReadOnlyList<DbActionSequence>? actionSequences = null,
        IReadOnlyList<ControlRuleDefinition>? rules = null)
        => new(
            "orders-form",
            "Orders Form",
            "Orders",
            DefinitionVersion: 1,
            SourceSchemaSignature: "orders:v1",
            Layout: new LayoutDefinition("absolute", 8, SnapToGrid: false, []),
            Controls:
            [
                new ControlDefinition(
                    "ordersGrid",
                    "childDataGrid",
                    new Rect(0, 0, 300, 120),
                    Binding: null,
                    Props: new PropertyBag(new Dictionary<string, object?>()),
                    ValidationOverride: null),
            ],
            EventBindings:
            [
                new FormEventBinding(
                    FormEventKind.OnLoad,
                    string.Empty,
                    ActionSequence: new DbActionSequence(steps, "LoadActions")),
            ],
            ActionSequences: actionSequences,
            Rules: rules);
}
