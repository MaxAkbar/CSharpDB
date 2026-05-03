using CSharpDB.Primitives;
using CSharpDB.Admin.Forms.Models;

namespace CSharpDB.Admin.Forms.Contracts;

public sealed record FormActionValidationResult(
    bool Succeeded,
    IReadOnlyList<FormActionValidationIssue> Issues);

public sealed record FormActionValidationIssue(
    FormActionValidationSeverity Severity,
    DbActionKind ActionKind,
    string Surface,
    string Location,
    string Message,
    string? Target = null,
    string? EventName = null,
    string? ActionSequence = null,
    int? StepIndex = null);

public enum FormActionValidationSeverity
{
    Warning,
    Error,
}

public sealed record FormActionRuntimeCapabilities(
    bool RecordActions = false,
    bool OpenForm = false,
    bool CloseForm = false,
    bool ApplyFilter = false,
    bool ClearFilter = false,
    bool RunSql = false,
    bool RunProcedure = false,
    bool SetControlProperty = false)
{
    public static FormActionRuntimeCapabilities None { get; } = new();

    public static FormActionRuntimeCapabilities RenderedForm { get; } = new(
        RecordActions: true,
        OpenForm: true,
        CloseForm: true,
        ApplyFilter: true,
        ClearFilter: true,
        SetControlProperty: true);
}

public sealed record FormActionValidationOptions(
    FormActionRuntimeCapabilities? RuntimeCapabilities = null,
    IReadOnlyCollection<string>? AvailableForms = null,
    IReadOnlyCollection<string>? AvailableProcedures = null,
    FormTableDefinition? Schema = null);
