namespace CSharpDB.Admin.Forms.Contracts;

public sealed record FormActionRuntimeContext(
    string? FormId,
    string? FormName,
    string? TableName,
    string? EventName,
    string? ActionSequenceName,
    int StepIndex,
    IReadOnlyDictionary<string, object?>? Record,
    IReadOnlyDictionary<string, object?>? BindingArguments,
    IReadOnlyDictionary<string, object?>? RuntimeArguments,
    IReadOnlyDictionary<string, object?>? StepArguments,
    IReadOnlyDictionary<string, string> Metadata);
