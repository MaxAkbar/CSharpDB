using CSharpDB.Primitives;

namespace CSharpDB.Admin.Forms.Contracts;

public interface IFormActionRuntime
{
    Task<FormEventDispatchResult> ExecuteRecordActionAsync(
        FormActionRuntimeContext context,
        DbActionStep step,
        CancellationToken ct);

    Task<FormEventDispatchResult> OpenFormAsync(
        FormActionRuntimeContext context,
        string formName,
        IReadOnlyDictionary<string, object?> arguments,
        CancellationToken ct);

    Task<FormEventDispatchResult> CloseFormAsync(
        FormActionRuntimeContext context,
        string? formName,
        CancellationToken ct);

    Task<FormEventDispatchResult> ApplyFilterAsync(
        FormActionRuntimeContext context,
        string target,
        string filter,
        IReadOnlyDictionary<string, object?> arguments,
        CancellationToken ct);

    Task<FormEventDispatchResult> ClearFilterAsync(
        FormActionRuntimeContext context,
        string target,
        CancellationToken ct);

    Task<FormEventDispatchResult> RunSqlAsync(
        FormActionRuntimeContext context,
        string sqlOrName,
        IReadOnlyDictionary<string, object?> arguments,
        CancellationToken ct);

    Task<FormEventDispatchResult> RunProcedureAsync(
        FormActionRuntimeContext context,
        string procedureName,
        IReadOnlyDictionary<string, object?> arguments,
        CancellationToken ct);

    Task<FormEventDispatchResult> SetControlPropertyAsync(
        FormActionRuntimeContext context,
        string controlId,
        string propertyName,
        object? value,
        CancellationToken ct);
}
