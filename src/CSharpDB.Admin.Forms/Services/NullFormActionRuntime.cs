using CSharpDB.Admin.Forms.Contracts;
using CSharpDB.Primitives;

namespace CSharpDB.Admin.Forms.Services;

public sealed class NullFormActionRuntime : IFormActionRuntime
{
    public static NullFormActionRuntime Instance { get; } = new();

    public Task<FormEventDispatchResult> ExecuteRecordActionAsync(
        FormActionRuntimeContext context,
        DbActionStep step,
        CancellationToken ct)
        => UnsupportedAsync(step.Kind);

    public Task<FormEventDispatchResult> OpenFormAsync(
        FormActionRuntimeContext context,
        string formName,
        IReadOnlyDictionary<string, object?> arguments,
        CancellationToken ct)
        => UnsupportedAsync(DbActionKind.OpenForm);

    public Task<FormEventDispatchResult> CloseFormAsync(
        FormActionRuntimeContext context,
        string? formName,
        CancellationToken ct)
        => UnsupportedAsync(DbActionKind.CloseForm);

    public Task<FormEventDispatchResult> ApplyFilterAsync(
        FormActionRuntimeContext context,
        string target,
        string filter,
        IReadOnlyDictionary<string, object?> arguments,
        CancellationToken ct)
        => UnsupportedAsync(DbActionKind.ApplyFilter);

    public Task<FormEventDispatchResult> ClearFilterAsync(
        FormActionRuntimeContext context,
        string target,
        CancellationToken ct)
        => UnsupportedAsync(DbActionKind.ClearFilter);

    public Task<FormEventDispatchResult> RunSqlAsync(
        FormActionRuntimeContext context,
        string sqlOrName,
        IReadOnlyDictionary<string, object?> arguments,
        CancellationToken ct)
        => UnsupportedAsync(DbActionKind.RunSql);

    public Task<FormEventDispatchResult> RunProcedureAsync(
        FormActionRuntimeContext context,
        string procedureName,
        IReadOnlyDictionary<string, object?> arguments,
        CancellationToken ct)
        => UnsupportedAsync(DbActionKind.RunProcedure);

    public Task<FormEventDispatchResult> SetControlPropertyAsync(
        FormActionRuntimeContext context,
        string controlId,
        string propertyName,
        object? value,
        CancellationToken ct)
        => UnsupportedAsync(DbActionKind.SetControlProperty);

    private static Task<FormEventDispatchResult> UnsupportedAsync(DbActionKind actionKind)
        => Task.FromResult(FormEventDispatchResult.Failure(
            $"Form action '{actionKind}' requires a rendered form runtime."));
}
