using CSharpDB.Admin.Forms.Contracts;
using CSharpDB.Admin.Forms.Models;
using CSharpDB.CodeModules.Runtime;
using CSharpDB.Primitives;

namespace CSharpDB.Admin.Forms.Services;

internal sealed class AdminFormCodeModuleCommandApi(
    FormDefinition form,
    DbCommandRegistry commands,
    DbExtensionPolicy callbackPolicy,
    IReadOnlyDictionary<string, object?>? record,
    IReadOnlyDictionary<string, object?>? bindingArguments,
    IReadOnlyDictionary<string, object?>? runtimeArguments,
    IReadOnlyDictionary<string, string> metadata,
    IReadOnlyList<DbActionSequence>? reusableSequences,
    IFormActionRuntime actionRuntime,
    Func<string, object?, Task>? setFieldValue = null,
    Func<string, Task>? showMessage = null,
    Func<DbActionStep, CancellationToken, Task<FormEventDispatchResult>>? executeBuiltInFormAction = null) : IFormCommandApi
{
    public async ValueTask SetFieldAsync(string fieldName, object? value, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(fieldName);
        ct.ThrowIfCancellationRequested();

        if (setFieldValue is not null)
        {
            await setFieldValue(fieldName, value);
            return;
        }

        if (record is not IDictionary<string, object?> mutable)
            throw new InvalidOperationException($"The current form record is read-only and field '{fieldName}' cannot be changed.");

        string key = mutable.Keys.FirstOrDefault(candidate => string.Equals(candidate, fieldName, StringComparison.OrdinalIgnoreCase))
            ?? throw new InvalidOperationException($"Unknown form field '{fieldName}'.");
        mutable[key] = value;
    }

    public async ValueTask<FormCommandApiResult> ShowMessageAsync(string message, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(message);
        ct.ThrowIfCancellationRequested();
        if (showMessage is not null)
            await showMessage(message);

        return FormCommandApiResult.Success(message);
    }

    public async ValueTask<FormCommandApiResult> RunActionSequenceAsync(
        string sequenceName,
        IReadOnlyDictionary<string, object?>? arguments = null,
        CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(sequenceName);
        FormEventDispatchResult result = await FormActionSequenceExecutor.ExecuteAsync(
            new DbActionSequence(
                [new DbActionStep(DbActionKind.RunActionSequence, SequenceName: sequenceName, Arguments: arguments)],
                Name: "__CodeModule"),
            commands,
            record,
            bindingArguments,
            runtimeArguments,
            metadata,
            reusableSequences,
            setFieldValue,
            showMessage,
            executeBuiltInFormAction,
            actionRuntime,
            callbackPolicy,
            ct);

        return ToApiResult(result);
    }

    public async ValueTask<DbCommandResult> RunHostCommandAsync(
        string commandName,
        IReadOnlyDictionary<string, object?>? arguments = null,
        CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(commandName);
        if (!commands.TryGetCommand(commandName, out DbCommandDefinition definition))
            return DbCommandResult.Failure($"Unknown form command '{commandName}'.");

        Dictionary<string, DbValue> commandArguments = DbCommandArguments.FromObjectDictionaries(record, runtimeArguments, bindingArguments, arguments);
        return await definition.InvokeAsync(
            commandArguments,
            metadata,
            callbackPolicy,
            DbExtensionHostMode.Embedded,
            ct);
    }

    public ValueTask<FormCommandApiResult> SaveRecordAsync(CancellationToken ct = default)
        => ExecuteRecordActionAsync(DbActionKind.SaveRecord, ct);

    public ValueTask<FormCommandApiResult> NewRecordAsync(CancellationToken ct = default)
        => ExecuteRecordActionAsync(DbActionKind.NewRecord, ct);

    public ValueTask<FormCommandApiResult> RefreshRecordsAsync(CancellationToken ct = default)
        => ExecuteRecordActionAsync(DbActionKind.RefreshRecords, ct);

    public async ValueTask<FormCommandApiResult> OpenFormAsync(
        string formName,
        IReadOnlyDictionary<string, object?>? arguments = null,
        CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(formName);
        FormEventDispatchResult result = await actionRuntime.OpenFormAsync(
            BuildRuntimeContext(null, arguments),
            formName,
            arguments ?? EmptyObjectDictionary.Instance,
            ct);
        return ToApiResult(result);
    }

    public async ValueTask<FormCommandApiResult> CloseFormAsync(string? formName = null, CancellationToken ct = default)
    {
        FormEventDispatchResult result = await actionRuntime.CloseFormAsync(
            BuildRuntimeContext(null, null),
            formName,
            ct);
        return ToApiResult(result);
    }

    public async ValueTask<FormCommandApiResult> ApplyFilterAsync(
        string filter,
        string target = "form",
        IReadOnlyDictionary<string, object?>? arguments = null,
        CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(filter);
        string resolvedTarget = string.IsNullOrWhiteSpace(target) ? "form" : target;
        FormEventDispatchResult result = await actionRuntime.ApplyFilterAsync(
            BuildRuntimeContext(null, arguments),
            resolvedTarget,
            filter,
            arguments ?? EmptyObjectDictionary.Instance,
            ct);
        return ToApiResult(result);
    }

    public async ValueTask<FormCommandApiResult> ClearFilterAsync(string target = "form", CancellationToken ct = default)
    {
        string resolvedTarget = string.IsNullOrWhiteSpace(target) ? "form" : target;
        FormEventDispatchResult result = await actionRuntime.ClearFilterAsync(
            BuildRuntimeContext(null, null),
            resolvedTarget,
            ct);
        return ToApiResult(result);
    }

    private async ValueTask<FormCommandApiResult> ExecuteRecordActionAsync(DbActionKind kind, CancellationToken ct)
    {
        var step = new DbActionStep(kind);
        FormEventDispatchResult result = executeBuiltInFormAction is not null && actionRuntime is NullFormActionRuntime
            ? await executeBuiltInFormAction(step, ct)
            : await actionRuntime.ExecuteRecordActionAsync(BuildRuntimeContext(step, null), step, ct);

        return ToApiResult(result);
    }

    private FormActionRuntimeContext BuildRuntimeContext(
        DbActionStep? step,
        IReadOnlyDictionary<string, object?>? stepArguments)
        => new(
            form.FormId,
            form.Name,
            form.TableName,
            metadata.TryGetValue("event", out string? eventName) ? eventName : null,
            "__CodeModule",
            0,
            record,
            bindingArguments,
            runtimeArguments,
            stepArguments,
            metadata);

    private static FormCommandApiResult ToApiResult(FormEventDispatchResult result)
        => result.Succeeded
            ? FormCommandApiResult.Success(result.Message)
            : FormCommandApiResult.Failure(result.Message ?? "The form command failed.");

    private static class EmptyObjectDictionary
    {
        public static readonly IReadOnlyDictionary<string, object?> Instance =
            new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
    }
}
