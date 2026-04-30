using System.Globalization;
using System.Text.Json;
using CSharpDB.Admin.Forms.Contracts;
using CSharpDB.Primitives;

namespace CSharpDB.Admin.Forms.Services;

internal static class FormActionSequenceExecutor
{
    private const int MaxNestedActionSequenceDepth = 8;

    public static async Task<FormEventDispatchResult> ExecuteAsync(
        DbActionSequence sequence,
        DbCommandRegistry commands,
        IReadOnlyDictionary<string, object?>? record,
        IReadOnlyDictionary<string, object?>? bindingArguments,
        IReadOnlyDictionary<string, object?>? runtimeArguments,
        IReadOnlyDictionary<string, string> metadata,
        IReadOnlyList<DbActionSequence>? reusableSequences = null,
        Func<string, object?, Task>? setFieldValue = null,
        Func<string, Task>? showMessage = null,
        Func<DbActionStep, CancellationToken, Task<FormEventDispatchResult>>? executeBuiltInFormAction = null,
        IFormActionRuntime? actionRuntime = null,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(sequence);
        ArgumentNullException.ThrowIfNull(commands);
        ArgumentNullException.ThrowIfNull(metadata);

        return await ExecuteCoreAsync(
            sequence,
            commands,
            record,
            bindingArguments,
            runtimeArguments,
            metadata,
            reusableSequences,
            setFieldValue,
            showMessage,
            executeBuiltInFormAction,
            actionRuntime ?? NullFormActionRuntime.Instance,
            ct,
            depth: 0);
    }

    private static async Task<FormEventDispatchResult> ExecuteCoreAsync(
        DbActionSequence sequence,
        DbCommandRegistry commands,
        IReadOnlyDictionary<string, object?>? record,
        IReadOnlyDictionary<string, object?>? bindingArguments,
        IReadOnlyDictionary<string, object?>? runtimeArguments,
        IReadOnlyDictionary<string, string> metadata,
        IReadOnlyList<DbActionSequence>? reusableSequences,
        Func<string, object?, Task>? setFieldValue,
        Func<string, Task>? showMessage,
        Func<DbActionStep, CancellationToken, Task<FormEventDispatchResult>>? executeBuiltInFormAction,
        IFormActionRuntime actionRuntime,
        CancellationToken ct,
        int depth)
    {
        IReadOnlyList<DbActionStep> steps = sequence.Steps ?? [];
        string? lastMessage = null;
        for (int i = 0; i < steps.Count; i++)
        {
            DbActionStep step = steps[i];
            FormEventDispatchResult result = await ExecuteStepAsync(
                sequence,
                step,
                i,
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
                ct,
                depth);

            if (!result.Succeeded && step.StopOnFailure)
                return result;

            if (result.Succeeded && !string.IsNullOrWhiteSpace(result.Message))
                lastMessage = result.Message;

            if (step.Kind == DbActionKind.Stop)
                return result;
        }

        return FormEventDispatchResult.Success(lastMessage);
    }

    private static async Task<FormEventDispatchResult> ExecuteStepAsync(
        DbActionSequence sequence,
        DbActionStep step,
        int stepIndex,
        DbCommandRegistry commands,
        IReadOnlyDictionary<string, object?>? record,
        IReadOnlyDictionary<string, object?>? bindingArguments,
        IReadOnlyDictionary<string, object?>? runtimeArguments,
        IReadOnlyDictionary<string, string> metadata,
        IReadOnlyList<DbActionSequence>? reusableSequences,
        Func<string, object?, Task>? setFieldValue,
        Func<string, Task>? showMessage,
        Func<DbActionStep, CancellationToken, Task<FormEventDispatchResult>>? executeBuiltInFormAction,
        IFormActionRuntime actionRuntime,
        CancellationToken ct,
        int depth)
    {
        try
        {
            if (!FormActionConditionEvaluator.TryEvaluate(
                step.Condition,
                record,
                bindingArguments,
                runtimeArguments,
                step.Arguments,
                out bool shouldRun,
                out string? conditionError))
            {
                return FormEventDispatchResult.Failure(
                    $"Form action '{step.Kind}' condition failed: {conditionError}");
            }

            if (!shouldRun)
                return FormEventDispatchResult.Success();

            return step.Kind switch
            {
                DbActionKind.RunCommand => await RunCommandAsync(sequence, step, stepIndex, commands, record, bindingArguments, runtimeArguments, metadata, ct),
                DbActionKind.SetFieldValue => await SetFieldValueAsync(step, record, setFieldValue),
                DbActionKind.ShowMessage => await ShowMessageAsync(step, showMessage),
                DbActionKind.Stop => FormEventDispatchResult.Success(ReadMessage(step)),
                DbActionKind.RunActionSequence => await RunActionSequenceAsync(
                    step,
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
                    ct,
                    depth),
                DbActionKind.OpenForm => await OpenFormAsync(sequence, step, stepIndex, record, bindingArguments, runtimeArguments, metadata, actionRuntime, executeBuiltInFormAction, ct),
                DbActionKind.CloseForm => await CloseFormAsync(sequence, step, stepIndex, record, bindingArguments, runtimeArguments, metadata, actionRuntime, executeBuiltInFormAction, ct),
                DbActionKind.ApplyFilter => await ApplyFilterAsync(sequence, step, stepIndex, record, bindingArguments, runtimeArguments, metadata, actionRuntime, executeBuiltInFormAction, ct),
                DbActionKind.ClearFilter => await ClearFilterAsync(sequence, step, stepIndex, record, bindingArguments, runtimeArguments, metadata, actionRuntime, executeBuiltInFormAction, ct),
                DbActionKind.RunSql => await RunSqlAsync(sequence, step, stepIndex, record, bindingArguments, runtimeArguments, metadata, actionRuntime, executeBuiltInFormAction, ct),
                DbActionKind.RunProcedure => await RunProcedureAsync(sequence, step, stepIndex, record, bindingArguments, runtimeArguments, metadata, actionRuntime, executeBuiltInFormAction, ct),
                DbActionKind.SetControlProperty => await SetControlPropertyAsync(sequence, step, stepIndex, record, bindingArguments, runtimeArguments, metadata, actionRuntime, executeBuiltInFormAction, ct),
                DbActionKind.SetControlVisibility => await SetSpecificControlPropertyAsync(sequence, step, stepIndex, "visible", record, bindingArguments, runtimeArguments, metadata, actionRuntime, executeBuiltInFormAction, ct),
                DbActionKind.SetControlEnabled => await SetSpecificControlPropertyAsync(sequence, step, stepIndex, "enabled", record, bindingArguments, runtimeArguments, metadata, actionRuntime, executeBuiltInFormAction, ct),
                DbActionKind.SetControlReadOnly => await SetSpecificControlPropertyAsync(sequence, step, stepIndex, "readOnly", record, bindingArguments, runtimeArguments, metadata, actionRuntime, executeBuiltInFormAction, ct),
                DbActionKind.NewRecord or
                DbActionKind.SaveRecord or
                DbActionKind.DeleteRecord or
                DbActionKind.RefreshRecords or
                DbActionKind.PreviousRecord or
                DbActionKind.NextRecord or
                DbActionKind.GoToRecord => await ExecuteRecordActionAsync(sequence, step, stepIndex, record, bindingArguments, runtimeArguments, metadata, actionRuntime, executeBuiltInFormAction, ct),
                _ => FormEventDispatchResult.Failure($"Unsupported form action kind '{step.Kind}'."),
            };
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            return FormEventDispatchResult.Failure(
                $"Form action '{step.Kind}' failed: {ex.Message}");
        }
    }

    private static Task<FormEventDispatchResult> ExecuteRecordActionAsync(
        DbActionSequence sequence,
        DbActionStep step,
        int stepIndex,
        IReadOnlyDictionary<string, object?>? record,
        IReadOnlyDictionary<string, object?>? bindingArguments,
        IReadOnlyDictionary<string, object?>? runtimeArguments,
        IReadOnlyDictionary<string, string> metadata,
        IFormActionRuntime actionRuntime,
        Func<DbActionStep, CancellationToken, Task<FormEventDispatchResult>>? executeBuiltInFormAction,
        CancellationToken ct)
    {
        if (executeBuiltInFormAction is not null && actionRuntime is NullFormActionRuntime)
            return executeBuiltInFormAction(step, ct);

        return actionRuntime.ExecuteRecordActionAsync(
            BuildRuntimeContext(sequence, step, stepIndex, record, bindingArguments, runtimeArguments, metadata),
            step,
            ct);
    }

    private static Task<FormEventDispatchResult> OpenFormAsync(
        DbActionSequence sequence,
        DbActionStep step,
        int stepIndex,
        IReadOnlyDictionary<string, object?>? record,
        IReadOnlyDictionary<string, object?>? bindingArguments,
        IReadOnlyDictionary<string, object?>? runtimeArguments,
        IReadOnlyDictionary<string, string> metadata,
        IFormActionRuntime actionRuntime,
        Func<DbActionStep, CancellationToken, Task<FormEventDispatchResult>>? executeBuiltInFormAction,
        CancellationToken ct)
    {
        if (executeBuiltInFormAction is not null && actionRuntime is NullFormActionRuntime)
            return executeBuiltInFormAction(step, ct);

        IReadOnlyDictionary<string, object?> arguments = NormalizeArguments(step.Arguments);
        string? formName = ReadText(step.Target)
            ?? ReadText(step.Value)
            ?? ReadArgumentText(arguments, "formName", "form", "name");
        if (string.IsNullOrWhiteSpace(formName))
            return Task.FromResult(FormEventDispatchResult.Failure("OpenForm action requires a target form name."));

        return actionRuntime.OpenFormAsync(
            BuildRuntimeContext(sequence, step, stepIndex, record, bindingArguments, runtimeArguments, metadata),
            formName,
            arguments,
            ct);
    }

    private static Task<FormEventDispatchResult> CloseFormAsync(
        DbActionSequence sequence,
        DbActionStep step,
        int stepIndex,
        IReadOnlyDictionary<string, object?>? record,
        IReadOnlyDictionary<string, object?>? bindingArguments,
        IReadOnlyDictionary<string, object?>? runtimeArguments,
        IReadOnlyDictionary<string, string> metadata,
        IFormActionRuntime actionRuntime,
        Func<DbActionStep, CancellationToken, Task<FormEventDispatchResult>>? executeBuiltInFormAction,
        CancellationToken ct)
    {
        if (executeBuiltInFormAction is not null && actionRuntime is NullFormActionRuntime)
            return executeBuiltInFormAction(step, ct);

        IReadOnlyDictionary<string, object?> arguments = NormalizeArguments(step.Arguments);
        string? formName = ReadText(step.Target)
            ?? ReadText(step.Value)
            ?? ReadArgumentText(arguments, "formName", "form", "name");

        return actionRuntime.CloseFormAsync(
            BuildRuntimeContext(sequence, step, stepIndex, record, bindingArguments, runtimeArguments, metadata),
            formName,
            ct);
    }

    private static Task<FormEventDispatchResult> ApplyFilterAsync(
        DbActionSequence sequence,
        DbActionStep step,
        int stepIndex,
        IReadOnlyDictionary<string, object?>? record,
        IReadOnlyDictionary<string, object?>? bindingArguments,
        IReadOnlyDictionary<string, object?>? runtimeArguments,
        IReadOnlyDictionary<string, string> metadata,
        IFormActionRuntime actionRuntime,
        Func<DbActionStep, CancellationToken, Task<FormEventDispatchResult>>? executeBuiltInFormAction,
        CancellationToken ct)
    {
        if (executeBuiltInFormAction is not null && actionRuntime is NullFormActionRuntime)
            return executeBuiltInFormAction(step, ct);

        IReadOnlyDictionary<string, object?> arguments = NormalizeArguments(step.Arguments);
        string target = ReadText(step.Target)
            ?? ReadArgumentText(arguments, "target")
            ?? "form";
        string? filter = ReadText(step.Value)
            ?? ReadArgumentText(arguments, "filter", "where");
        if (string.IsNullOrWhiteSpace(filter))
            return Task.FromResult(FormEventDispatchResult.Failure("ApplyFilter action requires a filter expression."));

        return actionRuntime.ApplyFilterAsync(
            BuildRuntimeContext(sequence, step, stepIndex, record, bindingArguments, runtimeArguments, metadata),
            target,
            filter,
            arguments,
            ct);
    }

    private static Task<FormEventDispatchResult> ClearFilterAsync(
        DbActionSequence sequence,
        DbActionStep step,
        int stepIndex,
        IReadOnlyDictionary<string, object?>? record,
        IReadOnlyDictionary<string, object?>? bindingArguments,
        IReadOnlyDictionary<string, object?>? runtimeArguments,
        IReadOnlyDictionary<string, string> metadata,
        IFormActionRuntime actionRuntime,
        Func<DbActionStep, CancellationToken, Task<FormEventDispatchResult>>? executeBuiltInFormAction,
        CancellationToken ct)
    {
        if (executeBuiltInFormAction is not null && actionRuntime is NullFormActionRuntime)
            return executeBuiltInFormAction(step, ct);

        IReadOnlyDictionary<string, object?> arguments = NormalizeArguments(step.Arguments);
        string target = ReadText(step.Target)
            ?? ReadArgumentText(arguments, "target")
            ?? "form";

        return actionRuntime.ClearFilterAsync(
            BuildRuntimeContext(sequence, step, stepIndex, record, bindingArguments, runtimeArguments, metadata),
            target,
            ct);
    }

    private static Task<FormEventDispatchResult> RunSqlAsync(
        DbActionSequence sequence,
        DbActionStep step,
        int stepIndex,
        IReadOnlyDictionary<string, object?>? record,
        IReadOnlyDictionary<string, object?>? bindingArguments,
        IReadOnlyDictionary<string, object?>? runtimeArguments,
        IReadOnlyDictionary<string, string> metadata,
        IFormActionRuntime actionRuntime,
        Func<DbActionStep, CancellationToken, Task<FormEventDispatchResult>>? executeBuiltInFormAction,
        CancellationToken ct)
    {
        if (executeBuiltInFormAction is not null && actionRuntime is NullFormActionRuntime)
            return executeBuiltInFormAction(step, ct);

        IReadOnlyDictionary<string, object?> arguments = NormalizeArguments(step.Arguments);
        string? sqlOrName = ReadText(step.Value)
            ?? ReadText(step.Target)
            ?? ReadArgumentText(arguments, "sql", "name");
        if (string.IsNullOrWhiteSpace(sqlOrName))
            return Task.FromResult(FormEventDispatchResult.Failure("RunSql action requires SQL text or a named SQL operation."));

        return actionRuntime.RunSqlAsync(
            BuildRuntimeContext(sequence, step, stepIndex, record, bindingArguments, runtimeArguments, metadata),
            sqlOrName,
            arguments,
            ct);
    }

    private static Task<FormEventDispatchResult> RunProcedureAsync(
        DbActionSequence sequence,
        DbActionStep step,
        int stepIndex,
        IReadOnlyDictionary<string, object?>? record,
        IReadOnlyDictionary<string, object?>? bindingArguments,
        IReadOnlyDictionary<string, object?>? runtimeArguments,
        IReadOnlyDictionary<string, string> metadata,
        IFormActionRuntime actionRuntime,
        Func<DbActionStep, CancellationToken, Task<FormEventDispatchResult>>? executeBuiltInFormAction,
        CancellationToken ct)
    {
        if (executeBuiltInFormAction is not null && actionRuntime is NullFormActionRuntime)
            return executeBuiltInFormAction(step, ct);

        IReadOnlyDictionary<string, object?> arguments = NormalizeArguments(step.Arguments);
        string? procedureName = ReadText(step.Target)
            ?? ReadText(step.Value)
            ?? ReadArgumentText(arguments, "procedureName", "procedure", "name");
        if (string.IsNullOrWhiteSpace(procedureName))
            return Task.FromResult(FormEventDispatchResult.Failure("RunProcedure action requires a procedure name."));

        return actionRuntime.RunProcedureAsync(
            BuildRuntimeContext(sequence, step, stepIndex, record, bindingArguments, runtimeArguments, metadata),
            procedureName,
            arguments,
            ct);
    }

    private static Task<FormEventDispatchResult> SetControlPropertyAsync(
        DbActionSequence sequence,
        DbActionStep step,
        int stepIndex,
        IReadOnlyDictionary<string, object?>? record,
        IReadOnlyDictionary<string, object?>? bindingArguments,
        IReadOnlyDictionary<string, object?>? runtimeArguments,
        IReadOnlyDictionary<string, string> metadata,
        IFormActionRuntime actionRuntime,
        Func<DbActionStep, CancellationToken, Task<FormEventDispatchResult>>? executeBuiltInFormAction,
        CancellationToken ct)
    {
        IReadOnlyDictionary<string, object?> arguments = NormalizeArguments(step.Arguments);
        string? propertyName = ReadArgumentText(arguments, "property", "propertyName");
        return SetControlPropertyCoreAsync(
            sequence,
            step,
            stepIndex,
            propertyName,
            ReadValue(step, arguments),
            record,
            bindingArguments,
            runtimeArguments,
            metadata,
            actionRuntime,
            executeBuiltInFormAction,
            ct);
    }

    private static Task<FormEventDispatchResult> SetSpecificControlPropertyAsync(
        DbActionSequence sequence,
        DbActionStep step,
        int stepIndex,
        string propertyName,
        IReadOnlyDictionary<string, object?>? record,
        IReadOnlyDictionary<string, object?>? bindingArguments,
        IReadOnlyDictionary<string, object?>? runtimeArguments,
        IReadOnlyDictionary<string, string> metadata,
        IFormActionRuntime actionRuntime,
        Func<DbActionStep, CancellationToken, Task<FormEventDispatchResult>>? executeBuiltInFormAction,
        CancellationToken ct)
    {
        IReadOnlyDictionary<string, object?> arguments = NormalizeArguments(step.Arguments);
        return SetControlPropertyCoreAsync(
            sequence,
            step,
            stepIndex,
            propertyName,
            ReadValue(step, arguments),
            record,
            bindingArguments,
            runtimeArguments,
            metadata,
            actionRuntime,
            executeBuiltInFormAction,
            ct);
    }

    private static Task<FormEventDispatchResult> SetControlPropertyCoreAsync(
        DbActionSequence sequence,
        DbActionStep step,
        int stepIndex,
        string? propertyName,
        object? value,
        IReadOnlyDictionary<string, object?>? record,
        IReadOnlyDictionary<string, object?>? bindingArguments,
        IReadOnlyDictionary<string, object?>? runtimeArguments,
        IReadOnlyDictionary<string, string> metadata,
        IFormActionRuntime actionRuntime,
        Func<DbActionStep, CancellationToken, Task<FormEventDispatchResult>>? executeBuiltInFormAction,
        CancellationToken ct)
    {
        if (executeBuiltInFormAction is not null && actionRuntime is NullFormActionRuntime)
            return executeBuiltInFormAction(step, ct);

        string? controlId = ReadText(step.Target);
        if (string.IsNullOrWhiteSpace(controlId))
            return Task.FromResult(FormEventDispatchResult.Failure($"{step.Kind} action requires a target control id."));

        if (string.IsNullOrWhiteSpace(propertyName))
            return Task.FromResult(FormEventDispatchResult.Failure("SetControlProperty action requires a property name."));

        return actionRuntime.SetControlPropertyAsync(
            BuildRuntimeContext(sequence, step, stepIndex, record, bindingArguments, runtimeArguments, metadata),
            controlId,
            propertyName,
            value,
            ct);
    }

    private static async Task<FormEventDispatchResult> RunCommandAsync(
        DbActionSequence sequence,
        DbActionStep step,
        int stepIndex,
        DbCommandRegistry commands,
        IReadOnlyDictionary<string, object?>? record,
        IReadOnlyDictionary<string, object?>? bindingArguments,
        IReadOnlyDictionary<string, object?>? runtimeArguments,
        IReadOnlyDictionary<string, string> metadata,
        CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(step.CommandName))
            return FormEventDispatchResult.Failure("RunCommand action requires a command name.");

        if (!commands.TryGetCommand(step.CommandName, out DbCommandDefinition definition))
            return FormEventDispatchResult.Failure($"Unknown form command '{step.CommandName}' for action sequence.");

        Dictionary<string, DbValue> arguments = DbCommandArguments.FromObjectDictionaries(
            record,
            bindingArguments,
            runtimeArguments,
            step.Arguments);
        Dictionary<string, string> stepMetadata = BuildStepMetadata(sequence, step, stepIndex, metadata);

        try
        {
            DbCommandResult result = await definition.InvokeAsync(arguments, stepMetadata, ct);
            if (result.Succeeded)
                return FormEventDispatchResult.Success(result.Message);

            string message = string.IsNullOrWhiteSpace(result.Message)
                ? $"Action command '{definition.Name}' failed."
                : result.Message;
            return FormEventDispatchResult.Failure(message);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            return FormEventDispatchResult.Failure(
                $"Action command '{definition.Name}' failed: {ex.Message}");
        }
    }

    private static async Task<FormEventDispatchResult> RunActionSequenceAsync(
        DbActionStep step,
        DbCommandRegistry commands,
        IReadOnlyDictionary<string, object?>? record,
        IReadOnlyDictionary<string, object?>? bindingArguments,
        IReadOnlyDictionary<string, object?>? runtimeArguments,
        IReadOnlyDictionary<string, string> metadata,
        IReadOnlyList<DbActionSequence>? reusableSequences,
        Func<string, object?, Task>? setFieldValue,
        Func<string, Task>? showMessage,
        Func<DbActionStep, CancellationToken, Task<FormEventDispatchResult>>? executeBuiltInFormAction,
        IFormActionRuntime actionRuntime,
        CancellationToken ct,
        int depth)
    {
        string? sequenceName = ReadSequenceName(step);
        if (string.IsNullOrWhiteSpace(sequenceName))
            return FormEventDispatchResult.Failure("RunActionSequence action requires a sequence name.");

        if (depth >= MaxNestedActionSequenceDepth)
            return FormEventDispatchResult.Failure(
                $"Action sequence nesting limit exceeded while running '{sequenceName}'.");

        IReadOnlyList<DbActionSequence> matches = (reusableSequences ?? [])
            .Where(sequence => string.Equals(sequence.Name, sequenceName, StringComparison.OrdinalIgnoreCase))
            .Take(2)
            .ToList();

        if (matches.Count == 0)
            return FormEventDispatchResult.Failure($"Unknown form action sequence '{sequenceName}'.");

        if (matches.Count > 1)
            return FormEventDispatchResult.Failure($"Form action sequence name '{sequenceName}' is ambiguous.");

        IReadOnlyDictionary<string, object?>? nestedRuntimeArguments =
            MergeRuntimeArguments(runtimeArguments, step.Arguments);

        return await ExecuteCoreAsync(
            matches[0],
            commands,
            record,
            bindingArguments,
            nestedRuntimeArguments,
            metadata,
            reusableSequences,
            setFieldValue,
            showMessage,
            executeBuiltInFormAction,
            actionRuntime,
            ct,
            depth + 1);
    }

    private static async Task<FormEventDispatchResult> SetFieldValueAsync(
        DbActionStep step,
        IReadOnlyDictionary<string, object?>? record,
        Func<string, object?, Task>? setFieldValue)
    {
        if (string.IsNullOrWhiteSpace(step.Target))
            return FormEventDispatchResult.Failure("SetFieldValue action requires a target field.");

        object? value = step.Value is null && step.Arguments?.TryGetValue("value", out object? argumentValue) == true
            ? NormalizeValue(argumentValue)
            : NormalizeValue(step.Value);
        if (setFieldValue is not null)
        {
            await setFieldValue(step.Target, value);
            return FormEventDispatchResult.Success();
        }

        if (record is IDictionary<string, object?> mutableRecord)
        {
            string key = mutableRecord.Keys.FirstOrDefault(candidate => string.Equals(candidate, step.Target, StringComparison.OrdinalIgnoreCase))
                ?? step.Target;
            mutableRecord[key] = value;
            return FormEventDispatchResult.Success();
        }

        return FormEventDispatchResult.Failure(
            $"SetFieldValue action could not update field '{step.Target}' because the current record is read-only.");
    }

    private static async Task<FormEventDispatchResult> ShowMessageAsync(
        DbActionStep step,
        Func<string, Task>? showMessage)
    {
        string message = ReadMessage(step) ?? string.Empty;
        if (string.IsNullOrWhiteSpace(message))
            return FormEventDispatchResult.Failure("ShowMessage action requires a message.");

        if (showMessage is not null)
            await showMessage(message);

        return FormEventDispatchResult.Success(message);
    }

    private static Dictionary<string, string> BuildStepMetadata(
        DbActionSequence sequence,
        DbActionStep step,
        int stepIndex,
        IReadOnlyDictionary<string, string> metadata)
    {
        var result = new Dictionary<string, string>(metadata, StringComparer.OrdinalIgnoreCase)
        {
            ["actionKind"] = step.Kind.ToString(),
            ["actionStep"] = stepIndex.ToString(CultureInfo.InvariantCulture),
        };

        if (!string.IsNullOrWhiteSpace(sequence.Name))
            result["actionSequence"] = sequence.Name;

        if (!string.IsNullOrWhiteSpace(step.Target))
            result["actionTarget"] = step.Target;

        if (!string.IsNullOrWhiteSpace(step.Condition))
            result["actionCondition"] = step.Condition;

        string? sequenceName = ReadSequenceName(step);
        if (!string.IsNullOrWhiteSpace(sequenceName))
            result["actionSequenceTarget"] = sequenceName;

        return result;
    }

    private static FormActionRuntimeContext BuildRuntimeContext(
        DbActionSequence sequence,
        DbActionStep step,
        int stepIndex,
        IReadOnlyDictionary<string, object?>? record,
        IReadOnlyDictionary<string, object?>? bindingArguments,
        IReadOnlyDictionary<string, object?>? runtimeArguments,
        IReadOnlyDictionary<string, string> metadata)
    {
        Dictionary<string, string> stepMetadata = BuildStepMetadata(sequence, step, stepIndex, metadata);
        return new FormActionRuntimeContext(
            ReadMetadata(stepMetadata, "formId"),
            ReadMetadata(stepMetadata, "formName"),
            ReadMetadata(stepMetadata, "tableName"),
            ReadMetadata(stepMetadata, "event"),
            string.IsNullOrWhiteSpace(sequence.Name) ? null : sequence.Name,
            stepIndex,
            record,
            bindingArguments,
            runtimeArguments,
            NormalizeArguments(step.Arguments),
            stepMetadata);
    }

    private static string? ReadMetadata(IReadOnlyDictionary<string, string> metadata, string key)
        => metadata.TryGetValue(key, out string? value) && !string.IsNullOrWhiteSpace(value)
            ? value
            : null;

    private static string? ReadSequenceName(DbActionStep step)
    {
        if (!string.IsNullOrWhiteSpace(step.SequenceName))
            return step.SequenceName.Trim();

        if (!string.IsNullOrWhiteSpace(step.Target))
            return step.Target.Trim();

        if (step.Arguments is null)
            return null;

        foreach (string key in new[] { "sequenceName", "sequence", "name" })
        {
            if (step.Arguments.TryGetValue(key, out object? value))
                return NormalizeValue(value)?.ToString();
        }

        return null;
    }

    private static IReadOnlyDictionary<string, object?>? MergeRuntimeArguments(
        IReadOnlyDictionary<string, object?>? runtimeArguments,
        IReadOnlyDictionary<string, object?>? stepArguments)
    {
        if (stepArguments is null || stepArguments.Count == 0)
            return runtimeArguments;

        var merged = runtimeArguments is null
            ? new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
            : new Dictionary<string, object?>(runtimeArguments, StringComparer.OrdinalIgnoreCase);

        foreach ((string key, object? value) in stepArguments)
            merged[key] = NormalizeValue(value);

        return merged;
    }

    private static IReadOnlyDictionary<string, object?> NormalizeArguments(
        IReadOnlyDictionary<string, object?>? arguments)
    {
        if (arguments is null || arguments.Count == 0)
            return EmptyObjectDictionary.Instance;

        var result = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        foreach ((string key, object? value) in arguments)
        {
            if (!string.IsNullOrWhiteSpace(key))
                result[key] = NormalizeValue(value);
        }

        return result;
    }

    private static object? ReadValue(DbActionStep step, IReadOnlyDictionary<string, object?> arguments)
        => step.Value is null && arguments.TryGetValue("value", out object? argumentValue)
            ? argumentValue
            : NormalizeValue(step.Value);

    private static string? ReadText(object? value)
        => NormalizeValue(value)?.ToString()?.Trim();

    private static string? ReadArgumentText(
        IReadOnlyDictionary<string, object?> arguments,
        params string[] keys)
    {
        foreach (string key in keys)
        {
            if (arguments.TryGetValue(key, out object? value))
            {
                string? text = ReadText(value);
                if (!string.IsNullOrWhiteSpace(text))
                    return text;
            }
        }

        return null;
    }

    private static string? ReadMessage(DbActionStep step)
    {
        if (!string.IsNullOrWhiteSpace(step.Message))
            return step.Message;

        if (step.Value is string text)
            return text;

        if (step.Value is JsonElement { ValueKind: JsonValueKind.String } json)
            return json.GetString();

        return null;
    }

    private static object? NormalizeValue(object? value)
    {
        return value switch
        {
            JsonElement json => NormalizeJsonValue(json),
            _ => value,
        };
    }

    private static object? NormalizeJsonValue(JsonElement value)
    {
        return value.ValueKind switch
        {
            JsonValueKind.Null => null,
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.String => value.GetString(),
            JsonValueKind.Number => value.TryGetInt64(out long integer) ? integer : value.GetDouble(),
            JsonValueKind.Object => value.EnumerateObject().ToDictionary(
                static property => property.Name,
                static property => NormalizeJsonValue(property.Value),
                StringComparer.OrdinalIgnoreCase),
            JsonValueKind.Array => value.EnumerateArray().Select(NormalizeJsonValue).ToArray(),
            _ => value.ToString(),
        };
    }

    private static class EmptyObjectDictionary
    {
        public static readonly IReadOnlyDictionary<string, object?> Instance =
            new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
    }
}
