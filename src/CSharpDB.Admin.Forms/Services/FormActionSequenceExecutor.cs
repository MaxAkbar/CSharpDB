using System.Globalization;
using System.Text.Json;
using CSharpDB.Admin.Forms.Contracts;
using CSharpDB.Primitives;

namespace CSharpDB.Admin.Forms.Services;

internal static class FormActionSequenceExecutor
{
    public static async Task<FormEventDispatchResult> ExecuteAsync(
        DbActionSequence sequence,
        DbCommandRegistry commands,
        IReadOnlyDictionary<string, object?>? record,
        IReadOnlyDictionary<string, object?>? bindingArguments,
        IReadOnlyDictionary<string, object?>? runtimeArguments,
        IReadOnlyDictionary<string, string> metadata,
        Func<string, object?, Task>? setFieldValue = null,
        Func<string, Task>? showMessage = null,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(sequence);
        ArgumentNullException.ThrowIfNull(commands);
        ArgumentNullException.ThrowIfNull(metadata);

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
                setFieldValue,
                showMessage,
                ct);

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
        Func<string, object?, Task>? setFieldValue,
        Func<string, Task>? showMessage,
        CancellationToken ct)
    {
        try
        {
            return step.Kind switch
            {
                DbActionKind.RunCommand => await RunCommandAsync(sequence, step, stepIndex, commands, record, bindingArguments, runtimeArguments, metadata, ct),
                DbActionKind.SetFieldValue => await SetFieldValueAsync(step, record, setFieldValue),
                DbActionKind.ShowMessage => await ShowMessageAsync(step, showMessage),
                DbActionKind.Stop => FormEventDispatchResult.Success(ReadMessage(step)),
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

        return result;
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
}
