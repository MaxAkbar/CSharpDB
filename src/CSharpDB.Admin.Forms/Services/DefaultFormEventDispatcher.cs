using CSharpDB.Admin.Forms.Contracts;
using CSharpDB.Admin.Forms.Models;
using CSharpDB.CodeModules;
using CSharpDB.Primitives;

namespace CSharpDB.Admin.Forms.Services;

public sealed class DefaultFormEventDispatcher : IFormEventDispatcher
{
    private readonly DbCommandRegistry _commands;
    private readonly DbExtensionPolicy _callbackPolicy;
    private readonly IFormActionRuntime _actionRuntime;
    private readonly ICodeModuleFormEventDispatcher _codeModules;

    public DefaultFormEventDispatcher(DbCommandRegistry commands)
        : this(commands, DbExtensionPolicies.DefaultHostCallbackPolicy, NullFormActionRuntime.Instance, NullCodeModuleFormEventDispatcher.Instance)
    {
    }

    public DefaultFormEventDispatcher(DbCommandRegistry commands, IFormActionRuntime actionRuntime)
        : this(commands, DbExtensionPolicies.DefaultHostCallbackPolicy, actionRuntime, NullCodeModuleFormEventDispatcher.Instance)
    {
    }

    public DefaultFormEventDispatcher(
        DbCommandRegistry commands,
        DbExtensionPolicy callbackPolicy,
        IFormActionRuntime actionRuntime)
        : this(commands, callbackPolicy, actionRuntime, NullCodeModuleFormEventDispatcher.Instance)
    {
    }

    public DefaultFormEventDispatcher(
        DbCommandRegistry commands,
        DbExtensionPolicy callbackPolicy,
        IFormActionRuntime actionRuntime,
        ICodeModuleFormEventDispatcher codeModules)
    {
        ArgumentNullException.ThrowIfNull(commands);
        ArgumentNullException.ThrowIfNull(callbackPolicy);
        ArgumentNullException.ThrowIfNull(actionRuntime);
        ArgumentNullException.ThrowIfNull(codeModules);

        _commands = commands;
        _callbackPolicy = callbackPolicy;
        _actionRuntime = actionRuntime;
        _codeModules = codeModules;
    }

    public async Task<FormEventDispatchResult> DispatchAsync(
        FormDefinition form,
        FormEventKind eventKind,
        IReadOnlyDictionary<string, object?>? record = null,
        CancellationToken ct = default)
        => await DispatchAsync(form, eventKind, record, _actionRuntime, ct);

    public async Task<FormEventDispatchResult> DispatchAsync(
        FormDefinition form,
        FormEventKind eventKind,
        IReadOnlyDictionary<string, object?>? record,
        IFormActionRuntime actionRuntime,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(form);
        ArgumentNullException.ThrowIfNull(actionRuntime);

        IReadOnlyList<FormEventBinding> bindings = form.EventBindings ?? [];
        foreach (FormEventBinding binding in bindings.Where(binding => binding.Event == eventKind))
        {
            Dictionary<string, string> metadata = FormCommandInvocation.BuildMetadata(form);
            metadata["event"] = eventKind.ToString();

            if (!string.IsNullOrWhiteSpace(binding.CommandName))
            {
                if (!_commands.TryGetCommand(binding.CommandName, out DbCommandDefinition definition))
                {
                    string message = $"Unknown form command '{binding.CommandName}' for event '{eventKind}'.";
                    DbCallbackDiagnostics.WriteMissingCommandInvocation(binding.CommandName, metadata, message);
                    return FormEventDispatchResult.Failure(message);
                }

                Dictionary<string, DbValue> arguments = FormCommandInvocation.BuildArguments(record, binding.Arguments);
                bool commandFailed = false;
                string? commandFailureMessage = null;
                try
                {
                    DbCommandResult result = await definition.InvokeAsync(
                        arguments,
                        metadata,
                        _callbackPolicy,
                        DbExtensionHostMode.Embedded,
                        ct);
                    if (!result.Succeeded)
                    {
                        commandFailed = true;
                        commandFailureMessage = string.IsNullOrWhiteSpace(result.Message)
                            ? $"Form event '{eventKind}' command '{definition.Name}' failed."
                            : result.Message;
                    }
                }
                catch (OperationCanceledException) when (ct.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    commandFailed = true;
                    commandFailureMessage = $"Form event '{eventKind}' command '{definition.Name}' failed: {ex.Message}";
                }

                if (commandFailed)
                {
                    if (binding.StopOnFailure)
                        return FormEventDispatchResult.Failure(commandFailureMessage!);

                    if (binding.CodeHandler is null && binding.ActionSequence is null)
                        continue;
                }
            }
            else if (binding.CodeHandler is null && binding.ActionSequence is null)
            {
                return FormEventDispatchResult.Failure($"Form event '{eventKind}' has no command, code handler, or action sequence.");
            }

            if (binding.CodeHandler is not null)
            {
                var commandApi = new AdminFormCodeModuleCommandApi(
                    form,
                    _commands,
                    _callbackPolicy,
                    record,
                    binding.Arguments,
                    runtimeArguments: null,
                    metadata,
                    form.ActionSequences,
                    actionRuntime);
                CodeModuleFormDispatchResult codeResult = await _codeModules.DispatchAsync(
                    binding.CodeHandler,
                    new CodeModuleFormEventDispatchContext(
                        form.FormId,
                        form.Name,
                        form.TableName,
                        eventKind.ToString(),
                        record,
                        binding.Arguments,
                        RuntimeArguments: null,
                        metadata,
                        commandApi,
                        IsCancelableEvent(eventKind)),
                    ct);

                if (!codeResult.Succeeded)
                {
                    if (binding.StopOnFailure)
                        return FormEventDispatchResult.Failure(codeResult.Message ?? $"Form event '{eventKind}' code handler failed.");

                    if (binding.ActionSequence is null)
                        continue;
                }
            }

            if (binding.ActionSequence is not null)
            {
                FormEventDispatchResult actionResult = await FormActionSequenceExecutor.ExecuteAsync(
                    binding.ActionSequence,
                    _commands,
                    record,
                    binding.Arguments,
                    runtimeArguments: null,
                    metadata,
                    reusableSequences: form.ActionSequences,
                    actionRuntime: actionRuntime,
                    callbackPolicy: _callbackPolicy,
                    ct: ct);

                if (!actionResult.Succeeded && binding.StopOnFailure)
                    return actionResult;
            }
        }

        return FormEventDispatchResult.Success();
    }

    private static bool IsCancelableEvent(FormEventKind eventKind)
        => eventKind is FormEventKind.BeforeInsert or FormEventKind.BeforeUpdate or FormEventKind.BeforeDelete;
}
