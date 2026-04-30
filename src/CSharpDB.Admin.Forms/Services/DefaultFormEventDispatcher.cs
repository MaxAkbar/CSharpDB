using CSharpDB.Admin.Forms.Contracts;
using CSharpDB.Admin.Forms.Models;
using CSharpDB.Primitives;

namespace CSharpDB.Admin.Forms.Services;

public sealed class DefaultFormEventDispatcher : IFormEventDispatcher
{
    private readonly DbCommandRegistry _commands;
    private readonly IFormActionRuntime _actionRuntime;

    public DefaultFormEventDispatcher(DbCommandRegistry commands)
        : this(commands, NullFormActionRuntime.Instance)
    {
    }

    public DefaultFormEventDispatcher(DbCommandRegistry commands, IFormActionRuntime actionRuntime)
    {
        ArgumentNullException.ThrowIfNull(commands);
        ArgumentNullException.ThrowIfNull(actionRuntime);

        _commands = commands;
        _actionRuntime = actionRuntime;
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
                    return FormEventDispatchResult.Failure($"Unknown form command '{binding.CommandName}' for event '{eventKind}'.");

                Dictionary<string, DbValue> arguments = FormCommandInvocation.BuildArguments(record, binding.Arguments);
                bool commandFailed = false;
                string? commandFailureMessage = null;
                try
                {
                    DbCommandResult result = await definition.InvokeAsync(arguments, metadata, ct);
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

                    if (binding.ActionSequence is null)
                        continue;
                }
            }
            else if (binding.ActionSequence is null)
            {
                return FormEventDispatchResult.Failure($"Form event '{eventKind}' has no command or action sequence.");
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
                    ct: ct);

                if (!actionResult.Succeeded && binding.StopOnFailure)
                    return actionResult;
            }
        }

        return FormEventDispatchResult.Success();
    }
}
