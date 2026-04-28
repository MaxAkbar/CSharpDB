using CSharpDB.Admin.Forms.Contracts;
using CSharpDB.Admin.Forms.Models;
using CSharpDB.Primitives;

namespace CSharpDB.Admin.Forms.Services;

public sealed class DefaultFormEventDispatcher(DbCommandRegistry commands) : IFormEventDispatcher
{
    public async Task<FormEventDispatchResult> DispatchAsync(
        FormDefinition form,
        FormEventKind eventKind,
        IReadOnlyDictionary<string, object?>? record = null,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(form);

        IReadOnlyList<FormEventBinding> bindings = form.EventBindings ?? [];
        foreach (FormEventBinding binding in bindings.Where(binding => binding.Event == eventKind))
        {
            if (string.IsNullOrWhiteSpace(binding.CommandName))
                return FormEventDispatchResult.Failure($"Form event '{eventKind}' has an empty command name.");

            if (!commands.TryGetCommand(binding.CommandName, out DbCommandDefinition definition))
                return FormEventDispatchResult.Failure($"Unknown form command '{binding.CommandName}' for event '{eventKind}'.");

            Dictionary<string, DbValue> arguments = FormCommandInvocation.BuildArguments(record, binding.Arguments);
            Dictionary<string, string> metadata = FormCommandInvocation.BuildMetadata(form);
            metadata["event"] = eventKind.ToString();

            DbCommandResult result;
            try
            {
                result = await definition.InvokeAsync(arguments, metadata, ct);
            }
            catch (Exception ex)
            {
                return FormEventDispatchResult.Failure(
                    $"Form event '{eventKind}' command '{definition.Name}' failed: {ex.Message}");
            }

            if (!result.Succeeded && binding.StopOnFailure)
            {
                string message = string.IsNullOrWhiteSpace(result.Message)
                    ? $"Form event '{eventKind}' command '{definition.Name}' failed."
                    : result.Message;
                return FormEventDispatchResult.Failure(message);
            }
        }

        return FormEventDispatchResult.Success();
    }
}
