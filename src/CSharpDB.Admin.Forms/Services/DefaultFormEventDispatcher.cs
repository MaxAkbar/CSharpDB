using System.Globalization;
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

            Dictionary<string, DbValue> arguments = BuildArguments(record, binding.Arguments);
            Dictionary<string, string> metadata = new(StringComparer.OrdinalIgnoreCase)
            {
                ["surface"] = "AdminForms",
                ["formId"] = form.FormId,
                ["formName"] = form.Name,
                ["tableName"] = form.TableName,
                ["event"] = eventKind.ToString(),
            };

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

    private static Dictionary<string, DbValue> BuildArguments(
        IReadOnlyDictionary<string, object?>? record,
        IReadOnlyDictionary<string, object?>? configuredArguments)
    {
        var arguments = new Dictionary<string, DbValue>(StringComparer.OrdinalIgnoreCase);

        if (record is not null)
        {
            foreach ((string key, object? value) in record)
                arguments[key] = ToDbValue(value);
        }

        if (configuredArguments is not null)
        {
            foreach ((string key, object? value) in configuredArguments)
            {
                if (!string.IsNullOrWhiteSpace(key))
                    arguments[key] = ToDbValue(value);
            }
        }

        return arguments;
    }

    private static DbValue ToDbValue(object? value) => value switch
    {
        null => DbValue.Null,
        DbValue dbValue => dbValue,
        bool boolValue => DbValue.FromInteger(boolValue ? 1 : 0),
        byte or sbyte or short or ushort or int or uint or long => DbValue.FromInteger(Convert.ToInt64(value, CultureInfo.InvariantCulture)),
        float or double or decimal => DbValue.FromReal(Convert.ToDouble(value, CultureInfo.InvariantCulture)),
        string text => DbValue.FromText(text),
        Guid guid => DbValue.FromText(guid.ToString("D")),
        DateOnly date => DbValue.FromText(date.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture)),
        DateTime dateTime => DbValue.FromText(dateTime.ToString("O", CultureInfo.InvariantCulture)),
        byte[] bytes => DbValue.FromBlob(bytes),
        _ => DbValue.FromText(Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty),
    };
}
