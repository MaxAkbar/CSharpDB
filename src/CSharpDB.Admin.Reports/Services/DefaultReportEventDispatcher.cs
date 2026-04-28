using CSharpDB.Admin.Reports.Contracts;
using CSharpDB.Admin.Reports.Models;
using CSharpDB.Primitives;

namespace CSharpDB.Admin.Reports.Services;

public sealed class DefaultReportEventDispatcher(DbCommandRegistry commands) : IReportEventDispatcher
{
    public async Task<ReportEventDispatchResult> DispatchAsync(
        ReportDefinition report,
        ReportSourceDefinition source,
        ReportEventKind eventKind,
        IReadOnlyDictionary<string, object?>? runtimeArguments = null,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(report);
        ArgumentNullException.ThrowIfNull(source);

        IReadOnlyList<ReportEventBinding> bindings = report.EventBindings ?? [];
        foreach (ReportEventBinding binding in bindings.Where(binding => binding.Event == eventKind))
        {
            if (string.IsNullOrWhiteSpace(binding.CommandName))
                return ReportEventDispatchResult.Failure($"Report event '{eventKind}' has an empty command name.");

            if (!commands.TryGetCommand(binding.CommandName, out DbCommandDefinition definition))
                return ReportEventDispatchResult.Failure($"Unknown report command '{binding.CommandName}' for event '{eventKind}'.");

            Dictionary<string, DbValue> arguments = DbCommandArguments.FromObjectDictionary(runtimeArguments, binding.Arguments);
            Dictionary<string, string> metadata = BuildMetadata(report, source, eventKind);

            DbCommandResult result;
            try
            {
                result = await definition.InvokeAsync(arguments, metadata, ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                return ReportEventDispatchResult.Failure(
                    $"Report event '{eventKind}' command '{definition.Name}' failed: {ex.Message}");
            }

            if (!result.Succeeded && binding.StopOnFailure)
            {
                string message = string.IsNullOrWhiteSpace(result.Message)
                    ? $"Report event '{eventKind}' command '{definition.Name}' failed."
                    : result.Message;
                return ReportEventDispatchResult.Failure(message);
            }
        }

        return ReportEventDispatchResult.Success();
    }

    private static Dictionary<string, string> BuildMetadata(
        ReportDefinition report,
        ReportSourceDefinition source,
        ReportEventKind eventKind)
        => new(StringComparer.OrdinalIgnoreCase)
        {
            ["surface"] = "AdminReports",
            ["reportId"] = report.ReportId,
            ["reportName"] = report.Name,
            ["sourceKind"] = source.Kind.ToString(),
            ["sourceName"] = source.Name,
            ["event"] = eventKind.ToString(),
        };
}
