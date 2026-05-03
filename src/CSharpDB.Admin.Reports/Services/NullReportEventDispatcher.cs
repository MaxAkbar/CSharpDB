using CSharpDB.Admin.Reports.Contracts;
using CSharpDB.Admin.Reports.Models;

namespace CSharpDB.Admin.Reports.Services;

public sealed class NullReportEventDispatcher : IReportEventDispatcher
{
    public static NullReportEventDispatcher Instance { get; } = new();

    private NullReportEventDispatcher()
    {
    }

    public Task<ReportEventDispatchResult> DispatchAsync(
        ReportDefinition report,
        ReportSourceDefinition source,
        ReportEventKind eventKind,
        IReadOnlyDictionary<string, object?>? runtimeArguments = null,
        CancellationToken ct = default)
        => Task.FromResult(ReportEventDispatchResult.Success());
}
