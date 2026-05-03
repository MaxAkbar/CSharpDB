using CSharpDB.Admin.Reports.Models;

namespace CSharpDB.Admin.Reports.Contracts;

public interface IReportEventDispatcher
{
    Task<ReportEventDispatchResult> DispatchAsync(
        ReportDefinition report,
        ReportSourceDefinition source,
        ReportEventKind eventKind,
        IReadOnlyDictionary<string, object?>? runtimeArguments = null,
        CancellationToken ct = default);
}
