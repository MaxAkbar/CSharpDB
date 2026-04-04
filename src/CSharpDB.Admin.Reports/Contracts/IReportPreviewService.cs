using CSharpDB.Admin.Reports.Models;

namespace CSharpDB.Admin.Reports.Contracts;

public interface IReportPreviewService
{
    Task<ReportPreviewResult> BuildPreviewAsync(ReportDefinition report, CancellationToken ct = default);
}
