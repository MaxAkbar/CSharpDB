using CSharpDB.Admin.Reports.Models;

namespace CSharpDB.Admin.Reports.Contracts;

public interface IReportRepository
{
    Task<ReportDefinition?> GetAsync(string reportId);
    Task<ReportDefinition> CreateAsync(ReportDefinition report);
    Task<ReportUpdateResult> TryUpdateAsync(string reportId, int expectedVersion, ReportDefinition updated);
    Task<IReadOnlyList<ReportDefinition>> ListAsync(ReportSourceKind? sourceKind = null, string? sourceName = null);
    Task<bool> DeleteAsync(string reportId);
}
