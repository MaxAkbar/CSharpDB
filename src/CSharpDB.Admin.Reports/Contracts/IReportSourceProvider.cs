using CSharpDB.Admin.Reports.Models;

namespace CSharpDB.Admin.Reports.Contracts;

public interface IReportSourceProvider
{
    Task<IReadOnlyList<ReportSourceReferenceItem>> ListSourceReferencesAsync();
    Task<ReportSourceDefinition?> GetSourceDefinitionAsync(ReportSourceReference source);
}
