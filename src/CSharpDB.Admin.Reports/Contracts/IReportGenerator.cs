using CSharpDB.Admin.Reports.Models;

namespace CSharpDB.Admin.Reports.Contracts;

public interface IReportGenerator
{
    ReportDefinition GenerateDefault(ReportSourceDefinition source);
}
