using CSharpDB.Admin.Reports.Models;

namespace CSharpDB.Admin.Reports.Contracts;

public abstract record ReportUpdateResult
{
    public sealed record Ok(ReportDefinition Doc) : ReportUpdateResult;
    public sealed record Conflict : ReportUpdateResult;
    public sealed record NotFound : ReportUpdateResult;
}
