namespace CSharpDB.Admin.Reports.Models;

public enum ReportEventKind
{
    OnOpen,
    BeforeRender,
    AfterRender,
}

public sealed record ReportEventBinding(
    ReportEventKind Event,
    string CommandName,
    IReadOnlyDictionary<string, object?>? Arguments = null,
    bool StopOnFailure = true);
