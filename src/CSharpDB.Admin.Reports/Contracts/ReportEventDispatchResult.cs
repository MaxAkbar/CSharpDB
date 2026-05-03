namespace CSharpDB.Admin.Reports.Contracts;

public sealed record ReportEventDispatchResult(bool Succeeded, string? Message = null)
{
    public static ReportEventDispatchResult Success(string? message = null) => new(true, message);

    public static ReportEventDispatchResult Failure(string message)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(message);
        return new(false, message);
    }
}
