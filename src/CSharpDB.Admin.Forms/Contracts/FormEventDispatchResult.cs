namespace CSharpDB.Admin.Forms.Contracts;

public sealed record FormEventDispatchResult(bool Succeeded, string? Message = null)
{
    public static FormEventDispatchResult Success(string? message = null) => new(true, message);

    public static FormEventDispatchResult Failure(string message)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(message);
        return new(false, message);
    }
}
