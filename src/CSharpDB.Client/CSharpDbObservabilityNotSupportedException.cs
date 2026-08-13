namespace CSharpDB.Client;

/// <summary>
/// Thrown when a client or remote server does not implement the optional
/// CSharpDB runtime-observability capability.
/// </summary>
public sealed class CSharpDbObservabilityNotSupportedException : CSharpDbClientException
{
    public const string SafeMessage =
        "This CSharpDB client or server does not support runtime observability diagnostics.";

    public CSharpDbObservabilityNotSupportedException()
        : base(SafeMessage)
    {
    }

    public CSharpDbObservabilityNotSupportedException(Exception innerException)
        : base(SafeMessage, innerException)
    {
    }
}
