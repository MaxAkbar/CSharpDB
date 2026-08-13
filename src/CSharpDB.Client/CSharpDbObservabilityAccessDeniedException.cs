namespace CSharpDB.Client;

/// <summary>
/// Thrown when a remote server denies access to the optional CSharpDB
/// runtime-observability capability.
/// </summary>
public sealed class CSharpDbObservabilityAccessDeniedException : CSharpDbClientException
{
    public const string SafeMessage =
        "Access to CSharpDB runtime observability diagnostics was denied.";

    public CSharpDbObservabilityAccessDeniedException()
        : base(SafeMessage)
    {
    }
}
