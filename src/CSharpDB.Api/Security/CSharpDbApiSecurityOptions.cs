namespace CSharpDB.Api.Security;

public sealed class CSharpDbApiSecurityOptions
{
    public const string DefaultApiKeyHeaderName = "X-CSharpDB-Api-Key";

    public CSharpDbRemoteSecurityMode Mode { get; set; } = CSharpDbRemoteSecurityMode.None;

    public string? ApiKey { get; set; }

    public string ApiKeyHeaderName { get; set; } = DefaultApiKeyHeaderName;

    /// <summary>
    /// Explicitly permits diagnostics requests from non-loopback addresses
    /// when <see cref="Mode"/> is <see cref="CSharpDbRemoteSecurityMode.None"/>.
    /// </summary>
    public bool AllowInsecureRemoteDiagnostics { get; set; }

    /// <summary>
    /// Explicitly authorizes access to sensitive query-detail diagnostics.
    /// This acknowledgement is required in addition to the ordinary
    /// diagnostics access policy.
    /// </summary>
    public bool AllowSensitiveQueryDetailAccess { get; set; }
}
