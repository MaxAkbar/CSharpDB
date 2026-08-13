using System.Net;

namespace CSharpDB.Api.Security;

/// <summary>
/// Identifies the diagnostics surface being authorized.
/// </summary>
public enum CSharpDbDiagnosticsAccessKind
{
    Runtime = 0,
    QueryDetail = 1,
}

/// <summary>
/// Transport-neutral outcome of a diagnostics access-policy evaluation.
/// </summary>
public enum CSharpDbDiagnosticsAccessDecision
{
    Forbidden = 0,
    Allowed = 1,
    Unauthenticated = 2,
}

/// <summary>
/// Evaluates the shared access policy for REST and gRPC diagnostics endpoints.
/// It is intentionally not installed as global API authentication policy.
/// </summary>
public static class CSharpDbDiagnosticsAccessPolicy
{
    public static CSharpDbDiagnosticsAccessDecision Evaluate(
        CSharpDbApiSecurityOptions security,
        IPAddress? remoteIpAddress,
        string? suppliedApiKey,
        CSharpDbDiagnosticsAccessKind accessKind)
    {
        ArgumentNullException.ThrowIfNull(security);

        if (!Enum.IsDefined(accessKind))
            return CSharpDbDiagnosticsAccessDecision.Forbidden;

        CSharpDbDiagnosticsAccessDecision baseDecision = security.Mode switch
        {
            CSharpDbRemoteSecurityMode.ApiKey =>
                CSharpDbApiKeyValidator.IsAuthorized(security, suppliedApiKey)
                    ? CSharpDbDiagnosticsAccessDecision.Allowed
                    : CSharpDbDiagnosticsAccessDecision.Unauthenticated,
            CSharpDbRemoteSecurityMode.None => EvaluateUnauthenticatedMode(
                security,
                remoteIpAddress),
            _ => CSharpDbDiagnosticsAccessDecision.Forbidden,
        };

        if (baseDecision != CSharpDbDiagnosticsAccessDecision.Allowed)
            return baseDecision;

        return accessKind == CSharpDbDiagnosticsAccessKind.QueryDetail &&
               !security.AllowSensitiveQueryDetailAccess
            ? CSharpDbDiagnosticsAccessDecision.Forbidden
            : CSharpDbDiagnosticsAccessDecision.Allowed;
    }

    private static CSharpDbDiagnosticsAccessDecision EvaluateUnauthenticatedMode(
        CSharpDbApiSecurityOptions security,
        IPAddress? remoteIpAddress)
    {
        if (!IsProvenRemoteAddress(remoteIpAddress))
            return CSharpDbDiagnosticsAccessDecision.Forbidden;

        return IPAddress.IsLoopback(remoteIpAddress!) ||
               security.AllowInsecureRemoteDiagnostics
            ? CSharpDbDiagnosticsAccessDecision.Allowed
            : CSharpDbDiagnosticsAccessDecision.Forbidden;
    }

    private static bool IsProvenRemoteAddress(IPAddress? address)
        => address is not null &&
           !address.Equals(IPAddress.Any) &&
           !address.Equals(IPAddress.IPv6Any) &&
           !address.Equals(IPAddress.None) &&
           !address.Equals(IPAddress.IPv6None);
}
