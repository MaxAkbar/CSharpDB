namespace CSharpDB.Daemon.Grpc;

internal static class CSharpDbGrpcMethodPolicy
{
    internal const string HealthCheckMethod =
        "/grpc.health.v1.Health/Check";
    internal const string HealthWatchMethod =
        "/grpc.health.v1.Health/Watch";

    private const string CSharpDbServicePrefix =
        "/csharpdb.rpc.CSharpDbRpc/";

    private static readonly HashSet<string> s_diagnosticsMethods =
        new(StringComparer.Ordinal)
        {
            CSharpDbServicePrefix + "GetRuntimeDiagnostics",
            CSharpDbServicePrefix + "GetStorageDiagnostics",
            CSharpDbServicePrefix + "GetWalDiagnostics",
            CSharpDbServicePrefix + "GetActiveQueries",
            CSharpDbServicePrefix + "GetRecentQueries",
            CSharpDbServicePrefix + "GetQueryPlanDiagnostics",
            CSharpDbServicePrefix + "GetSessions",
            CSharpDbServicePrefix + "GetActiveMaintenanceOperations",
            CSharpDbServicePrefix + "GetRecentMaintenanceOperations",
            CSharpDbServicePrefix + "GetQueryDetail",
        };

    internal static bool IsHealthMethod(string? method)
        => string.Equals(method, HealthCheckMethod, StringComparison.Ordinal) ||
           string.Equals(method, HealthWatchMethod, StringComparison.Ordinal);

    internal static bool IsDiagnosticsMethod(string? method)
        => method is not null && s_diagnosticsMethods.Contains(method);
}
