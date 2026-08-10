using CSharpDB.Engine;
using CSharpDB.Observability;

namespace CSharpDB.Data;

/// <summary>
/// Defers lifecycle listener callbacks until a Data-layer serialization boundary
/// has been released. The disabled path deliberately returns before creating a
/// session identifier or operation scope.
/// </summary>
internal static class DataLifecycleDiagnosticBoundary
{
    internal static bool IsDatabaseOpenBoundaryEnabled(
        DatabaseOptions? databaseOptions)
        => IsLifecycleLoggingEnabled(
            databaseOptions?.ObservabilityOptions);

    internal static bool IsDatabaseCloseBoundaryEnabled(
        CSharpDbObservabilityOptions? observabilityOptions)
        => IsLifecycleLoggingEnabled(observabilityOptions);

    internal static bool IsTransactionBoundaryEnabled(
        CSharpDbObservabilityOptions? observabilityOptions)
        => IsLifecycleLoggingEnabled(observabilityOptions);

    internal static IDisposable EnterEnabledBoundary(
        OpaqueDiagnosticsId sessionId)
    {
        ArgumentNullException.ThrowIfNull(sessionId);
        return CSharpDbOperationScope.EnterBoundary(
            CSharpDbTransport.Direct,
            sessionId);
    }

    internal static bool IsLifecycleLoggingEnabled(
        CSharpDbObservabilityOptions? observabilityOptions)
        => observabilityOptions is
        {
            Enabled: true,
            Logging.Enabled: true,
        };
}
