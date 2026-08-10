using CSharpDB.Engine;
using CSharpDB.Observability;
using CSharpDB.Sql;

namespace CSharpDB.Data;

internal static class QueryObservabilitySource
{
    internal static QueryFingerprint? CreateFingerprint(
        Database database,
        string? observabilitySql)
    {
        ArgumentNullException.ThrowIfNull(database);

        if (!database.IsObservabilityEnabled ||
            CSharpDbOperationScope.IsDiagnosticsSuppressed ||
            string.IsNullOrWhiteSpace(observabilitySql) ||
            !HasQueryListener())
        {
            return null;
        }

        try
        {
            return SqlQueryFingerprintProvider.Instance.CreateFingerprint(observabilitySql);
        }
        catch
        {
            // Source fingerprinting is diagnostic work and must never change
            // execution of an otherwise valid prepared command.
            return null;
        }
    }

    private static bool HasQueryListener()
    {
        CSharpDbDiagnosticEventPublisher publisher = CSharpDbDiagnostics.EventPublisher;
        return publisher.IsEnabled(CSharpDbLogEvents.QueryCompleted) ||
               publisher.IsEnabled(CSharpDbLogEvents.QueryFailed) ||
               publisher.IsEnabled(CSharpDbLogEvents.QueryCanceled) ||
               publisher.IsEnabled(CSharpDbLogEvents.SlowQuery);
    }

    internal static bool IsObservationRequested(CSharpDbObservabilityOptions? options)
    {
        if (CSharpDbOperationScope.IsDiagnosticsSuppressed ||
            options?.Enabled != true ||
            options.Logging?.Enabled != true)
            return false;

        CSharpDbDiagnosticEventPublisher publisher = CSharpDbDiagnostics.EventPublisher;
        return (options.Logging.Queries &&
                (publisher.IsEnabled(CSharpDbLogEvents.QueryCompleted) ||
                 publisher.IsEnabled(CSharpDbLogEvents.QueryFailed) ||
                 publisher.IsEnabled(CSharpDbLogEvents.QueryCanceled))) ||
               (options.Logging.SlowQueries && publisher.IsEnabled(CSharpDbLogEvents.SlowQuery));
    }

    internal static bool IsBoundaryRequired(CSharpDbObservabilityOptions? options)
        => !CSharpDbOperationScope.IsDiagnosticsSuppressed &&
           options?.Enabled == true &&
           options.Logging?.Enabled == true &&
           (options.Logging.Queries || options.Logging.SlowQueries);
}
