using CSharpDB.Observability;

namespace CSharpDB.Client;

/// <summary>
/// Optional runtime-observability capability implemented separately from
/// <see cref="ICSharpDbClient"/> so existing client implementations remain
/// source and binary compatible.
/// </summary>
public interface ICSharpDbObservabilityClient
{
    /// <summary>
    /// Gets the current runtime summary without initiating offline inspection.
    /// </summary>
    Task<DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot>>
        GetRuntimeDiagnosticsAsync(CancellationToken ct = default);

    /// <summary>
    /// Gets at most <paramref name="maximumRecords"/> active query records.
    /// </summary>
    Task<DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>>>
        GetActiveQueriesAsync(
            int maximumRecords,
            CancellationToken ct = default);

    /// <summary>
    /// Gets at most <paramref name="maximumRecords"/> recent query records.
    /// </summary>
    Task<DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<RecentQuerySnapshot>>>
        GetRecentQueriesAsync(
            int maximumRecords,
            CancellationToken ct = default);

    /// <summary>
    /// Gets the bounded automatic plan diagnostics retained for one operation.
    /// This method never executes or replays the query.
    /// </summary>
    Task<DiagnosticsTopologySnapshot<DiagnosticsValueSnapshot<QueryPlanDiagnosticsSnapshot>>>
        GetQueryPlanDiagnosticsAsync(
            OpaqueDiagnosticsId operationId,
            CancellationToken ct = default);

    /// <summary>
    /// Gets at most <paramref name="maximumRecords"/> safe logical session
    /// records. Client-managed transaction bearer identifiers are never
    /// returned.
    /// </summary>
    Task<DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<SessionDiagnosticsSnapshot>>>
        GetSessionsAsync(
            int maximumRecords,
            CancellationToken ct = default);

    /// <summary>
    /// Gets separately authorized captured query detail. Ordinary runtime,
    /// active-query, and recent-query responses never contain captured SQL.
    /// </summary>
    Task<DiagnosticsTopologySnapshot<DiagnosticsValueSnapshot<QueryDetailSnapshot>>>
        GetQueryDetailAsync(
            OpaqueDiagnosticsId operationId,
            CancellationToken ct = default);
}
