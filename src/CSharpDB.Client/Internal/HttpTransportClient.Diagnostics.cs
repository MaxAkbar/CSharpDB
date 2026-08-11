using System.Net;
using System.Net.Http.Json;
using System.Text.Json.Serialization.Metadata;
using CSharpDB.Observability;

namespace CSharpDB.Client.Internal;

internal sealed partial class HttpTransportClient
{
    public Task<DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot>>
        GetRuntimeDiagnosticsAsync(CancellationToken ct = default)
        => GetDiagnosticsAsync(
            BuildUri("api/diagnostics/runtime"),
            DiagnosticsJsonTypeInfo<DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot>>(),
            ct);

    public Task<DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>>>
        GetActiveQueriesAsync(
            int maximumRecords,
            CancellationToken ct = default)
    {
        ValidateDiagnosticsMaximumRecords(maximumRecords);
        return GetDiagnosticsAsync(
            BuildUri(
                "api/diagnostics/queries/active",
                Q("maximumRecords", maximumRecords.ToString(
                    System.Globalization.CultureInfo.InvariantCulture))),
            DiagnosticsJsonTypeInfo<DiagnosticsTopologySnapshot<
                DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>>>(),
            ct);
    }

    public Task<DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<RecentQuerySnapshot>>>
        GetRecentQueriesAsync(
            int maximumRecords,
            CancellationToken ct = default)
    {
        ValidateDiagnosticsMaximumRecords(maximumRecords);
        return GetDiagnosticsAsync(
            BuildUri(
                "api/diagnostics/queries/recent",
                Q("maximumRecords", maximumRecords.ToString(
                    System.Globalization.CultureInfo.InvariantCulture))),
            DiagnosticsJsonTypeInfo<DiagnosticsTopologySnapshot<
                DiagnosticsCollectionSnapshot<RecentQuerySnapshot>>>(),
            ct);
    }

    public Task<DiagnosticsTopologySnapshot<DiagnosticsValueSnapshot<QueryPlanDiagnosticsSnapshot>>>
        GetQueryPlanDiagnosticsAsync(
            OpaqueDiagnosticsId operationId,
            CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(operationId);
        return GetDiagnosticsAsync(
            BuildUri(
                $"api/diagnostics/queries/{Escape(operationId.Value)}/plan"),
            DiagnosticsJsonTypeInfo<DiagnosticsTopologySnapshot<
                DiagnosticsValueSnapshot<QueryPlanDiagnosticsSnapshot>>>(),
            ct);
    }

    public Task<DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<SessionDiagnosticsSnapshot>>>
        GetSessionsAsync(
            int maximumRecords,
            CancellationToken ct = default)
    {
        ValidateDiagnosticsMaximumRecords(maximumRecords);
        return GetDiagnosticsAsync(
            BuildUri(
                "api/diagnostics/sessions",
                Q("maximumRecords", maximumRecords.ToString(
                    System.Globalization.CultureInfo.InvariantCulture))),
            DiagnosticsJsonTypeInfo<DiagnosticsTopologySnapshot<
                DiagnosticsCollectionSnapshot<SessionDiagnosticsSnapshot>>>(),
            ct);
    }

    public Task<DiagnosticsTopologySnapshot<DiagnosticsValueSnapshot<QueryDetailSnapshot>>>
        GetQueryDetailAsync(
            OpaqueDiagnosticsId operationId,
            CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(operationId);
        return GetDiagnosticsAsync(
            BuildUri(
                $"api/diagnostics/queries/{Escape(operationId.Value)}/detail"),
            DiagnosticsJsonTypeInfo<DiagnosticsTopologySnapshot<
                DiagnosticsValueSnapshot<QueryDetailSnapshot>>>(),
            ct);
    }

    private async Task<T> GetDiagnosticsAsync<T>(
        Uri uri,
        JsonTypeInfo<T> typeInfo,
        CancellationToken ct)
    {
        using HttpResponseMessage response = await SendAsync(
            HttpMethod.Get,
            uri,
            payload: null,
            ct).ConfigureAwait(false);
        if (response.StatusCode is HttpStatusCode.NotFound or
            HttpStatusCode.NotImplemented)
        {
            throw new CSharpDbObservabilityNotSupportedException();
        }

        if (!response.IsSuccessStatusCode)
            throw await CreateHttpExceptionAsync(response, ct).ConfigureAwait(false);

        T? payload = await response.Content
            .ReadFromJsonAsync(typeInfo, ct)
            .ConfigureAwait(false);
        return payload ?? throw new CSharpDbClientException(
            "HTTP diagnostics transport returned an empty response payload.");
    }

    private static JsonTypeInfo<T> DiagnosticsJsonTypeInfo<T>()
        => (JsonTypeInfo<T>)(CSharpDbObservabilityJsonContext.Default
            .GetTypeInfo(typeof(T)) ??
            throw new InvalidOperationException(
                "The diagnostics response is missing source-generated JSON metadata."));

    private static void ValidateDiagnosticsMaximumRecords(int maximumRecords)
    {
        if (maximumRecords <= 0 ||
            maximumRecords > CSharpDbObservabilityOptions.MaximumHistoryCapacity)
        {
            throw new ArgumentOutOfRangeException(nameof(maximumRecords));
        }
    }
}
