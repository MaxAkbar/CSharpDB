using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using CSharpDB.Client.Grpc;
using CSharpDB.Observability;
using Grpc.Core;

namespace CSharpDB.Client.Internal;

internal sealed partial class GrpcTransportClient
{
    public Task<DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot>>
        GetRuntimeDiagnosticsAsync(CancellationToken ct = default)
        => CallDiagnosticsAsync(
            _client.GetRuntimeDiagnosticsAsync(
                EmptyRequest,
                cancellationToken: ct),
            DiagnosticsJsonTypeInfo<DiagnosticsTopologySnapshot<
                RuntimeDiagnosticsSnapshot>>(),
            ct);

    public Task<DiagnosticsTopologySnapshot<
        DiagnosticsValueSnapshot<StorageRuntimeDiagnosticsSnapshot>>>
        GetStorageDiagnosticsAsync(CancellationToken ct = default)
        => CallDiagnosticsAsync(
            _client.GetStorageDiagnosticsAsync(
                EmptyRequest,
                cancellationToken: ct),
            DiagnosticsJsonTypeInfo<DiagnosticsTopologySnapshot<
                DiagnosticsValueSnapshot<StorageRuntimeDiagnosticsSnapshot>>>(),
            ct);

    public Task<DiagnosticsTopologySnapshot<
        DiagnosticsValueSnapshot<WalRuntimeDiagnosticsSnapshot>>>
        GetWalDiagnosticsAsync(CancellationToken ct = default)
        => CallDiagnosticsAsync(
            _client.GetWalDiagnosticsAsync(
                EmptyRequest,
                cancellationToken: ct),
            DiagnosticsJsonTypeInfo<DiagnosticsTopologySnapshot<
                DiagnosticsValueSnapshot<WalRuntimeDiagnosticsSnapshot>>>(),
            ct);

    public Task<DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>>>
        GetActiveQueriesAsync(
            int maximumRecords,
            CancellationToken ct = default)
    {
        ValidateDiagnosticsMaximumRecords(maximumRecords);
        return CallDiagnosticsAsync(
            _client.GetActiveQueriesAsync(
                new DiagnosticsRecordsRequest
                {
                    MaximumRecords = maximumRecords,
                },
                cancellationToken: ct),
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
        return CallDiagnosticsAsync(
            _client.GetRecentQueriesAsync(
                new DiagnosticsRecordsRequest
                {
                    MaximumRecords = maximumRecords,
                },
                cancellationToken: ct),
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
        return CallDiagnosticsAsync(
            _client.GetQueryPlanDiagnosticsAsync(
                new DiagnosticsOperationRequest
                {
                    OperationId = operationId.Value,
                },
                cancellationToken: ct),
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
        return CallDiagnosticsAsync(
            _client.GetSessionsAsync(
                new DiagnosticsRecordsRequest
                {
                    MaximumRecords = maximumRecords,
                },
                cancellationToken: ct),
            DiagnosticsJsonTypeInfo<DiagnosticsTopologySnapshot<
                DiagnosticsCollectionSnapshot<SessionDiagnosticsSnapshot>>>(),
            ct);
    }

    public Task<DiagnosticsTopologySnapshot<
        DiagnosticsCollectionSnapshot<MaintenanceOperationSnapshot>>>
        GetActiveMaintenanceOperationsAsync(
            int maximumRecords,
            CancellationToken ct = default)
    {
        ValidateDiagnosticsMaximumRecords(maximumRecords);
        return CallDiagnosticsAsync(
            _client.GetActiveMaintenanceOperationsAsync(
                new DiagnosticsRecordsRequest
                {
                    MaximumRecords = maximumRecords,
                },
                cancellationToken: ct),
            DiagnosticsJsonTypeInfo<DiagnosticsTopologySnapshot<
                DiagnosticsCollectionSnapshot<MaintenanceOperationSnapshot>>>(),
            ct);
    }

    public Task<DiagnosticsTopologySnapshot<
        DiagnosticsCollectionSnapshot<MaintenanceOperationSnapshot>>>
        GetRecentMaintenanceOperationsAsync(
            int maximumRecords,
            CancellationToken ct = default)
    {
        ValidateDiagnosticsMaximumRecords(maximumRecords);
        return CallDiagnosticsAsync(
            _client.GetRecentMaintenanceOperationsAsync(
                new DiagnosticsRecordsRequest
                {
                    MaximumRecords = maximumRecords,
                },
                cancellationToken: ct),
            DiagnosticsJsonTypeInfo<DiagnosticsTopologySnapshot<
                DiagnosticsCollectionSnapshot<MaintenanceOperationSnapshot>>>(),
            ct);
    }

    public Task<DiagnosticsTopologySnapshot<DiagnosticsValueSnapshot<QueryDetailSnapshot>>>
        GetQueryDetailAsync(
            OpaqueDiagnosticsId operationId,
            CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(operationId);
        return CallDiagnosticsAsync(
            _client.GetQueryDetailAsync(
                new DiagnosticsOperationRequest
                {
                    OperationId = operationId.Value,
                },
                cancellationToken: ct),
            DiagnosticsJsonTypeInfo<DiagnosticsTopologySnapshot<
                DiagnosticsValueSnapshot<QueryDetailSnapshot>>>(),
            ct);
    }

    private static async Task<T> CallDiagnosticsAsync<T>(
        AsyncUnaryCall<DiagnosticsJsonResponse> call,
        JsonTypeInfo<T> typeInfo,
        CancellationToken ct)
    {
        try
        {
            DiagnosticsJsonResponse response = await call.ResponseAsync
                .ConfigureAwait(false);
            T? payload = JsonSerializer.Deserialize(
                response.JsonUtf8.Span,
                typeInfo);
            return payload ?? throw new CSharpDbClientException(
                "gRPC diagnostics transport returned an empty response payload.");
        }
        catch (RpcException ex) when (
            ex.StatusCode == StatusCode.Cancelled &&
            ct.IsCancellationRequested)
        {
            throw new OperationCanceledException(ct);
        }
        catch (RpcException ex) when (ex.StatusCode == StatusCode.Unimplemented)
        {
            // An older or custom server may control the gRPC status detail.
            // Do not retain that remote text in the public safe exception.
            throw new CSharpDbObservabilityNotSupportedException();
        }
        catch (RpcException ex) when (
            ex.StatusCode is StatusCode.Unauthenticated or
            StatusCode.PermissionDenied)
        {
            // Do not retain the server-controlled status detail or trailers.
            throw new CSharpDbObservabilityAccessDeniedException();
        }
        catch (RpcException ex)
        {
            throw TranslateRpcException(ex);
        }
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
