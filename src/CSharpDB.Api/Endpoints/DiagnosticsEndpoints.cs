using System.Globalization;
using System.Text.Json.Serialization.Metadata;
using CSharpDB.Api.Diagnostics;
using CSharpDB.Api.Security;
using CSharpDB.Client;
using CSharpDB.Observability;
using Microsoft.Extensions.Options;

namespace CSharpDB.Api.Endpoints;

public static class DiagnosticsEndpoints
{
    private const string UnauthorizedDetail =
        "A valid CSharpDB API key is required for runtime diagnostics.";
    private const string ForbiddenDetail =
        "Runtime diagnostics access is not permitted from this endpoint.";

    public static RouteGroupBuilder MapDiagnosticsEndpoints(
        this RouteGroupBuilder group)
    {
        group.MapGet("/diagnostics/runtime", GetRuntimeDiagnostics);
        group.MapGet("/diagnostics/storage", GetStorageDiagnostics);
        group.MapGet("/diagnostics/wal", GetWalDiagnostics);
        group.MapGet("/diagnostics/queries/active", GetActiveQueries);
        group.MapGet("/diagnostics/queries/recent", GetRecentQueries);
        group.MapGet(
            "/diagnostics/queries/{operationId}/plan",
            GetQueryPlanDiagnostics);
        group.MapGet("/diagnostics/sessions", GetSessions);
        group.MapGet(
            "/diagnostics/maintenance/active",
            GetActiveMaintenanceOperations);
        group.MapGet(
            "/diagnostics/maintenance/recent",
            GetRecentMaintenanceOperations);
        group.MapGet(
            "/diagnostics/queries/{operationId}/detail",
            GetQueryDetail);
        return group;
    }

    private static async Task<IResult> GetRuntimeDiagnostics(
        ICSharpDbClient db,
        HttpContext context,
        IOptions<CSharpDbApiSecurityOptions> security)
    {
        IResult? denied = Authorize(
            context,
            security.Value,
            CSharpDbDiagnosticsAccessKind.Runtime);
        return denied ?? await ExecuteAsync(
            db,
            static (capability, ct) => capability.GetRuntimeDiagnosticsAsync(ct),
            JsonTypes.Runtime,
            context.RequestAborted);
    }

    private static async Task<IResult> GetStorageDiagnostics(
        ICSharpDbClient db,
        HttpContext context,
        IOptions<CSharpDbApiSecurityOptions> security)
    {
        IResult? denied = Authorize(
            context,
            security.Value,
            CSharpDbDiagnosticsAccessKind.Runtime);
        return denied ?? await ExecuteAsync(
            db,
            static (capability, ct) =>
                capability.GetStorageDiagnosticsAsync(ct),
            JsonTypes.Storage,
            context.RequestAborted);
    }

    private static async Task<IResult> GetWalDiagnostics(
        ICSharpDbClient db,
        HttpContext context,
        IOptions<CSharpDbApiSecurityOptions> security)
    {
        IResult? denied = Authorize(
            context,
            security.Value,
            CSharpDbDiagnosticsAccessKind.Runtime);
        return denied ?? await ExecuteAsync(
            db,
            static (capability, ct) => capability.GetWalDiagnosticsAsync(ct),
            JsonTypes.Wal,
            context.RequestAborted);
    }

    private static async Task<IResult> GetActiveQueries(
        string? maximumRecords,
        ICSharpDbClient db,
        HttpContext context,
        IOptions<CSharpDbApiSecurityOptions> security)
    {
        IResult? denied = Authorize(
            context,
            security.Value,
            CSharpDbDiagnosticsAccessKind.Runtime);
        if (denied is not null)
            return denied;
        if (!TryParseMaximumRecords(maximumRecords, out int recordLimit))
            return InvalidMaximumRecords();

        return await ExecuteAsync(
            db,
            (capability, ct) => capability.GetActiveQueriesAsync(
                recordLimit,
                ct),
            JsonTypes.Active,
            context.RequestAborted);
    }

    private static async Task<IResult> GetRecentQueries(
        string? maximumRecords,
        ICSharpDbClient db,
        HttpContext context,
        IOptions<CSharpDbApiSecurityOptions> security)
    {
        IResult? denied = Authorize(
            context,
            security.Value,
            CSharpDbDiagnosticsAccessKind.Runtime);
        if (denied is not null)
            return denied;
        if (!TryParseMaximumRecords(maximumRecords, out int recordLimit))
            return InvalidMaximumRecords();

        return await ExecuteAsync(
            db,
            (capability, ct) => capability.GetRecentQueriesAsync(
                recordLimit,
                ct),
            JsonTypes.Recent,
            context.RequestAborted);
    }

    private static async Task<IResult> GetQueryPlanDiagnostics(
        string operationId,
        ICSharpDbClient db,
        HttpContext context,
        IOptions<CSharpDbApiSecurityOptions> security)
    {
        IResult? denied = Authorize(
            context,
            security.Value,
            CSharpDbDiagnosticsAccessKind.Runtime);
        if (denied is not null)
            return denied;
        if (!TryCreateOperationId(operationId, out OpaqueDiagnosticsId? id))
            return InvalidOperationId();

        return await ExecuteAsync(
            db,
            (capability, ct) => capability.GetQueryPlanDiagnosticsAsync(id!, ct),
            JsonTypes.Plan,
            context.RequestAborted);
    }

    private static async Task<IResult> GetSessions(
        string? maximumRecords,
        ICSharpDbClient db,
        HttpContext context,
        IServiceProvider services,
        IOptions<CSharpDbApiSecurityOptions> security)
    {
        IResult? denied = Authorize(
            context,
            security.Value,
            CSharpDbDiagnosticsAccessKind.Runtime);
        if (denied is not null)
            return denied;
        if (!TryParseMaximumRecords(maximumRecords, out int recordLimit))
            return InvalidMaximumRecords();

        return await ExecuteAsync(
            db,
            (capability, ct) => GetSessionsWithHostRequestsAsync(
                capability,
                TryGetHostRequestContributor(services),
                recordLimit,
                ct),
            JsonTypes.Sessions,
            context.RequestAborted);
    }

    private static async Task<IResult> GetQueryDetail(
        string operationId,
        ICSharpDbClient db,
        HttpContext context,
        IOptions<CSharpDbApiSecurityOptions> security)
    {
        IResult? denied = Authorize(
            context,
            security.Value,
            CSharpDbDiagnosticsAccessKind.QueryDetail);
        if (denied is not null)
            return denied;
        if (!TryCreateOperationId(operationId, out OpaqueDiagnosticsId? id))
            return InvalidOperationId();

        return await ExecuteAsync(
            db,
            (capability, ct) => capability.GetQueryDetailAsync(id!, ct),
            JsonTypes.Detail,
            context.RequestAborted);
    }

    private static async Task<IResult> GetActiveMaintenanceOperations(
        string? maximumRecords,
        ICSharpDbClient db,
        HttpContext context,
        IOptions<CSharpDbApiSecurityOptions> security)
    {
        IResult? denied = Authorize(
            context,
            security.Value,
            CSharpDbDiagnosticsAccessKind.Runtime);
        if (denied is not null)
            return denied;
        if (!TryParseMaximumRecords(maximumRecords, out int recordLimit))
            return InvalidMaximumRecords();

        return await ExecuteAsync(
            db,
            (capability, ct) =>
                capability.GetActiveMaintenanceOperationsAsync(recordLimit, ct),
            JsonTypes.ActiveMaintenance,
            context.RequestAborted);
    }

    private static async Task<IResult> GetRecentMaintenanceOperations(
        string? maximumRecords,
        ICSharpDbClient db,
        HttpContext context,
        IOptions<CSharpDbApiSecurityOptions> security)
    {
        IResult? denied = Authorize(
            context,
            security.Value,
            CSharpDbDiagnosticsAccessKind.Runtime);
        if (denied is not null)
            return denied;
        if (!TryParseMaximumRecords(maximumRecords, out int recordLimit))
            return InvalidMaximumRecords();

        return await ExecuteAsync(
            db,
            (capability, ct) =>
                capability.GetRecentMaintenanceOperationsAsync(recordLimit, ct),
            JsonTypes.RecentMaintenance,
            context.RequestAborted);
    }

    private static async Task<IResult> ExecuteAsync<T>(
        ICSharpDbClient db,
        Func<ICSharpDbObservabilityClient, CancellationToken, Task<T>> action,
        JsonTypeInfo<T> jsonTypeInfo,
        CancellationToken cancellationToken)
    {
        if (db is not ICSharpDbObservabilityClient capability)
            return Unsupported();

        try
        {
            using IDisposable suppression =
                CSharpDbOperationScope.SuppressDiagnostics();
            T result = await action(capability, cancellationToken)
                .ConfigureAwait(false);
            return Results.Json(result, jsonTypeInfo);
        }
        catch (CSharpDbObservabilityNotSupportedException)
        {
            return Unsupported();
        }
    }

    private static async Task<DiagnosticsTopologySnapshot<
        DiagnosticsCollectionSnapshot<SessionDiagnosticsSnapshot>>>
        GetSessionsWithHostRequestsAsync(
            ICSharpDbObservabilityClient capability,
            ICSharpDbHostRequestDiagnosticsContributor? contributor,
            int maximumRecords,
            CancellationToken cancellationToken)
    {
        DiagnosticsTopologySnapshot<
            DiagnosticsCollectionSnapshot<SessionDiagnosticsSnapshot>> result =
            await capability.GetSessionsAsync(
                maximumRecords,
                cancellationToken).ConfigureAwait(false);
        return CSharpDbHostRequestDiagnosticsProjection.MergeSessions(
            result,
            contributor,
            maximumRecords);
    }

    private static ICSharpDbHostRequestDiagnosticsContributor?
        TryGetHostRequestContributor(IServiceProvider services)
    {
        try
        {
            if (services.GetService<CSharpDbObservabilityOptions>()
                    ?.Enabled != true)
            {
                return null;
            }

            return services.GetService<
                ICSharpDbHostRequestDiagnosticsContributor>();
        }
        catch
        {
            return null;
        }
    }

    private static IResult? Authorize(
        HttpContext context,
        CSharpDbApiSecurityOptions security,
        CSharpDbDiagnosticsAccessKind accessKind)
    {
        string headerName = CSharpDbApiKeyValidator.NormalizeHeaderName(
            security.ApiKeyHeaderName);
        string? suppliedApiKey = context.Request.Headers.TryGetValue(
            headerName,
            out var values)
            ? values.FirstOrDefault()
            : null;
        CSharpDbDiagnosticsAccessDecision decision =
            CSharpDbDiagnosticsAccessPolicy.Evaluate(
                security,
                context.Connection.RemoteIpAddress,
                suppliedApiKey,
                accessKind);
        return decision switch
        {
            CSharpDbDiagnosticsAccessDecision.Allowed => null,
            CSharpDbDiagnosticsAccessDecision.Unauthenticated => Results.Problem(
                statusCode: StatusCodes.Status401Unauthorized,
                title: "Unauthorized",
                detail: UnauthorizedDetail),
            _ => Results.Problem(
                statusCode: StatusCodes.Status403Forbidden,
                title: "Forbidden",
                detail: ForbiddenDetail),
        };
    }

    private static bool IsValidMaximumRecords(int maximumRecords)
        => maximumRecords > 0 &&
           maximumRecords <= CSharpDbObservabilityOptions.MaximumHistoryCapacity;

    private static bool TryParseMaximumRecords(
        string? value,
        out int maximumRecords)
        => int.TryParse(
               value,
               NumberStyles.None,
               CultureInfo.InvariantCulture,
               out maximumRecords) &&
           IsValidMaximumRecords(maximumRecords);

    private static bool TryCreateOperationId(
        string value,
        out OpaqueDiagnosticsId? operationId)
    {
        try
        {
            operationId = new OpaqueDiagnosticsId(value);
            return true;
        }
        catch (ArgumentException)
        {
            operationId = null;
            return false;
        }
    }

    private static IResult InvalidMaximumRecords()
        => Results.Problem(
            statusCode: StatusCodes.Status400BadRequest,
            title: "Invalid diagnostics record limit",
            detail: $"maximumRecords must be between 1 and {CSharpDbObservabilityOptions.MaximumHistoryCapacity}.");

    private static IResult InvalidOperationId()
        => Results.Problem(
            statusCode: StatusCodes.Status400BadRequest,
            title: "Invalid diagnostics operation id",
            detail: "A 32-character lowercase hexadecimal diagnostics operation id is required.");

    private static IResult Unsupported()
        => Results.Problem(
            statusCode: StatusCodes.Status501NotImplemented,
            title: "Runtime diagnostics are not supported",
            detail: CSharpDbObservabilityNotSupportedException.SafeMessage);

    private static class JsonTypes
    {
        internal static readonly JsonTypeInfo<
            DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot>> Runtime =
            Get<DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot>>();

        internal static readonly JsonTypeInfo<DiagnosticsTopologySnapshot<
            DiagnosticsValueSnapshot<StorageRuntimeDiagnosticsSnapshot>>> Storage =
            Get<DiagnosticsTopologySnapshot<
                DiagnosticsValueSnapshot<StorageRuntimeDiagnosticsSnapshot>>>();

        internal static readonly JsonTypeInfo<DiagnosticsTopologySnapshot<
            DiagnosticsValueSnapshot<WalRuntimeDiagnosticsSnapshot>>> Wal =
            Get<DiagnosticsTopologySnapshot<
                DiagnosticsValueSnapshot<WalRuntimeDiagnosticsSnapshot>>>();

        internal static readonly JsonTypeInfo<DiagnosticsTopologySnapshot<
            DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>>> Active =
            Get<DiagnosticsTopologySnapshot<
                DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>>>();

        internal static readonly JsonTypeInfo<DiagnosticsTopologySnapshot<
            DiagnosticsCollectionSnapshot<RecentQuerySnapshot>>> Recent =
            Get<DiagnosticsTopologySnapshot<
                DiagnosticsCollectionSnapshot<RecentQuerySnapshot>>>();

        internal static readonly JsonTypeInfo<DiagnosticsTopologySnapshot<
            DiagnosticsValueSnapshot<QueryPlanDiagnosticsSnapshot>>> Plan =
            Get<DiagnosticsTopologySnapshot<
                DiagnosticsValueSnapshot<QueryPlanDiagnosticsSnapshot>>>();

        internal static readonly JsonTypeInfo<DiagnosticsTopologySnapshot<
            DiagnosticsCollectionSnapshot<SessionDiagnosticsSnapshot>>> Sessions =
            Get<DiagnosticsTopologySnapshot<
                DiagnosticsCollectionSnapshot<SessionDiagnosticsSnapshot>>>();

        internal static readonly JsonTypeInfo<DiagnosticsTopologySnapshot<
            DiagnosticsCollectionSnapshot<MaintenanceOperationSnapshot>>>
            ActiveMaintenance =
                Get<DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<
                    MaintenanceOperationSnapshot>>>();

        internal static readonly JsonTypeInfo<DiagnosticsTopologySnapshot<
            DiagnosticsCollectionSnapshot<MaintenanceOperationSnapshot>>>
            RecentMaintenance =
                Get<DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<
                    MaintenanceOperationSnapshot>>>();

        internal static readonly JsonTypeInfo<DiagnosticsTopologySnapshot<
            DiagnosticsValueSnapshot<QueryDetailSnapshot>>> Detail =
            Get<DiagnosticsTopologySnapshot<
                DiagnosticsValueSnapshot<QueryDetailSnapshot>>>();

        private static JsonTypeInfo<T> Get<T>()
            => (JsonTypeInfo<T>)(CSharpDbObservabilityJsonContext.Default
                .GetTypeInfo(typeof(T)) ??
                throw new InvalidOperationException(
                    "The diagnostics response is missing source-generated JSON metadata."));
    }
}
