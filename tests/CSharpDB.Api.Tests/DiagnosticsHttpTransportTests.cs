using System.Net;
using System.Reflection;
using CSharpDB.Api.Diagnostics;
using CSharpDB.Api.Security;
using CSharpDB.Client;
using CSharpDB.Observability;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using ClientTransport = CSharpDB.Client.CSharpDbTransport;

namespace CSharpDB.Api.Tests;

public sealed class DiagnosticsHttpTransportTests
{
    private const string ApiKey = "diagnostics-http-test-key";
    private static readonly OpaqueDiagnosticsId OperationId =
        new("0123456789abcdef0123456789abcdef");

    [Fact]
    public async Task HttpCapability_RoundTripsAllMethods_WithSourceGeneratedContracts()
    {
        IDiagnosticsCaptureClient inner =
            DispatchProxy.Create<IDiagnosticsCaptureClient, DiagnosticsCaptureProxy>();
        var capture = (DiagnosticsCaptureProxy)inner;
        await using WebApplication app = await StartAppAsync(
            inner,
            static options =>
            {
                options.Mode = CSharpDbRemoteSecurityMode.ApiKey;
                options.ApiKey = ApiKey;
                options.AllowSensitiveQueryDetailAccess = true;
            });
        using HttpClient httpClient = app.GetTestClient();
        await using ICSharpDbClient client = CSharpDbClient.Create(
            new CSharpDbClientOptions
            {
                Transport = ClientTransport.Http,
                Endpoint = httpClient.BaseAddress!.ToString(),
                HttpClient = httpClient,
                ApiKey = ApiKey,
            });
        var diagnostics = Assert.IsAssignableFrom<ICSharpDbObservabilityClient>(
            client);

        DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot> runtime =
            await diagnostics.GetRuntimeDiagnosticsAsync(Ct);
        DiagnosticsTopologySnapshot<DiagnosticsValueSnapshot<StorageRuntimeDiagnosticsSnapshot>> storage =
            await diagnostics.GetStorageDiagnosticsAsync(Ct);
        DiagnosticsTopologySnapshot<DiagnosticsValueSnapshot<WalRuntimeDiagnosticsSnapshot>> wal =
            await diagnostics.GetWalDiagnosticsAsync(Ct);
        DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<ActiveQuerySnapshot>> active =
            await diagnostics.GetActiveQueriesAsync(7, Ct);
        DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<RecentQuerySnapshot>> recent =
            await diagnostics.GetRecentQueriesAsync(8, Ct);
        DiagnosticsTopologySnapshot<DiagnosticsValueSnapshot<QueryPlanDiagnosticsSnapshot>> plan =
            await diagnostics.GetQueryPlanDiagnosticsAsync(OperationId, Ct);
        DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<SessionDiagnosticsSnapshot>> sessions =
            await diagnostics.GetSessionsAsync(9, Ct);
        DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<MaintenanceOperationSnapshot>> activeMaintenance =
            await diagnostics.GetActiveMaintenanceOperationsAsync(10, Ct);
        DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<MaintenanceOperationSnapshot>> recentMaintenance =
            await diagnostics.GetRecentMaintenanceOperationsAsync(11, Ct);
        DiagnosticsTopologySnapshot<DiagnosticsValueSnapshot<QueryDetailSnapshot>> detail =
            await diagnostics.GetQueryDetailAsync(OperationId, Ct);

        Assert.All(
            new IRuntimeDiagnosticsSnapshot[]
            {
                runtime, storage, wal, active, recent, plan, sessions,
                activeMaintenance, recentMaintenance, detail,
            },
            snapshot =>
            {
                Assert.Equal(DiagnosticsAvailability.Disabled, snapshot.Metadata.Availability);
                Assert.Equal("http-test", snapshot.Metadata.DatabaseAlias);
            });
        Assert.Equal(10, capture.InvocationCount);
        Assert.Equal([7, 8, 9, 10, 11], capture.MaximumRecords);
        Assert.Equal([OperationId, OperationId], capture.OperationIds);
        Assert.True(capture.AllCallsWereCanceledCapable);
        Assert.True(capture.AllCallsWereDiagnosticsSuppressed);
    }

    [Fact]
    public async Task DiagnosticsPolicy_RequiresKey_AndSeparatelyAuthorizesDetail()
    {
        IDiagnosticsCaptureClient inner =
            DispatchProxy.Create<IDiagnosticsCaptureClient, DiagnosticsCaptureProxy>();
        await using WebApplication app = await StartAppAsync(
            inner,
            static options =>
            {
                options.Mode = CSharpDbRemoteSecurityMode.ApiKey;
                options.ApiKey = ApiKey;
                options.AllowSensitiveQueryDetailAccess = false;
            });
        using HttpClient httpClient = app.GetTestClient();

        using HttpResponseMessage missingKey = await httpClient.GetAsync(
            "/api/diagnostics/runtime",
            Ct);
        Assert.Equal(HttpStatusCode.Unauthorized, missingKey.StatusCode);

        using var runtimeRequest = new HttpRequestMessage(
            HttpMethod.Get,
            "/api/diagnostics/runtime");
        runtimeRequest.Headers.TryAddWithoutValidation(
            CSharpDbApiSecurityOptions.DefaultApiKeyHeaderName,
            ApiKey);
        using HttpResponseMessage runtime = await httpClient.SendAsync(
            runtimeRequest,
            Ct);
        Assert.Equal(HttpStatusCode.OK, runtime.StatusCode);

        using var detailRequest = new HttpRequestMessage(
            HttpMethod.Get,
            $"/api/diagnostics/queries/{OperationId.Value}/detail");
        detailRequest.Headers.TryAddWithoutValidation(
            CSharpDbApiSecurityOptions.DefaultApiKeyHeaderName,
            ApiKey);
        using HttpResponseMessage detail = await httpClient.SendAsync(
            detailRequest,
            Ct);
        Assert.Equal(HttpStatusCode.Forbidden, detail.StatusCode);

        await using ICSharpDbClient missingKeyClient = CSharpDbClient.Create(
            new CSharpDbClientOptions
            {
                Transport = ClientTransport.Http,
                Endpoint = httpClient.BaseAddress!.ToString(),
                HttpClient = httpClient,
            });
        var missingKeyDiagnostics = Assert.IsAssignableFrom<
            ICSharpDbObservabilityClient>(missingKeyClient);
        CSharpDbObservabilityAccessDeniedException unauthenticated =
            await Assert.ThrowsAsync<CSharpDbObservabilityAccessDeniedException>(
                () => missingKeyDiagnostics.GetRuntimeDiagnosticsAsync(Ct));

        await using ICSharpDbClient detailDeniedClient = CSharpDbClient.Create(
            new CSharpDbClientOptions
            {
                Transport = ClientTransport.Http,
                Endpoint = httpClient.BaseAddress.ToString(),
                HttpClient = httpClient,
                ApiKey = ApiKey,
            });
        var detailDeniedDiagnostics = Assert.IsAssignableFrom<
            ICSharpDbObservabilityClient>(detailDeniedClient);
        CSharpDbObservabilityAccessDeniedException forbidden =
            await Assert.ThrowsAsync<CSharpDbObservabilityAccessDeniedException>(
                () => detailDeniedDiagnostics.GetQueryDetailAsync(OperationId, Ct));

        Assert.All(
            new[] { unauthenticated, forbidden },
            error =>
            {
                Assert.Equal(
                    CSharpDbObservabilityAccessDeniedException.SafeMessage,
                    error.Message);
                Assert.DoesNotContain(ApiKey, error.Message, StringComparison.Ordinal);
                Assert.Null(error.InnerException);
            });
    }

    [Theory]
    [InlineData("127.0.0.1", false, HttpStatusCode.OK)]
    [InlineData("203.0.113.10", false, HttpStatusCode.Forbidden)]
    [InlineData("203.0.113.10", true, HttpStatusCode.OK)]
    public async Task DiagnosticsPolicy_NoneMode_UsesTheProvenPeerAddress(
        string remoteAddress,
        bool allowInsecureRemote,
        HttpStatusCode expectedStatus)
    {
        IDiagnosticsCaptureClient inner =
            DispatchProxy.Create<IDiagnosticsCaptureClient, DiagnosticsCaptureProxy>();
        await using WebApplication app = await StartAppAsync(
            inner,
            options =>
            {
                options.Mode = CSharpDbRemoteSecurityMode.None;
                options.AllowInsecureRemoteDiagnostics = allowInsecureRemote;
            },
            remoteAddress: IPAddress.Parse(remoteAddress));
        using HttpClient httpClient = app.GetTestClient();

        using HttpResponseMessage response = await httpClient.GetAsync(
            "/api/diagnostics/runtime",
            Ct);

        Assert.Equal(expectedStatus, response.StatusCode);
    }

    [Fact]
    public async Task DiagnosticsAuthorization_PrecedesMissingBoundsAndMalformedIds()
    {
        IDiagnosticsCaptureClient inner =
            DispatchProxy.Create<IDiagnosticsCaptureClient, DiagnosticsCaptureProxy>();
        await using WebApplication app = await StartAppAsync(
            inner,
            static options =>
            {
                options.Mode = CSharpDbRemoteSecurityMode.None;
                options.AllowInsecureRemoteDiagnostics = false;
            },
            remoteAddress: IPAddress.Parse("203.0.113.20"));
        using HttpClient httpClient = app.GetTestClient();

        using HttpResponseMessage missingLimit = await httpClient.GetAsync(
            "/api/diagnostics/queries/active",
            Ct);
        using HttpResponseMessage malformedLimit = await httpClient.GetAsync(
            "/api/diagnostics/queries/active?maximumRecords=not-a-number",
            Ct);
        using HttpResponseMessage malformedMaintenanceLimit =
            await httpClient.GetAsync(
                "/api/diagnostics/maintenance/active?maximumRecords=not-a-number",
                Ct);
        using HttpResponseMessage malformedId = await httpClient.GetAsync(
            "/api/diagnostics/queries/not-an-operation-id/plan",
            Ct);

        Assert.Equal(HttpStatusCode.Forbidden, missingLimit.StatusCode);
        Assert.Equal(HttpStatusCode.Forbidden, malformedLimit.StatusCode);
        Assert.Equal(
            HttpStatusCode.Forbidden,
            malformedMaintenanceLimit.StatusCode);
        Assert.Equal(HttpStatusCode.Forbidden, malformedId.StatusCode);
        Assert.Equal(0, ((DiagnosticsCaptureProxy)inner).InvocationCount);
    }

    [Fact]
    public async Task DiagnosticsServer_RejectsMissingOrInvalidInputsBeforeCapabilityInvocation()
    {
        IDiagnosticsCaptureClient inner =
            DispatchProxy.Create<IDiagnosticsCaptureClient, DiagnosticsCaptureProxy>();
        await using WebApplication app = await StartAppAsync(
            inner,
            static options =>
            {
                options.Mode = CSharpDbRemoteSecurityMode.ApiKey;
                options.ApiKey = ApiKey;
            });
        using HttpClient httpClient = app.GetTestClient();
        httpClient.DefaultRequestHeaders.TryAddWithoutValidation(
            CSharpDbApiSecurityOptions.DefaultApiKeyHeaderName,
            ApiKey);

        using HttpResponseMessage missingLimit = await httpClient.GetAsync(
            "/api/diagnostics/queries/recent",
            Ct);
        using HttpResponseMessage malformedLimit = await httpClient.GetAsync(
            "/api/diagnostics/queries/recent?maximumRecords=not-a-number",
            Ct);
        using HttpResponseMessage excessiveLimit = await httpClient.GetAsync(
            $"/api/diagnostics/sessions?maximumRecords={CSharpDbObservabilityOptions.MaximumHistoryCapacity + 1}",
            Ct);
        using HttpResponseMessage excessiveMaintenanceLimit =
            await httpClient.GetAsync(
                $"/api/diagnostics/maintenance/recent?maximumRecords={CSharpDbObservabilityOptions.MaximumHistoryCapacity + 1}",
                Ct);
        using HttpResponseMessage malformedId = await httpClient.GetAsync(
            "/api/diagnostics/queries/not-an-operation-id/plan",
            Ct);

        Assert.Equal(HttpStatusCode.BadRequest, missingLimit.StatusCode);
        Assert.Equal(HttpStatusCode.BadRequest, malformedLimit.StatusCode);
        Assert.Equal(HttpStatusCode.BadRequest, excessiveLimit.StatusCode);
        Assert.Equal(
            HttpStatusCode.BadRequest,
            excessiveMaintenanceLimit.StatusCode);
        Assert.Equal(HttpStatusCode.BadRequest, malformedId.StatusCode);
        Assert.Equal(0, ((DiagnosticsCaptureProxy)inner).InvocationCount);
    }

    [Fact]
    public async Task CustomRoutePrefix_MapsTheDiagnosticsSubtree()
    {
        IDiagnosticsCaptureClient inner =
            DispatchProxy.Create<IDiagnosticsCaptureClient, DiagnosticsCaptureProxy>();
        await using WebApplication app = await StartAppAsync(
            inner,
            static options =>
            {
                options.Mode = CSharpDbRemoteSecurityMode.ApiKey;
                options.ApiKey = ApiKey;
            },
            configureHost: static options => options.RoutePrefix = "/db");
        using HttpClient httpClient = app.GetTestClient();
        using var request = new HttpRequestMessage(
            HttpMethod.Get,
            "/db/diagnostics/runtime");
        request.Headers.TryAddWithoutValidation(
            CSharpDbApiSecurityOptions.DefaultApiKeyHeaderName,
            ApiKey);

        using HttpResponseMessage response = await httpClient.SendAsync(
            request,
            Ct);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal(1, ((DiagnosticsCaptureProxy)inner).InvocationCount);
    }

    [Fact]
    public async Task DisabledObservability_DoesNotResolveTheHostRequestTracker()
    {
        int contributorResolutions = 0;
        IDiagnosticsCaptureClient inner =
            DispatchProxy.Create<IDiagnosticsCaptureClient, DiagnosticsCaptureProxy>();
        await using WebApplication app = await StartAppAsync(
            inner,
            static options =>
            {
                options.Mode = CSharpDbRemoteSecurityMode.ApiKey;
                options.ApiKey = ApiKey;
            },
            configureServices: services =>
            {
                services.AddSingleton(new CSharpDbObservabilityOptions
                {
                    Enabled = false,
                });
                services.AddSingleton<ICSharpDbHostRequestDiagnosticsContributor>(
                    _ =>
                    {
                        Interlocked.Increment(ref contributorResolutions);
                        throw new InvalidOperationException(
                            "A disabled tracker must remain unresolved.");
                    });
            });
        using HttpClient httpClient = app.GetTestClient();
        using var request = new HttpRequestMessage(
            HttpMethod.Get,
            "/api/diagnostics/sessions?maximumRecords=4");
        request.Headers.TryAddWithoutValidation(
            CSharpDbApiSecurityOptions.DefaultApiKeyHeaderName,
            ApiKey);

        using HttpResponseMessage response = await httpClient.SendAsync(
            request,
            Ct);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal(0, contributorResolutions);
    }

    [Fact]
    public async Task Sessions_IncludeInFlightHttpRequests_ButNotTheDiagnosticsRequest()
    {
        var entered = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        IDiagnosticsCaptureClient inner =
            DispatchProxy.Create<IDiagnosticsCaptureClient, DiagnosticsCaptureProxy>();
        ((DiagnosticsCaptureProxy)inner).ReturnAvailableSessions = true;
        await using WebApplication app = await StartAppAsync(
            inner,
            static options =>
            {
                options.Mode = CSharpDbRemoteSecurityMode.ApiKey;
                options.ApiKey = ApiKey;
            },
            configureServices: static services =>
            {
                services.AddSingleton(new CSharpDbObservabilityOptions
                {
                    Enabled = true,
                });
                services.AddSingleton(new CSharpDbHostRequestDiagnostics(16));
                services.AddSingleton<ICSharpDbHostRequestDiagnosticsContributor>(
                    provider => provider.GetRequiredService<
                        CSharpDbHostRequestDiagnostics>());
            },
            configureApp: app => app.MapGet(
                "/api/hold-diagnostics-session",
                async (HttpContext context) =>
                {
                    entered.TrySetResult();
                    await release.Task.WaitAsync(context.RequestAborted);
                    return Results.NoContent();
                }));
        using HttpClient httpClient = app.GetTestClient();
        await using ICSharpDbClient client = CSharpDbClient.Create(
            new CSharpDbClientOptions
            {
                Transport = ClientTransport.Http,
                Endpoint = httpClient.BaseAddress!.ToString(),
                HttpClient = httpClient,
                ApiKey = ApiKey,
            });
        var diagnostics = Assert.IsAssignableFrom<ICSharpDbObservabilityClient>(
            client);

        using var heldMessage = new HttpRequestMessage(
            HttpMethod.Get,
            "/api/hold-diagnostics-session");
        heldMessage.Headers.TryAddWithoutValidation(
            CSharpDbApiSecurityOptions.DefaultApiKeyHeaderName,
            ApiKey);
        Task<HttpResponseMessage> heldRequest = httpClient.SendAsync(
            heldMessage,
            Ct);
        try
        {
            await entered.Task.WaitAsync(Ct);
            DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<
                SessionDiagnosticsSnapshot>> sessions =
                await diagnostics.GetSessionsAsync(8, Ct);

            SessionDiagnosticsSnapshot session = Assert.Single(
                sessions.Aggregate.Records!);
            Assert.Equal(
                CSharpDB.Observability.CSharpDbTransport.Http,
                session.Transport);
            Assert.Equal(DiagnosticsSessionState.Active, session.State);
        }
        finally
        {
            release.TrySetResult();
            using HttpResponseMessage heldResponse = await heldRequest;
        }
    }

    [Fact]
    public void SessionResponseCapping_DoesNotInflateCumulativeDroppedCount()
    {
        var tracker = new CSharpDbHostRequestDiagnostics(1);
        using IDisposable request = tracker.TryBeginRequest(
            new OpaqueDiagnosticsId("11111111111111111111111111111111"),
            CSharpDB.Observability.CSharpDbTransport.Http,
            currentOperationId: null)!;
        DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<
            SessionDiagnosticsSnapshot>> databaseSessions =
                AvailableSessions(capacity: 1, includeExistingRecord: true);

        DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<
            SessionDiagnosticsSnapshot>> capped =
                CSharpDbHostRequestDiagnosticsProjection.MergeSessions(
                    databaseSessions,
                    tracker,
                    maximumRecords: 1);

        Assert.Single(capped.Aggregate.Records!);
        Assert.True(capped.Aggregate.IsTruncated);
        Assert.True(capped.Aggregate.Metadata.RecordsTruncated);
        Assert.Equal(0, capped.Aggregate.DroppedCount);

        Assert.Null(tracker.TryBeginRequest(
            new OpaqueDiagnosticsId("22222222222222222222222222222222"),
            CSharpDB.Observability.CSharpDbTransport.Http,
            currentOperationId: null));
        DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<
            SessionDiagnosticsSnapshot>> withRegistryDrop =
                CSharpDbHostRequestDiagnosticsProjection.MergeSessions(
                    databaseSessions,
                    tracker,
                    maximumRecords: 1);

        Assert.Equal(1, withRegistryDrop.Aggregate.DroppedCount);
        Assert.True(withRegistryDrop.Aggregate.IsTruncated);
    }

    [Fact]
    public void SessionResponseCapping_BoundsAggregateAndAllShardRecordsGlobally()
    {
        var tracker = new CSharpDbHostRequestDiagnostics(1);
        using IDisposable request = tracker.TryBeginRequest(
            new OpaqueDiagnosticsId("11111111111111111111111111111111"),
            CSharpDB.Observability.CSharpDbTransport.Http,
            currentOperationId: null)!;
        DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<
            SessionDiagnosticsSnapshot>> databaseSessions = ShardedSessions(
                includeAggregateRecord: false,
                ("beta", RecordCount: 1, IdBase: 200),
                ("alpha", RecordCount: 1, IdBase: 100));

        DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<
            SessionDiagnosticsSnapshot>> capped =
                CSharpDbHostRequestDiagnosticsProjection.MergeSessions(
                    databaseSessions,
                    tracker,
                    maximumRecords: 1);

        Assert.Equal(1, CountSessionRecords(capped));
        SessionDiagnosticsSnapshot host = Assert.Single(capped.Aggregate.Records!);
        Assert.Equal(DiagnosticsSessionState.Active, host.State);
        Assert.Equal(
            "11111111111111111111111111111111",
            host.SessionId.Value);
        Assert.False(capped.Aggregate.IsTruncated);
        Assert.Equal(0, capped.Aggregate.DroppedCount);
        Assert.False(capped.ShardsTruncated);
        Assert.All(capped.Shards!, shard =>
        {
            DiagnosticsCollectionSnapshot<SessionDiagnosticsSnapshot> child =
                Assert.IsType<DiagnosticsCollectionSnapshot<SessionDiagnosticsSnapshot>>(
                    shard.Value);
            Assert.Empty(child.Records!);
            Assert.True(child.IsTruncated);
            Assert.True(child.Metadata.RecordsTruncated);
            Assert.Equal(0, child.DroppedCount);
            Assert.Equal(
                databaseSessions.Shards!
                    .Single(original => original.ShardAlias == shard.ShardAlias)
                    .Value!.Metadata.ServerInstanceId,
                child.Metadata.ServerInstanceId);
        });
    }

    [Fact]
    public void SessionResponseCapping_RebudgetsShardsByStableAliasQuotientAndRemainder()
    {
        var tracker = new CSharpDbHostRequestDiagnostics(1);
        using IDisposable request = tracker.TryBeginRequest(
            new OpaqueDiagnosticsId("11111111111111111111111111111111"),
            CSharpDB.Observability.CSharpDbTransport.Http,
            currentOperationId: null)!;
        DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<
            SessionDiagnosticsSnapshot>> databaseSessions = ShardedSessions(
                includeAggregateRecord: true,
                ("gamma", RecordCount: 3, IdBase: 300),
                ("alpha", RecordCount: 3, IdBase: 100),
                ("beta", RecordCount: 3, IdBase: 200));

        DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<
            SessionDiagnosticsSnapshot>> capped =
                CSharpDbHostRequestDiagnosticsProjection.MergeSessions(
                    databaseSessions,
                    tracker,
                    maximumRecords: 6);

        Assert.Equal(6, CountSessionRecords(capped));
        Assert.Equal(
            [
                "11111111111111111111111111111111",
                "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
            ],
            capped.Aggregate.Records!
                .Select(static record => record.SessionId.Value)
                .ToArray());
        Assert.False(capped.Aggregate.IsTruncated);
        Assert.Equal(0, capped.Aggregate.DroppedCount);
        Assert.Equal(
            ["gamma", "alpha", "beta"],
            capped.Shards!.Select(static shard => shard.ShardAlias).ToArray());

        IReadOnlyDictionary<string, ShardDiagnosticsSection<
            DiagnosticsCollectionSnapshot<SessionDiagnosticsSnapshot>>> shards =
                capped.Shards!.ToDictionary(
                    static shard => shard.ShardAlias,
                    StringComparer.Ordinal);
        Assert.Equal(
            [CreateOpaqueId(100).Value, CreateOpaqueId(101).Value],
            shards["alpha"].Value!.Records!
                .Select(static record => record.SessionId.Value)
                .ToArray());
        Assert.Equal(
            [CreateOpaqueId(200).Value],
            shards["beta"].Value!.Records!
                .Select(static record => record.SessionId.Value)
                .ToArray());
        Assert.Equal(
            [CreateOpaqueId(300).Value],
            shards["gamma"].Value!.Records!
                .Select(static record => record.SessionId.Value)
                .ToArray());
        Assert.All(shards.Values, static shard =>
        {
            Assert.True(shard.Value!.IsTruncated);
            Assert.Equal(0, shard.Value.DroppedCount);
        });
        Assert.False(capped.ShardsTruncated);
    }

    [Fact]
    public async Task CapabilityArgumentFailures_DoNotLeakDetailsOverHttp()
    {
        const string canary = "http-private-capability-argument";
        IDiagnosticsCaptureClient inner =
            DispatchProxy.Create<IDiagnosticsCaptureClient, DiagnosticsCaptureProxy>();
        ((DiagnosticsCaptureProxy)inner).RuntimeFailure =
            new ArgumentException(canary);
        await using WebApplication app = await StartAppAsync(
            inner,
            static options =>
            {
                options.Mode = CSharpDbRemoteSecurityMode.ApiKey;
                options.ApiKey = ApiKey;
            });
        using HttpClient httpClient = app.GetTestClient();
        using var request = new HttpRequestMessage(
            HttpMethod.Get,
            "/api/diagnostics/runtime");
        request.Headers.TryAddWithoutValidation(
            CSharpDbApiSecurityOptions.DefaultApiKeyHeaderName,
            ApiKey);

        using HttpResponseMessage response = await httpClient.SendAsync(
            request,
            Ct);
        string payload = await response.Content.ReadAsStringAsync(Ct);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.DoesNotContain(canary, payload, StringComparison.Ordinal);
        Assert.Contains("The request is invalid.", payload, StringComparison.Ordinal);
    }

    [Fact]
    public async Task MissingOptionalCapability_ReturnsNotImplemented_AndHttpClientMapsIt()
    {
        ICSharpDbClient inner =
            DispatchProxy.Create<ICSharpDbClient, UnsupportedClientProxy>();
        await using WebApplication app = await StartAppAsync(
            inner,
            static options =>
            {
                options.Mode = CSharpDbRemoteSecurityMode.ApiKey;
                options.ApiKey = ApiKey;
            });
        using HttpClient httpClient = app.GetTestClient();
        await using ICSharpDbClient client = CSharpDbClient.Create(
            new CSharpDbClientOptions
            {
                Transport = ClientTransport.Http,
                Endpoint = httpClient.BaseAddress!.ToString(),
                HttpClient = httpClient,
                ApiKey = ApiKey,
            });
        var diagnostics = Assert.IsAssignableFrom<ICSharpDbObservabilityClient>(
            client);

        var error = await Assert.ThrowsAsync<
            CSharpDbObservabilityNotSupportedException>(
            () => diagnostics.GetRuntimeDiagnosticsAsync(Ct));
        Assert.Equal(
            CSharpDbObservabilityNotSupportedException.SafeMessage,
            error.Message);
    }

    [Fact]
    public async Task HttpClient_ValidatesBoundsBeforeSendingRequest()
    {
        var handler = new CountingHandler();
        using var httpClient = new HttpClient(handler)
        {
            BaseAddress = new Uri("http://localhost/"),
        };
        await using ICSharpDbClient client = CSharpDbClient.Create(
            new CSharpDbClientOptions
            {
                Transport = ClientTransport.Http,
                Endpoint = httpClient.BaseAddress.ToString(),
                HttpClient = httpClient,
            });
        var diagnostics = Assert.IsAssignableFrom<ICSharpDbObservabilityClient>(
            client);

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            diagnostics.GetActiveQueriesAsync(0, Ct));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            diagnostics.GetRecentQueriesAsync(
                CSharpDbObservabilityOptions.MaximumHistoryCapacity + 1,
                Ct));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            diagnostics.GetSessionsAsync(-1, Ct));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            diagnostics.GetActiveMaintenanceOperationsAsync(0, Ct));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            diagnostics.GetRecentMaintenanceOperationsAsync(
                CSharpDbObservabilityOptions.MaximumHistoryCapacity + 1,
                Ct));
        Assert.Equal(0, handler.SendCount);
    }

    [Theory]
    [InlineData(HttpStatusCode.NotFound)]
    [InlineData(HttpStatusCode.NotImplemented)]
    public async Task HttpClient_MapsOnlyDiagnosticsCapabilityStatusCodesToUnsupported(
        HttpStatusCode statusCode)
    {
        using var httpClient = new HttpClient(new StatusHandler(statusCode))
        {
            BaseAddress = new Uri("http://localhost/"),
        };
        await using ICSharpDbClient client = CSharpDbClient.Create(
            new CSharpDbClientOptions
            {
                Transport = ClientTransport.Http,
                Endpoint = httpClient.BaseAddress.ToString(),
                HttpClient = httpClient,
            });
        var diagnostics = Assert.IsAssignableFrom<ICSharpDbObservabilityClient>(
            client);

        await Assert.ThrowsAsync<CSharpDbObservabilityNotSupportedException>(
            () => diagnostics.GetRuntimeDiagnosticsAsync(Ct));
    }

    [Theory]
    [InlineData(HttpStatusCode.Unauthorized)]
    [InlineData(HttpStatusCode.Forbidden)]
    public async Task HttpClient_MapsOnlyDiagnosticsAccessStatusCodesToSafeDenied(
        HttpStatusCode statusCode)
    {
        const string remoteCanary =
            "remote-secret-sql-path-C:/private/diagnostics.db";
        using var httpClient = new HttpClient(
            new StatusHandler(statusCode, remoteCanary))
        {
            BaseAddress = new Uri("http://localhost/"),
        };
        await using ICSharpDbClient client = CSharpDbClient.Create(
            new CSharpDbClientOptions
            {
                Transport = ClientTransport.Http,
                Endpoint = httpClient.BaseAddress.ToString(),
                HttpClient = httpClient,
            });
        var diagnostics = Assert.IsAssignableFrom<ICSharpDbObservabilityClient>(
            client);

        CSharpDbObservabilityAccessDeniedException error =
            await Assert.ThrowsAsync<CSharpDbObservabilityAccessDeniedException>(
                () => diagnostics.GetRuntimeDiagnosticsAsync(Ct));

        Assert.Equal(CSharpDbObservabilityAccessDeniedException.SafeMessage, error.Message);
        Assert.DoesNotContain(remoteCanary, error.Message, StringComparison.Ordinal);
        Assert.Null(error.InnerException);
    }

    [Fact]
    public async Task HttpClient_DoesNotMapOrdinaryNotFoundToDiagnosticsUnsupported()
    {
        using var httpClient = new HttpClient(
            new StatusHandler(HttpStatusCode.NotFound))
        {
            BaseAddress = new Uri("http://localhost/"),
        };
        await using ICSharpDbClient client = CSharpDbClient.Create(
            new CSharpDbClientOptions
            {
                Transport = ClientTransport.Http,
                Endpoint = httpClient.BaseAddress.ToString(),
                HttpClient = httpClient,
            });

        CSharpDbClientException error = await Assert.ThrowsAsync<
            CSharpDbClientException>(() => client.GetInfoAsync(Ct));

        Assert.IsNotType<CSharpDbObservabilityNotSupportedException>(error);
    }

    [Theory]
    [InlineData(HttpStatusCode.Unauthorized)]
    [InlineData(HttpStatusCode.Forbidden)]
    public async Task HttpClient_DoesNotMapOrdinaryAccessFailuresToDiagnosticsDenied(
        HttpStatusCode statusCode)
    {
        using var httpClient = new HttpClient(new StatusHandler(statusCode))
        {
            BaseAddress = new Uri("http://localhost/"),
        };
        await using ICSharpDbClient client = CSharpDbClient.Create(
            new CSharpDbClientOptions
            {
                Transport = ClientTransport.Http,
                Endpoint = httpClient.BaseAddress.ToString(),
                HttpClient = httpClient,
            });

        CSharpDbClientException error = await Assert.ThrowsAsync<
            CSharpDbClientException>(() => client.GetInfoAsync(Ct));

        Assert.IsNotType<CSharpDbObservabilityAccessDeniedException>(error);
    }

    private static CancellationToken Ct =>
        TestContext.Current.CancellationToken;

    private static async Task<WebApplication> StartAppAsync(
        ICSharpDbClient client,
        Action<CSharpDbApiSecurityOptions> configureSecurity,
        Action<CSharpDbRestApiHostOptions>? configureHost = null,
        Action<IServiceCollection>? configureServices = null,
        Action<WebApplication>? configureApp = null,
        IPAddress? remoteAddress = null)
    {
        WebApplicationBuilder builder = WebApplication.CreateBuilder(
            new WebApplicationOptions
            {
                EnvironmentName = "Testing",
            });
        builder.WebHost.UseTestServer();
        builder.Services.AddCSharpDbRestApi(configureSecurity);
        builder.Services.AddSingleton(client);
        configureServices?.Invoke(builder.Services);

        WebApplication app = builder.Build();
        if (remoteAddress is not null)
        {
            app.Use((context, next) =>
            {
                context.Connection.RemoteIpAddress = remoteAddress;
                return next(context);
            });
        }
        app.MapCSharpDbRestApi(configureHost);
        configureApp?.Invoke(app);
        await app.StartAsync(Ct);
        return app;
    }

    private static DiagnosticsSnapshotMetadata DisabledMetadata()
        => new(
            CSharpDbDiagnostics.SchemaVersion,
            new DateTimeOffset(2026, 8, 10, 12, 0, 0, TimeSpan.Zero),
            "abcdef0123456789abcdef0123456789",
            counterEpoch: 0,
            DiagnosticsScope.Instance,
            DiagnosticsAvailability.Disabled,
            DiagnosticsSource.Client,
            "http-test",
            recordsTruncated: false,
            fieldsTruncated: false);

    private static DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot>
        DisabledRuntime()
    {
        DiagnosticsSnapshotMetadata metadata = DisabledMetadata();
        var runtime = new RuntimeDiagnosticsSnapshot(
            metadata,
            DiagnosticsSection<QueryDiagnosticsSummary>.WithoutValue(
                DiagnosticsAvailability.Disabled),
            DiagnosticsSection<ConnectionDiagnosticsSnapshot>.WithoutValue(
                DiagnosticsAvailability.Disabled),
            DiagnosticsSection<StorageRuntimeDiagnosticsSnapshot>.WithoutValue(
                DiagnosticsAvailability.Disabled),
            DiagnosticsSection<WalRuntimeDiagnosticsSnapshot>.WithoutValue(
                DiagnosticsAvailability.Disabled),
            DiagnosticsSection<MaintenanceOperationSnapshot>.WithoutValue(
                DiagnosticsAvailability.Disabled),
            DiagnosticsSection<HealthDiagnosticsSnapshot>.WithoutValue(
                DiagnosticsAvailability.Disabled));
        return new DiagnosticsTopologySnapshot<RuntimeDiagnosticsSnapshot>(
            runtime, null, null, null, null);
    }

    private static DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<T>>
        DisabledCollection<T>()
        where T : class, IRuntimeDiagnosticsSnapshot
    {
        DiagnosticsSnapshotMetadata metadata = DisabledMetadata();
        var collection = new DiagnosticsCollectionSnapshot<T>(
            metadata, null, null, null, null, null);
        return new DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<T>>(
            collection, null, null, null, null);
    }

    private static DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<
        SessionDiagnosticsSnapshot>> AvailableSessions(
            int capacity,
            bool includeExistingRecord = false)
    {
        DiagnosticsSnapshotMetadata metadata = new(
            CSharpDbDiagnostics.SchemaVersion,
            new DateTimeOffset(2026, 8, 10, 12, 0, 0, TimeSpan.Zero),
            "abcdef0123456789abcdef0123456789",
            counterEpoch: 0,
            DiagnosticsScope.Instance,
            DiagnosticsAvailability.Available,
            DiagnosticsSource.Client,
            "http-test",
            recordsTruncated: false,
            fieldsTruncated: false);
        IReadOnlyList<SessionDiagnosticsSnapshot> records = includeExistingRecord
            ?
            [
                new SessionDiagnosticsSnapshot(
                    metadata,
                    new OpaqueDiagnosticsId(
                        "33333333333333333333333333333333"),
                    metadata.CapturedAtUtc,
                    metadata.CapturedAtUtc,
                    CurrentOperationId: null,
                    HasActiveReader: false,
                    HasActiveTransaction: false,
                    Transport: CSharpDB.Observability.CSharpDbTransport.Embedded)
                {
                    State = DiagnosticsSessionState.Idle,
                },
            ]
            : [];
        var collection = new DiagnosticsCollectionSnapshot<
            SessionDiagnosticsSnapshot>(
                metadata,
                records,
                capacity,
                retention: null,
                droppedCount: 0,
                isTruncated: false);
        return new DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<
            SessionDiagnosticsSnapshot>>(
                collection,
                null,
                null,
                null,
                null);
    }

    private static DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<
        SessionDiagnosticsSnapshot>> ShardedSessions(
            bool includeAggregateRecord,
            params (string Alias, int RecordCount, int IdBase)[] shardDefinitions)
    {
        DateTimeOffset capturedAtUtc =
            new(2026, 8, 10, 12, 0, 0, TimeSpan.Zero);
        DiagnosticsSnapshotMetadata aggregateMetadata = new(
            CSharpDbDiagnostics.SchemaVersion,
            capturedAtUtc,
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            counterEpoch: 7,
            DiagnosticsScope.Aggregate,
            DiagnosticsAvailability.Available,
            DiagnosticsSource.Client,
            "sharded-http-test",
            recordsTruncated: false,
            fieldsTruncated: false);
        IReadOnlyList<SessionDiagnosticsSnapshot> aggregateRecords =
            includeAggregateRecord
                ?
                [
                    CreateSession(
                        aggregateMetadata,
                        new OpaqueDiagnosticsId(
                            "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")),
                ]
                : [];
        var aggregate = new DiagnosticsCollectionSnapshot<SessionDiagnosticsSnapshot>(
            aggregateMetadata,
            aggregateRecords,
            capacity: 16,
            retention: null,
            droppedCount: 0,
            isTruncated: false);

        ShardDiagnosticsSection<DiagnosticsCollectionSnapshot<
            SessionDiagnosticsSnapshot>>[] shards = shardDefinitions
                .Select((definition, index) =>
                {
                    DiagnosticsSnapshotMetadata metadata = new(
                        CSharpDbDiagnostics.SchemaVersion,
                        capturedAtUtc.AddSeconds(index + 1),
                        CreateOpaqueId(1000 + index).Value,
                        counterEpoch: index + 10,
                        DiagnosticsScope.Shard,
                        DiagnosticsAvailability.Available,
                        DiagnosticsSource.Client,
                        definition.Alias,
                        recordsTruncated: false,
                        fieldsTruncated: false);
                    SessionDiagnosticsSnapshot[] records = Enumerable
                        .Range(0, definition.RecordCount)
                        .Select(recordIndex => CreateSession(
                            metadata,
                            CreateOpaqueId(definition.IdBase + recordIndex)))
                        .ToArray();
                    var collection = new DiagnosticsCollectionSnapshot<
                        SessionDiagnosticsSnapshot>(
                            metadata,
                            records,
                            capacity: Math.Max(1, definition.RecordCount),
                            retention: null,
                            droppedCount: 0,
                            isTruncated: false);
                    return new ShardDiagnosticsSection<DiagnosticsCollectionSnapshot<
                        SessionDiagnosticsSnapshot>>(
                            definition.Alias,
                            DiagnosticsAvailability.Available,
                            collection);
                })
                .ToArray();
        return new DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<
            SessionDiagnosticsSnapshot>>(
                aggregate,
                shards,
                shardCapacity: shardDefinitions.Length,
                droppedShardCount: 0,
                shardsTruncated: false);
    }

    private static SessionDiagnosticsSnapshot CreateSession(
        DiagnosticsSnapshotMetadata metadata,
        OpaqueDiagnosticsId sessionId)
        => new(
            metadata,
            sessionId,
            metadata.CapturedAtUtc,
            metadata.CapturedAtUtc,
            CurrentOperationId: null,
            HasActiveReader: false,
            HasActiveTransaction: false,
            CSharpDB.Observability.CSharpDbTransport.Direct)
        {
            State = DiagnosticsSessionState.Idle,
        };

    private static OpaqueDiagnosticsId CreateOpaqueId(int value)
        => new(value.ToString("x32"));

    private static int CountSessionRecords(
        DiagnosticsTopologySnapshot<DiagnosticsCollectionSnapshot<
            SessionDiagnosticsSnapshot>> topology)
        => (topology.Aggregate.Records?.Count ?? 0) +
           (topology.Shards?.Sum(static shard => shard.Value?.Records?.Count ?? 0) ?? 0);

    private static DiagnosticsTopologySnapshot<DiagnosticsValueSnapshot<T>>
        DisabledValue<T>()
        where T : class, IRuntimeDiagnosticsSnapshot
    {
        DiagnosticsSnapshotMetadata metadata = DisabledMetadata();
        var value = new DiagnosticsValueSnapshot<T>(metadata, null);
        return new DiagnosticsTopologySnapshot<DiagnosticsValueSnapshot<T>>(
            value, null, null, null, null);
    }

    public interface IDiagnosticsCaptureClient :
        ICSharpDbClient,
        ICSharpDbObservabilityClient;

    public class DiagnosticsCaptureProxy : DispatchProxy
    {
        private readonly List<int> _maximumRecords = [];
        private readonly List<OpaqueDiagnosticsId> _operationIds = [];

        public int InvocationCount { get; private set; }
        public IReadOnlyList<int> MaximumRecords => _maximumRecords;
        public IReadOnlyList<OpaqueDiagnosticsId> OperationIds => _operationIds;
        public bool ReturnAvailableSessions { get; set; }
        public Exception? RuntimeFailure { get; set; }
        public bool AllCallsWereCanceledCapable { get; private set; } = true;
        public bool AllCallsWereDiagnosticsSuppressed { get; private set; } = true;

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            string? name = targetMethod?.Name;
            if (name == "get_DataSource")
                return "diagnostics-http-capture";
            if (name == "DisposeAsync")
                return ValueTask.CompletedTask;

            InvocationCount++;
            CancellationToken cancellationToken = (CancellationToken)args![^1]!;
            AllCallsWereCanceledCapable &= cancellationToken.CanBeCanceled;
            AllCallsWereDiagnosticsSuppressed &=
                CSharpDbOperationScope.IsDiagnosticsSuppressed;
            return name switch
            {
                "GetRuntimeDiagnosticsAsync" when RuntimeFailure is not null =>
                    Task.FromException<DiagnosticsTopologySnapshot<
                        RuntimeDiagnosticsSnapshot>>(RuntimeFailure),
                "GetRuntimeDiagnosticsAsync" => Task.FromResult(
                    DisabledRuntime()),
                "GetStorageDiagnosticsAsync" => Task.FromResult(
                    DisabledValue<StorageRuntimeDiagnosticsSnapshot>()),
                "GetWalDiagnosticsAsync" => Task.FromResult(
                    DisabledValue<WalRuntimeDiagnosticsSnapshot>()),
                "GetActiveQueriesAsync" => CaptureMaximum(
                    (int)args[0]!,
                    DisabledCollection<ActiveQuerySnapshot>()),
                "GetRecentQueriesAsync" => CaptureMaximum(
                    (int)args[0]!,
                    DisabledCollection<RecentQuerySnapshot>()),
                "GetQueryPlanDiagnosticsAsync" => CaptureOperation(
                    (OpaqueDiagnosticsId)args[0]!,
                    DisabledValue<QueryPlanDiagnosticsSnapshot>()),
                "GetSessionsAsync" => CaptureMaximum(
                    (int)args[0]!,
                    ReturnAvailableSessions
                        ? AvailableSessions((int)args[0]!)
                        : DisabledCollection<SessionDiagnosticsSnapshot>()),
                "GetActiveMaintenanceOperationsAsync" => CaptureMaximum(
                    (int)args[0]!,
                    DisabledCollection<MaintenanceOperationSnapshot>()),
                "GetRecentMaintenanceOperationsAsync" => CaptureMaximum(
                    (int)args[0]!,
                    DisabledCollection<MaintenanceOperationSnapshot>()),
                "GetQueryDetailAsync" => CaptureOperation(
                    (OpaqueDiagnosticsId)args[0]!,
                    DisabledValue<QueryDetailSnapshot>()),
                _ => throw new NotSupportedException(name),
            };
        }

        private Task<T> CaptureMaximum<T>(int maximumRecords, T value)
        {
            _maximumRecords.Add(maximumRecords);
            return Task.FromResult(value);
        }

        private Task<T> CaptureOperation<T>(
            OpaqueDiagnosticsId operationId,
            T value)
        {
            _operationIds.Add(operationId);
            return Task.FromResult(value);
        }
    }

    public class UnsupportedClientProxy : DispatchProxy
    {
        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
            => targetMethod?.Name switch
            {
                "get_DataSource" => "unsupported-diagnostics-client",
                "DisposeAsync" => ValueTask.CompletedTask,
                _ => throw new NotSupportedException(targetMethod?.Name),
            };
    }

    private sealed class CountingHandler : HttpMessageHandler
    {
        public int SendCount { get; private set; }

        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            SendCount++;
            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK));
        }
    }

    private sealed class StatusHandler(
        HttpStatusCode statusCode,
        string? responseBody = null) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            var response = new HttpResponseMessage(statusCode);
            if (responseBody is not null)
                response.Content = new StringContent(responseBody);
            return Task.FromResult(response);
        }
    }
}
