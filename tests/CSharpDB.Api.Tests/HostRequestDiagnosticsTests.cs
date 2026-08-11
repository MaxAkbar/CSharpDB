using System.Collections.Concurrent;
using System.Text.Json;
using CSharpDB.Api.Diagnostics;
using CSharpDB.Api.Middleware;
using CSharpDB.Observability;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Client = CSharpDB.Client;

namespace CSharpDB.Api.Tests;

[Collection("ObservabilityDiagnostics")]
public sealed class HostRequestDiagnosticsTests
{
    [Fact]
    public async Task EnabledLoggingOffHttpRequest_IsVisibleWithBoundarySessionAndOperation()
    {
        const string privateCanary = "PrivateCanary42";
        CSharpDbObservabilityOptions options = CreateOptions(loggingEnabled: false);
        var tracker = new CSharpDbHostRequestDiagnostics(capacity: 4);
        using ServiceProvider services = CreateServices(tracker);
        HostRequestDiagnosticsRawCollection? captured = null;
        OpaqueDiagnosticsId? boundarySessionId = null;
        OpaqueDiagnosticsId? operationId = null;
        CSharpDbOperationContext operation = CSharpDbOperationContext.CreateRoot(
            CSharpDbOperationClass.Query,
            CSharpDbTransport.Http,
            privateCanary);
        operationId = operation.OperationId;
        var middleware = new CSharpDbOperationScopeMiddleware(
            _ =>
            {
                Assert.Equal(CSharpDbTransport.Http, CSharpDbOperationScope.CurrentTransport);
                boundarySessionId = CSharpDbOperationScope.CurrentSessionId;
                Assert.NotNull(boundarySessionId);

                captured = tracker.Capture();

                return Task.CompletedTask;
            },
            options,
            services);

        using (CSharpDbOperationScope.Enter(operation))
            await middleware.InvokeAsync(new DefaultHttpContext());

        HostRequestDiagnosticsRawSnapshot record = Assert.Single(captured!.Records);
        Assert.Equal(boundarySessionId, record.SessionId);
        Assert.Equal(operationId, record.CurrentOperationId);
        Assert.Equal(CSharpDbTransport.Http, record.Transport);
        Assert.NotEqual(default, record.CreatedAtUtc);
        Assert.True(record.LastActiveAtUtc >= record.CreatedAtUtc);
        string json = JsonSerializer.Serialize(captured);
        Assert.DoesNotContain(privateCanary, json, StringComparison.Ordinal);
        Assert.DoesNotContain("path", json, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("header", json, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("remote", json, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("bearer", json, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("sql", json, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("runtimeId", json, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("counterEpoch", json, StringComparison.OrdinalIgnoreCase);
        Assert.Empty(tracker.Capture().Records);
        Assert.Equal(CSharpDbTransport.Embedded, CSharpDbOperationScope.CurrentTransport);
        Assert.Null(CSharpDbOperationScope.CurrentSessionId);
    }

    [Fact]
    public async Task HttpRequest_CleansUpAfterFailureAndCancellation()
    {
        CSharpDbObservabilityOptions options = CreateOptions(loggingEnabled: false);
        var tracker = new CSharpDbHostRequestDiagnostics(capacity: 4);
        using ServiceProvider services = CreateServices(tracker);
        var expected = new InvalidOperationException("downstream");
        var failing = new CSharpDbOperationScopeMiddleware(
            _ => Task.FromException(expected),
            options,
            services);

        InvalidOperationException actual = await Assert.ThrowsAsync<InvalidOperationException>(
            () => failing.InvokeAsync(new DefaultHttpContext()));

        Assert.Same(expected, actual);
        Assert.Empty(tracker.Capture().Records);

        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        var canceled = new CSharpDbOperationScopeMiddleware(
            _ => Task.FromCanceled(cancellation.Token),
            options,
            services);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => canceled.InvokeAsync(new DefaultHttpContext()));

        Assert.Empty(tracker.Capture().Records);
    }

    [Fact]
    public void DisabledHttpRequest_DoesNotResolveTrackerOrCreateScope()
    {
        int trackerFactoryCalls = 0;
        var services = new ServiceCollection();
        services.AddSingleton(_ =>
        {
            trackerFactoryCalls++;
            return new CSharpDbHostRequestDiagnostics(capacity: 1);
        });
        using ServiceProvider provider = services.BuildServiceProvider();
        Task continuationTask = Task.CompletedTask;
        var middleware = new CSharpDbOperationScopeMiddleware(
            _ =>
            {
                Assert.Equal(
                    CSharpDbTransport.Embedded,
                    CSharpDbOperationScope.CurrentTransport);
                Assert.Null(CSharpDbOperationScope.CurrentSessionId);
                return continuationTask;
            },
            new CSharpDbObservabilityOptions(),
            provider);

        Task returned = middleware.InvokeAsync(new DefaultHttpContext());

        Assert.Same(continuationTask, returned);
        Assert.Equal(0, trackerFactoryCalls);
    }

    [Fact]
    public void DiagnosticsHttpRequest_IsExplicitlySuppressed()
    {
        var tracker = new CSharpDbHostRequestDiagnostics(capacity: 1);
        using ServiceProvider services = CreateServices(tracker);
        Task continuationTask = Task.CompletedTask;
        var middleware = new CSharpDbOperationScopeMiddleware(
            _ =>
            {
                Assert.Equal(
                    CSharpDbTransport.Embedded,
                    CSharpDbOperationScope.CurrentTransport);
                Assert.Null(CSharpDbOperationScope.CurrentSessionId);
                Assert.Empty(tracker.Capture().Records);
                return continuationTask;
            },
            CreateOptions(loggingEnabled: false),
            services);
        var context = new DefaultHttpContext();
        context.Request.Path = "/api/diagnostics/recent-queries";

        Task returned = middleware.InvokeAsync(context);

        Assert.Same(continuationTask, returned);
        Assert.Empty(tracker.Capture().Records);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CustomRestPrefix_SuppressesOnlyItsExactDiagnosticsSubtree(
        bool applyMiddlewareToApiOnly)
    {
        WebApplicationBuilder builder = WebApplication.CreateBuilder(
            new WebApplicationOptions { EnvironmentName = "Testing" });
        builder.WebHost.UseTestServer();
        builder.Configuration.AddInMemoryCollection(
            new Dictionary<string, string?>
            {
                ["CSharpDB:Observability:Enabled"] = "true",
                ["CSharpDB:Observability:Logging:Enabled"] = "false",
            });
        builder.Services.AddCSharpDbObservability(builder.Configuration);
        builder.Services.AddCSharpDbRestApi();
        builder.Services.AddSingleton<Client.ICSharpDbClient>(_ =>
            Client.CSharpDbClient.Create(new Client.CSharpDbClientOptions
            {
                Transport = CSharpDB.Client.CSharpDbTransport.Direct,
                ConnectionString = "Data Source=:memory:",
            }));

        await using WebApplication app = builder.Build();
        var captures = new ConcurrentQueue<HostRequestDiagnosticsRawCollection>();
        CSharpDbTransport diagnosticsTransport = CSharpDbTransport.Unknown;
        CSharpDbTransport ordinaryTransport = CSharpDbTransport.Unknown;
        OpaqueDiagnosticsId? diagnosticsSession = null;
        OpaqueDiagnosticsId? ordinarySession = null;
        CSharpDbHostRequestDiagnostics tracker = app.Services
            .GetRequiredService<CSharpDbHostRequestDiagnostics>();
        Assert.True(app.Services
            .GetRequiredService<CSharpDbObservabilityOptions>()
            .Enabled);
        app.MapCSharpDbRestApi(options =>
        {
            options.RoutePrefix = "/db";
            options.ApplyMiddlewareToApiOnly = applyMiddlewareToApiOnly;
        });
        app.MapGet(
            "/db/diagnostics/canary",
            () =>
            {
                diagnosticsTransport = CSharpDbOperationScope.CurrentTransport;
                diagnosticsSession = CSharpDbOperationScope.CurrentSessionId;
                captures.Enqueue(tracker.Capture());
            });
        app.MapGet(
            "/db/request-canary",
            () =>
            {
                ordinaryTransport = CSharpDbOperationScope.CurrentTransport;
                ordinarySession = CSharpDbOperationScope.CurrentSessionId;
                captures.Enqueue(tracker.Capture());
            });
        await app.StartAsync(TestContext.Current.CancellationToken);

        using HttpClient client = app.GetTestClient();
        using HttpResponseMessage diagnostics = await client.GetAsync(
            "/db/diagnostics/canary",
            TestContext.Current.CancellationToken);
        using HttpResponseMessage ordinary = await client.GetAsync(
            "/db/request-canary",
            TestContext.Current.CancellationToken);

        diagnostics.EnsureSuccessStatusCode();
        ordinary.EnsureSuccessStatusCode();
        HostRequestDiagnosticsRawCollection skipped = Assert.IsType<
            HostRequestDiagnosticsRawCollection>(captures.TryDequeue(out var first)
                ? first
                : null);
        HostRequestDiagnosticsRawCollection tracked = Assert.IsType<
            HostRequestDiagnosticsRawCollection>(captures.TryDequeue(out var second)
                ? second
                : null);
        Assert.Empty(skipped.Records);
        Assert.Equal(CSharpDbTransport.Embedded, diagnosticsTransport);
        Assert.Null(diagnosticsSession);
        Assert.Equal(CSharpDbTransport.Http, ordinaryTransport);
        Assert.NotNull(ordinarySession);
        Assert.Single(tracked.Records);
        Assert.Equal(CSharpDbTransport.Http, tracked.Records[0].Transport);
        Assert.Empty(tracker.Capture().Records);
    }

    [Fact]
    public async Task NestedHttpRequest_CanCaptureAndReenterWithoutLeakingSessions()
    {
        var tracker = new CSharpDbHostRequestDiagnostics(capacity: 4);
        using ServiceProvider services = CreateServices(tracker);
        CSharpDbObservabilityOptions options = CreateOptions(loggingEnabled: false);
        OpaqueDiagnosticsId? outerSession = null;
        OpaqueDiagnosticsId? innerSession = null;
        var inner = new CSharpDbOperationScopeMiddleware(
            _ =>
            {
                innerSession = CSharpDbOperationScope.CurrentSessionId;
                HostRequestDiagnosticsRawCollection capture = tracker.Capture();
                Assert.Equal(2, capture.Records.Count);
                Assert.Contains(capture.Records, item => item.SessionId == outerSession);
                Assert.Contains(capture.Records, item => item.SessionId == innerSession);
                return Task.CompletedTask;
            },
            options,
            services);
        var outer = new CSharpDbOperationScopeMiddleware(
            async _ =>
            {
                outerSession = CSharpDbOperationScope.CurrentSessionId;
                Assert.Single(tracker.Capture().Records);
                await inner.InvokeAsync(new DefaultHttpContext());
                HostRequestDiagnosticsRawSnapshot remaining =
                    Assert.Single(tracker.Capture().Records);
                Assert.Equal(outerSession, remaining.SessionId);
            },
            options,
            services);

        await outer.InvokeAsync(new DefaultHttpContext());

        Assert.NotNull(outerSession);
        Assert.NotNull(innerSession);
        Assert.NotEqual(outerSession, innerSession);
        Assert.Empty(tracker.Capture().Records);
    }

    [Fact]
    public void ConcurrentRequests_AreBoundedAndReportDropsWithoutEviction()
    {
        const int capacity = 4;
        const int requestCount = 64;
        var tracker = new CSharpDbHostRequestDiagnostics(capacity);
        var leases = new ConcurrentBag<IDisposable>();

        Parallel.For(0, requestCount, _ =>
        {
            IDisposable? lease = tracker.TryBeginRequest(
                OpaqueDiagnosticsId.Create(),
                CSharpDbTransport.Http,
                currentOperationId: null);
            if (lease is not null)
                leases.Add(lease);
        });

        HostRequestDiagnosticsRawCollection captured = tracker.Capture();
        Assert.Equal(capacity, captured.Capacity);
        Assert.Equal(capacity, captured.Records.Count);
        Assert.Equal(requestCount - capacity, captured.DroppedCount);
        Assert.True(captured.IsTruncated);
        Assert.Equal(capacity, captured.Records.Select(item => item.SessionId).Distinct().Count());

        foreach (IDisposable lease in leases)
            lease.Dispose();

        HostRequestDiagnosticsRawCollection afterCleanup = tracker.Capture();
        Assert.Empty(afterCleanup.Records);
        Assert.Equal(requestCount - capacity, afterCleanup.DroppedCount);
        Assert.True(afterCleanup.IsTruncated);
    }

    private static CSharpDbObservabilityOptions CreateOptions(bool loggingEnabled)
        => new()
        {
            Enabled = true,
            Logging = new CSharpDbLoggingOptions { Enabled = loggingEnabled },
        };

    private static ServiceProvider CreateServices(
        CSharpDbHostRequestDiagnostics tracker)
    {
        var services = new ServiceCollection();
        services.AddSingleton(tracker);
        return services.BuildServiceProvider();
    }
}
