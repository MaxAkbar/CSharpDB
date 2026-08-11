extern alias CSharpDbApi;

using CSharpDB.Daemon.Grpc;
using CSharpDB.Observability;
using Grpc.Core;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using CSharpDbHostRequestDiagnostics =
    CSharpDbApi::CSharpDB.Api.Diagnostics.CSharpDbHostRequestDiagnostics;
using HostRequestDiagnosticsRawCollection =
    CSharpDbApi::CSharpDB.Api.Diagnostics.HostRequestDiagnosticsRawCollection;
using HostRequestDiagnosticsRawSnapshot =
    CSharpDbApi::CSharpDB.Api.Diagnostics.HostRequestDiagnosticsRawSnapshot;
using CSharpDbOperationScopeMiddleware =
    CSharpDbApi::CSharpDB.Api.Middleware.CSharpDbOperationScopeMiddleware;

namespace CSharpDB.Daemon.Tests;

[Collection("ObservabilityDiagnostics")]
public sealed class HostRequestDiagnosticsGrpcTests
{
    [Fact]
    public async Task EnabledLoggingOffGrpcRequest_IsVisibleWithBoundarySessionAndCleansUp()
    {
        CSharpDbObservabilityOptions options = CreateOptions(loggingEnabled: false);
        var tracker = new CSharpDbHostRequestDiagnostics(capacity: 4);
        using ServiceProvider services = CreateServices(tracker);
        var interceptor = new CSharpDbOperationScopeGrpcInterceptor(options, services);
        OpaqueDiagnosticsId? boundarySessionId = null;

        string response = await interceptor.UnaryServerHandler<string, string>(
            "request",
            null!,
            (request, _) =>
            {
                Assert.Equal("request", request);
                Assert.Equal(CSharpDbTransport.Grpc, CSharpDbOperationScope.CurrentTransport);
                boundarySessionId = CSharpDbOperationScope.CurrentSessionId;
                HostRequestDiagnosticsRawSnapshot record =
                    Assert.Single(tracker.Capture().Records);
                Assert.Equal(boundarySessionId, record.SessionId);
                Assert.Equal(CSharpDbTransport.Grpc, record.Transport);
                Assert.NotEqual(default, record.CreatedAtUtc);
                Assert.True(record.LastActiveAtUtc >= record.CreatedAtUtc);
                return Task.FromResult("response");
            });

        Assert.Equal("response", response);
        Assert.NotNull(boundarySessionId);
        Assert.Empty(tracker.Capture().Records);
        Assert.Equal(CSharpDbTransport.Embedded, CSharpDbOperationScope.CurrentTransport);
        Assert.Null(CSharpDbOperationScope.CurrentSessionId);
    }

    [Fact]
    public async Task GrpcRequest_CleansUpAfterFailureAndCancellation()
    {
        CSharpDbObservabilityOptions options = CreateOptions(loggingEnabled: false);
        var tracker = new CSharpDbHostRequestDiagnostics(capacity: 4);
        using ServiceProvider services = CreateServices(tracker);
        var interceptor = new CSharpDbOperationScopeGrpcInterceptor(options, services);
        var expected = new InvalidOperationException("downstream");

        InvalidOperationException actual = await Assert.ThrowsAsync<InvalidOperationException>(
            () => interceptor.UnaryServerHandler<string, string>(
                "request",
                null!,
                (_, _) => Task.FromException<string>(expected)));

        Assert.Same(expected, actual);
        Assert.Empty(tracker.Capture().Records);

        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => interceptor.UnaryServerHandler<string, string>(
                "request",
                null!,
                (_, _) => Task.FromCanceled<string>(cancellation.Token)));

        Assert.Empty(tracker.Capture().Records);
    }

    [Fact]
    public void DisabledGrpcRequest_DoesNotResolveTrackerOrCreateScope()
    {
        int trackerFactoryCalls = 0;
        var services = new ServiceCollection();
        services.AddSingleton(_ =>
        {
            trackerFactoryCalls++;
            return new CSharpDbHostRequestDiagnostics(capacity: 1);
        });
        using ServiceProvider provider = services.BuildServiceProvider();
        Task<string> continuationTask = Task.FromResult("response");
        var interceptor = new CSharpDbOperationScopeGrpcInterceptor(
            new CSharpDbObservabilityOptions(),
            provider);

        Task<string> returned = interceptor.UnaryServerHandler<string, string>(
            "request",
            null!,
            (_, _) =>
            {
                Assert.Equal(
                    CSharpDbTransport.Embedded,
                    CSharpDbOperationScope.CurrentTransport);
                Assert.Null(CSharpDbOperationScope.CurrentSessionId);
                return continuationTask;
            });

        Assert.Same(continuationTask, returned);
        Assert.Equal(0, trackerFactoryCalls);
    }

    [Theory]
    [InlineData("/csharpdb.rpc.CSharpDbRpc/GetRuntimeDiagnostics")]
    [InlineData("/csharpdb.rpc.CSharpDbRpc/GetActiveQueries")]
    [InlineData("/csharpdb.rpc.CSharpDbRpc/GetRecentQueries")]
    [InlineData("/csharpdb.rpc.CSharpDbRpc/GetQueryPlanDiagnostics")]
    [InlineData("/csharpdb.rpc.CSharpDbRpc/GetSessions")]
    [InlineData("/csharpdb.rpc.CSharpDbRpc/GetQueryDetail")]
    public void DiagnosticsGrpcMethods_AreExplicitlySuppressed(string method)
    {
        Assert.True(CSharpDbOperationScopeGrpcInterceptor.IsDiagnosticsMethod(method));
        Assert.False(CSharpDbOperationScopeGrpcInterceptor.IsDiagnosticsMethod(
            "/csharpdb.rpc.CSharpDbRpc/ExecuteSql"));
        Assert.False(CSharpDbOperationScopeGrpcInterceptor.IsDiagnosticsMethod(
            method + "Internal"));
    }

    [Fact]
    public async Task HttpAndGrpcBoundaries_UseOneSharedTrackerWithDistinctSessions()
    {
        CSharpDbObservabilityOptions options = CreateOptions(loggingEnabled: false);
        var tracker = new CSharpDbHostRequestDiagnostics(capacity: 4);
        using ServiceProvider services = CreateServices(tracker);
        var grpc = new CSharpDbOperationScopeGrpcInterceptor(options, services);
        OpaqueDiagnosticsId? httpSession = null;
        OpaqueDiagnosticsId? grpcSession = null;
        async Task HandleHttpAsync(HttpContext _)
        {
            httpSession = CSharpDbOperationScope.CurrentSessionId;
            await grpc.UnaryServerHandler<string, string>(
                "request",
                null!,
                (_, _) =>
                {
                    grpcSession = CSharpDbOperationScope.CurrentSessionId;
                    HostRequestDiagnosticsRawCollection capture = tracker.Capture();
                    Assert.Equal(2, capture.Records.Count);
                    Assert.Contains(
                        capture.Records,
                        item => item.SessionId == httpSession &&
                                item.Transport == CSharpDbTransport.Http);
                    Assert.Contains(
                        capture.Records,
                        item => item.SessionId == grpcSession &&
                                item.Transport == CSharpDbTransport.Grpc);
                    return Task.FromResult("response");
                });
        }

        var http = new CSharpDbOperationScopeMiddleware(
            HandleHttpAsync,
            options,
            services);

        await http.InvokeAsync(new DefaultHttpContext());

        Assert.NotNull(httpSession);
        Assert.NotNull(grpcSession);
        Assert.NotEqual(httpSession, grpcSession);
        Assert.Empty(tracker.Capture().Records);
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
