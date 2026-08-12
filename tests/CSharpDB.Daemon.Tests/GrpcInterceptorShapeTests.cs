extern alias CSharpDbApi;

using CSharpDB.Client;
using CSharpDB.Daemon.Grpc;
using CSharpDB.Observability;
using Grpc.Core;
using Microsoft.Extensions.Options;
using CSharpDbApiSecurityOptions =
    CSharpDbApi::CSharpDB.Api.Security.CSharpDbApiSecurityOptions;
using CSharpDbRemoteSecurityMode =
    CSharpDbApi::CSharpDB.Api.Security.CSharpDbRemoteSecurityMode;

namespace CSharpDB.Daemon.Tests;

public sealed class GrpcInterceptorShapeTests
{
    private const string HealthCheck = "/grpc.health.v1.Health/Check";
    private const string HealthWatch = "/grpc.health.v1.Health/Watch";
    private const string NormalMethod = "/csharpdb.rpc.CSharpDbRpc/GetInfo";

    [Theory]
    [InlineData(HealthCheck)]
    [InlineData(HealthWatch)]
    public async Task ApiKeyInterceptor_ExemptsOnlyExactStandardHealthMethods(
        string method)
    {
        var interceptor = CreateApiKeyInterceptor();
        ServerCallContext context = TestServerCallContext.Create(method);
        int invoked = 0;

        string unary = await interceptor.UnaryServerHandler(
            "request",
            context,
            (_, _) =>
            {
                invoked++;
                return Task.FromResult("unary");
            });
        string clientStreaming = await interceptor.ClientStreamingServerHandler(
            EmptyAsyncStreamReader<string>.Instance,
            context,
            (_, _) =>
            {
                invoked++;
                return Task.FromResult("client");
            });
        await interceptor.ServerStreamingServerHandler(
            "request",
            NullServerStreamWriter<string>.Instance,
            context,
            (_, _, _) =>
            {
                invoked++;
                return Task.CompletedTask;
            });
        await interceptor.DuplexStreamingServerHandler(
            EmptyAsyncStreamReader<string>.Instance,
            NullServerStreamWriter<string>.Instance,
            context,
            (_, _, _) =>
            {
                invoked++;
                return Task.CompletedTask;
            });

        Assert.Equal("unary", unary);
        Assert.Equal("client", clientStreaming);
        Assert.Equal(4, invoked);
    }

    [Theory]
    [InlineData(NormalMethod)]
    [InlineData(HealthCheck + "Internal")]
    [InlineData("/custom.Health/Check")]
    public async Task ApiKeyInterceptor_ProtectsEveryRpcShape(string method)
    {
        var interceptor = CreateApiKeyInterceptor();
        ServerCallContext context = TestServerCallContext.Create(method);
        int invoked = 0;

        RpcException unary = await Assert.ThrowsAsync<RpcException>(() =>
            interceptor.UnaryServerHandler(
                "request",
                context,
                (_, _) =>
                {
                    invoked++;
                    return Task.FromResult("unary");
                }));
        RpcException client = await Assert.ThrowsAsync<RpcException>(() =>
            interceptor.ClientStreamingServerHandler(
                EmptyAsyncStreamReader<string>.Instance,
                context,
                (_, _) =>
                {
                    invoked++;
                    return Task.FromResult("client");
                }));
        RpcException server = await Assert.ThrowsAsync<RpcException>(() =>
            interceptor.ServerStreamingServerHandler(
                "request",
                NullServerStreamWriter<string>.Instance,
                context,
                (_, _, _) =>
                {
                    invoked++;
                    return Task.CompletedTask;
                }));
        RpcException duplex = await Assert.ThrowsAsync<RpcException>(() =>
            interceptor.DuplexStreamingServerHandler(
                EmptyAsyncStreamReader<string>.Instance,
                NullServerStreamWriter<string>.Instance,
                context,
                (_, _, _) =>
                {
                    invoked++;
                    return Task.CompletedTask;
                }));

        Assert.All(
            new[] { unary, client, server, duplex },
            exception => Assert.Equal(
                StatusCode.Unauthenticated,
                exception.StatusCode));
        Assert.Equal(0, invoked);
    }

    [Theory]
    [InlineData(HealthCheck)]
    [InlineData(HealthWatch)]
    public async Task OperationAndRouteInterceptors_BypassHealthAcrossAllShapes(
        string method)
    {
        var accessor = new CSharpDbRouteContextAccessor
        {
            Current = new CSharpDbRouteContext
            {
                Keyspace = "outer",
                Key = "outer-key",
            },
        };
        CSharpDbRouteContext expected = accessor.Current!;
        var route = new CSharpDbRouteContextGrpcInterceptor(accessor);
        var operation = new CSharpDbOperationScopeGrpcInterceptor(
            EnabledOptions());
        ServerCallContext context = TestServerCallContext.Create(
            method,
            new Metadata
            {
                { CSharpDbRouteHeaderNames.GrpcKeyspace, "ignored" },
            });
        int invoked = 0;

        await route.UnaryServerHandler(
            "request",
            context,
            async (request, callContext) =>
            {
                await operation.UnaryServerHandler(
                    request,
                    callContext,
                    (_, _) =>
                    {
                        Assert.Null(CSharpDbOperationScope.Current);
                        Assert.Same(expected, accessor.Current);
                        invoked++;
                        return Task.FromResult("ok");
                    });
                return "ok";
            });
        await route.ClientStreamingServerHandler(
            EmptyAsyncStreamReader<string>.Instance,
            context,
            async (stream, callContext) =>
            {
                _ = await operation.ClientStreamingServerHandler(
                    stream,
                    callContext,
                    (_, _) =>
                    {
                        Assert.Null(CSharpDbOperationScope.Current);
                        Assert.Same(expected, accessor.Current);
                        invoked++;
                        return Task.FromResult("ok");
                    });
                return "ok";
            });
        await route.ServerStreamingServerHandler(
            "request",
            NullServerStreamWriter<string>.Instance,
            context,
            (request, writer, callContext) =>
                operation.ServerStreamingServerHandler(
                    request,
                    writer,
                    callContext,
                    (_, _, _) =>
                    {
                        Assert.Null(CSharpDbOperationScope.Current);
                        Assert.Same(expected, accessor.Current);
                        invoked++;
                        return Task.CompletedTask;
                    }));
        await route.DuplexStreamingServerHandler(
            EmptyAsyncStreamReader<string>.Instance,
            NullServerStreamWriter<string>.Instance,
            context,
            (reader, writer, callContext) =>
                operation.DuplexStreamingServerHandler(
                    reader,
                    writer,
                    callContext,
                    (_, _, _) =>
                    {
                        Assert.Null(CSharpDbOperationScope.Current);
                        Assert.Same(expected, accessor.Current);
                        invoked++;
                        return Task.CompletedTask;
                    }));

        Assert.Equal(4, invoked);
        Assert.Same(expected, accessor.Current);
    }

    [Fact]
    public async Task OperationAndRouteInterceptors_ApplyAndRestoreAcrossAllShapes()
    {
        var outer = new CSharpDbRouteContext
        {
            Keyspace = "outer",
            Key = "outer-key",
        };
        var accessor = new CSharpDbRouteContextAccessor { Current = outer };
        var route = new CSharpDbRouteContextGrpcInterceptor(accessor);
        var operation = new CSharpDbOperationScopeGrpcInterceptor(
            EnabledOptions());
        ServerCallContext context = TestServerCallContext.Create(
            NormalMethod,
            new Metadata
            {
                { CSharpDbRouteHeaderNames.GrpcKeyspace, "tenant" },
                { CSharpDbRouteHeaderNames.GrpcShardKey, "customer-42" },
            });
        int invoked = 0;

        void AssertActive()
        {
            Assert.Equal(
                CSharpDB.Observability.CSharpDbTransport.Grpc,
                CSharpDbOperationScope.CurrentTransport);
            Assert.Equal("tenant", accessor.Current?.Keyspace);
            Assert.Equal("customer-42", accessor.Current?.Key);
            invoked++;
        }

        await route.UnaryServerHandler(
            "request",
            context,
            async (request, callContext) =>
            {
                _ = await operation.UnaryServerHandler(
                    request,
                    callContext,
                    (_, _) =>
                    {
                        AssertActive();
                        return Task.FromResult("ok");
                    });
                return "ok";
            });
        await route.ClientStreamingServerHandler(
            EmptyAsyncStreamReader<string>.Instance,
            context,
            async (reader, callContext) =>
            {
                _ = await operation.ClientStreamingServerHandler(
                    reader,
                    callContext,
                    (_, _) =>
                    {
                        AssertActive();
                        return Task.FromResult("ok");
                    });
                return "ok";
            });
        await route.ServerStreamingServerHandler(
            "request",
            NullServerStreamWriter<string>.Instance,
            context,
            (request, writer, callContext) =>
                operation.ServerStreamingServerHandler(
                    request,
                    writer,
                    callContext,
                    (_, _, _) =>
                    {
                        AssertActive();
                        return Task.CompletedTask;
                    }));
        await route.DuplexStreamingServerHandler(
            EmptyAsyncStreamReader<string>.Instance,
            NullServerStreamWriter<string>.Instance,
            context,
            (reader, writer, callContext) =>
                operation.DuplexStreamingServerHandler(
                    reader,
                    writer,
                    callContext,
                    (_, _, _) =>
                    {
                        AssertActive();
                        return Task.CompletedTask;
                    }));

        Assert.Equal(4, invoked);
        Assert.Same(outer, accessor.Current);
        Assert.Equal(
            CSharpDB.Observability.CSharpDbTransport.Embedded,
            CSharpDbOperationScope.CurrentTransport);
    }

    private static CSharpDbApiKeyGrpcInterceptor CreateApiKeyInterceptor()
        => new(Options.Create(new CSharpDbApiSecurityOptions
        {
            Mode = CSharpDbRemoteSecurityMode.ApiKey,
            ApiKey = "secret",
        }));

    private static CSharpDbObservabilityOptions EnabledOptions()
        => new()
        {
            Enabled = true,
            Logging = new CSharpDbLoggingOptions { Enabled = true },
        };
}

internal sealed class TestServerCallContext : ServerCallContext
{
    private readonly string _method;
    private readonly Metadata _requestHeaders;
    private readonly CancellationToken _cancellationToken;
    private readonly Metadata _responseTrailers = [];
    private Status _status;
    private WriteOptions? _writeOptions;

    private TestServerCallContext(
        string method,
        Metadata? requestHeaders,
        CancellationToken cancellationToken)
    {
        _method = method;
        _requestHeaders = requestHeaders ?? [];
        _cancellationToken = cancellationToken;
    }

    internal static TestServerCallContext Create(
        string method,
        Metadata? requestHeaders = null)
        => new(method, requestHeaders, CancellationToken.None);

    internal static TestServerCallContext CreateWithCancellation(
        string method,
        Metadata? requestHeaders,
        CancellationToken cancellationToken)
        => new(method, requestHeaders, cancellationToken);

    protected override string MethodCore => _method;
    protected override string HostCore => "localhost";
    protected override string PeerCore => "ipv4:127.0.0.1:12345";
    protected override DateTime DeadlineCore => DateTime.MaxValue;
    protected override Metadata RequestHeadersCore => _requestHeaders;
    protected override CancellationToken CancellationTokenCore =>
        _cancellationToken;
    protected override Metadata ResponseTrailersCore => _responseTrailers;
    protected override Status StatusCore
    {
        get => _status;
        set => _status = value;
    }
    protected override WriteOptions? WriteOptionsCore
    {
        get => _writeOptions;
        set => _writeOptions = value;
    }
    protected override AuthContext AuthContextCore =>
        new(string.Empty, new Dictionary<string, List<AuthProperty>>());
    protected override ContextPropagationToken CreatePropagationTokenCore(
        ContextPropagationOptions? options)
        => throw new NotSupportedException();
    protected override Task WriteResponseHeadersAsyncCore(
        Metadata responseHeaders)
        => Task.CompletedTask;
}

internal sealed class EmptyAsyncStreamReader<T> : IAsyncStreamReader<T>
{
    internal static EmptyAsyncStreamReader<T> Instance { get; } = new();
    public T Current => throw new InvalidOperationException();
    public Task<bool> MoveNext(CancellationToken cancellationToken)
        => Task.FromResult(false);
}

internal sealed class NullServerStreamWriter<T> : IServerStreamWriter<T>
{
    internal static NullServerStreamWriter<T> Instance { get; } = new();
    public WriteOptions? WriteOptions { get; set; }
    public Task WriteAsync(T message) => Task.CompletedTask;
    public Task WriteAsync(T message, CancellationToken cancellationToken)
        => Task.CompletedTask;
}
