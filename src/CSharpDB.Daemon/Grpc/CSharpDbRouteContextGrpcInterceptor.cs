using CSharpDB.Client;
using Grpc.Core;
using Grpc.Core.Interceptors;

namespace CSharpDB.Daemon.Grpc;

public sealed class CSharpDbRouteContextGrpcInterceptor(ICSharpDbRouteContextAccessor routeContextAccessor) : Interceptor
{
    public override Task<TResponse> UnaryServerHandler<TRequest, TResponse>(
        TRequest request,
        ServerCallContext context,
        UnaryServerMethod<TRequest, TResponse> continuation)
    {
        return CSharpDbGrpcMethodPolicy.IsHealthMethod(context.Method)
            ? continuation(request, context)
            : InvokeWithRouteContextAsync(request, context, continuation);
    }

    public override Task<TResponse> ClientStreamingServerHandler<TRequest, TResponse>(
        IAsyncStreamReader<TRequest> requestStream,
        ServerCallContext context,
        ClientStreamingServerMethod<TRequest, TResponse> continuation)
    {
        return CSharpDbGrpcMethodPolicy.IsHealthMethod(context.Method)
            ? continuation(requestStream, context)
            : InvokeWithRouteContextAsync(requestStream, context, continuation);
    }

    public override Task ServerStreamingServerHandler<TRequest, TResponse>(
        TRequest request,
        IServerStreamWriter<TResponse> responseStream,
        ServerCallContext context,
        ServerStreamingServerMethod<TRequest, TResponse> continuation)
    {
        return CSharpDbGrpcMethodPolicy.IsHealthMethod(context.Method)
            ? continuation(request, responseStream, context)
            : InvokeWithRouteContextAsync(
                request,
                responseStream,
                context,
                continuation);
    }

    public override Task DuplexStreamingServerHandler<TRequest, TResponse>(
        IAsyncStreamReader<TRequest> requestStream,
        IServerStreamWriter<TResponse> responseStream,
        ServerCallContext context,
        DuplexStreamingServerMethod<TRequest, TResponse> continuation)
    {
        return CSharpDbGrpcMethodPolicy.IsHealthMethod(context.Method)
            ? continuation(requestStream, responseStream, context)
            : InvokeWithRouteContextAsync(
                requestStream,
                responseStream,
                context,
                continuation);
    }

    private async Task<TResponse> InvokeWithRouteContextAsync<TRequest, TResponse>(
        TRequest request,
        ServerCallContext context,
        UnaryServerMethod<TRequest, TResponse> continuation)
        where TRequest : class
        where TResponse : class
    {
        CSharpDbRouteContext? previous = EnterRouteContext(context);
        try
        {
            return await continuation(request, context).ConfigureAwait(false);
        }
        finally
        {
            routeContextAccessor.Current = previous;
        }
    }

    private async Task<TResponse> InvokeWithRouteContextAsync<TRequest, TResponse>(
        IAsyncStreamReader<TRequest> requestStream,
        ServerCallContext context,
        ClientStreamingServerMethod<TRequest, TResponse> continuation)
        where TRequest : class
        where TResponse : class
    {
        CSharpDbRouteContext? previous = EnterRouteContext(context);
        try
        {
            return await continuation(requestStream, context)
                .ConfigureAwait(false);
        }
        finally
        {
            routeContextAccessor.Current = previous;
        }
    }

    private async Task InvokeWithRouteContextAsync<TRequest, TResponse>(
        TRequest request,
        IServerStreamWriter<TResponse> responseStream,
        ServerCallContext context,
        ServerStreamingServerMethod<TRequest, TResponse> continuation)
        where TRequest : class
        where TResponse : class
    {
        CSharpDbRouteContext? previous = EnterRouteContext(context);
        try
        {
            await continuation(request, responseStream, context)
                .ConfigureAwait(false);
        }
        finally
        {
            routeContextAccessor.Current = previous;
        }
    }

    private async Task InvokeWithRouteContextAsync<TRequest, TResponse>(
        IAsyncStreamReader<TRequest> requestStream,
        IServerStreamWriter<TResponse> responseStream,
        ServerCallContext context,
        DuplexStreamingServerMethod<TRequest, TResponse> continuation)
        where TRequest : class
        where TResponse : class
    {
        CSharpDbRouteContext? previous = EnterRouteContext(context);
        try
        {
            await continuation(requestStream, responseStream, context)
                .ConfigureAwait(false);
        }
        finally
        {
            routeContextAccessor.Current = previous;
        }
    }

    private CSharpDbRouteContext? EnterRouteContext(ServerCallContext context)
    {
        string? keyspace = ReadMetadata(context, CSharpDbRouteHeaderNames.GrpcKeyspace);
        string? shardKey = ReadMetadata(context, CSharpDbRouteHeaderNames.GrpcShardKey);

        if ((keyspace is null) != (shardKey is null))
        {
            throw new RpcException(new Status(
                StatusCode.InvalidArgument,
                $"Both {CSharpDbRouteHeaderNames.GrpcKeyspace} and {CSharpDbRouteHeaderNames.GrpcShardKey} metadata entries are required for sharded requests."));
        }

        CSharpDbRouteContext? previous = routeContextAccessor.Current;
        routeContextAccessor.Current = keyspace is null
            ? null
            : new CSharpDbRouteContext
            {
                Keyspace = keyspace,
                Key = shardKey!,
            };

        return previous;
    }

    private static string? ReadMetadata(ServerCallContext context, string key)
    {
        string? value = context.RequestHeaders.GetValue(key);
        return string.IsNullOrWhiteSpace(value) ? null : value.Trim();
    }
}
