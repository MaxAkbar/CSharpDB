using CSharpDB.Client;
using Grpc.Core;
using Grpc.Core.Interceptors;

namespace CSharpDB.Daemon.Grpc;

public sealed class CSharpDbRouteContextGrpcInterceptor(ICSharpDbRouteContextAccessor routeContextAccessor) : Interceptor
{
    public override async Task<TResponse> UnaryServerHandler<TRequest, TResponse>(
        TRequest request,
        ServerCallContext context,
        UnaryServerMethod<TRequest, TResponse> continuation)
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

        try
        {
            return await continuation(request, context).ConfigureAwait(false);
        }
        finally
        {
            routeContextAccessor.Current = previous;
        }
    }

    private static string? ReadMetadata(ServerCallContext context, string key)
    {
        string? value = context.RequestHeaders.GetValue(key);
        return string.IsNullOrWhiteSpace(value) ? null : value.Trim();
    }
}
