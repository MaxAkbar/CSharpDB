using Grpc.Core;
using Grpc.Core.Interceptors;

namespace CSharpDB.Client.Internal;

internal sealed class GrpcRouteContextClientInterceptor(CSharpDbRouteContext routeContext) : Interceptor
{
    public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(
        TRequest request,
        ClientInterceptorContext<TRequest, TResponse> context,
        AsyncUnaryCallContinuation<TRequest, TResponse> continuation)
    {
        var metadata = new Metadata();
        if (context.Options.Headers is not null)
        {
            foreach (Metadata.Entry entry in context.Options.Headers)
            {
                if (entry.IsBinary)
                    metadata.Add(entry.Key, entry.ValueBytes);
                else
                    metadata.Add(entry.Key, entry.Value);
            }
        }

        metadata.Add(CSharpDbRouteHeaderNames.GrpcKeyspace, routeContext.Keyspace);
        metadata.Add(CSharpDbRouteHeaderNames.GrpcShardKey, routeContext.Key);

        var options = context.Options.WithHeaders(metadata);
        return continuation(request, new ClientInterceptorContext<TRequest, TResponse>(
            context.Method,
            context.Host,
            options));
    }
}
