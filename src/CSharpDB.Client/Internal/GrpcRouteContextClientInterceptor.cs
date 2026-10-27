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
        return continuation(request, WithHeaders(context));
    }

    public override AsyncDuplexStreamingCall<TRequest, TResponse> AsyncDuplexStreamingCall<TRequest, TResponse>(
        ClientInterceptorContext<TRequest, TResponse> context,
        AsyncDuplexStreamingCallContinuation<TRequest, TResponse> continuation)
        => continuation(WithHeaders(context));

    private ClientInterceptorContext<TRequest, TResponse> WithHeaders<TRequest, TResponse>(
        ClientInterceptorContext<TRequest, TResponse> context) where TRequest : class where TResponse : class
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
        return new ClientInterceptorContext<TRequest, TResponse>(
            context.Method,
            context.Host,
            options);
    }
}
