using CSharpDB.Api.Security;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Microsoft.Extensions.Options;

namespace CSharpDB.Daemon.Grpc;

public sealed class CSharpDbApiKeyGrpcInterceptor(IOptions<CSharpDbApiSecurityOptions> options) : Interceptor
{
    private const string UnauthenticatedDetail = "A valid CSharpDB API key is required.";

    public override async Task<TResponse> UnaryServerHandler<TRequest, TResponse>(
        TRequest request,
        ServerCallContext context,
        UnaryServerMethod<TRequest, TResponse> continuation)
    {
        Authorize(context);
        return await continuation(request, context).ConfigureAwait(false);
    }

    public override async Task<TResponse> ClientStreamingServerHandler<TRequest, TResponse>(
        IAsyncStreamReader<TRequest> requestStream,
        ServerCallContext context,
        ClientStreamingServerMethod<TRequest, TResponse> continuation)
    {
        Authorize(context);
        return await continuation(requestStream, context).ConfigureAwait(false);
    }

    public override async Task ServerStreamingServerHandler<TRequest, TResponse>(
        TRequest request,
        IServerStreamWriter<TResponse> responseStream,
        ServerCallContext context,
        ServerStreamingServerMethod<TRequest, TResponse> continuation)
    {
        Authorize(context);
        await continuation(request, responseStream, context).ConfigureAwait(false);
    }

    public override async Task DuplexStreamingServerHandler<TRequest, TResponse>(
        IAsyncStreamReader<TRequest> requestStream,
        IServerStreamWriter<TResponse> responseStream,
        ServerCallContext context,
        DuplexStreamingServerMethod<TRequest, TResponse> continuation)
    {
        Authorize(context);
        await continuation(requestStream, responseStream, context).ConfigureAwait(false);
    }

    private void Authorize(ServerCallContext context)
    {
        if (CSharpDbGrpcMethodPolicy.IsHealthMethod(context.Method))
            return;

        CSharpDbApiSecurityOptions security = options.Value;
        string headerName = CSharpDbApiKeyValidator.NormalizeHeaderName(
            security.ApiKeyHeaderName,
            forGrpc: true);
        string? suppliedApiKey = context.RequestHeaders.GetValue(headerName);

        if (!CSharpDbApiKeyValidator.IsAuthorized(security, suppliedApiKey))
        {
            throw new RpcException(new Status(
                StatusCode.Unauthenticated,
                UnauthenticatedDetail));
        }
    }
}
