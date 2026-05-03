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
        CSharpDbApiSecurityOptions security = options.Value;
        string headerName = CSharpDbApiKeyValidator.NormalizeHeaderName(security.ApiKeyHeaderName, forGrpc: true);
        string? suppliedApiKey = context.RequestHeaders.GetValue(headerName);

        if (!CSharpDbApiKeyValidator.IsAuthorized(security, suppliedApiKey))
            throw new RpcException(new Status(StatusCode.Unauthenticated, UnauthenticatedDetail));

        return await continuation(request, context).ConfigureAwait(false);
    }
}
