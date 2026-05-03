using Grpc.Core;
using Grpc.Core.Interceptors;

namespace CSharpDB.Client.Internal;

internal sealed class GrpcApiKeyClientInterceptor : Interceptor
{
    private const string DefaultApiKeyHeaderName = "X-CSharpDB-Api-Key";

    private readonly string _apiKey;
    private readonly string _apiKeyHeaderName;

    public GrpcApiKeyClientInterceptor(string apiKey, string? apiKeyHeaderName)
    {
        _apiKey = apiKey;
        _apiKeyHeaderName = NormalizeHeaderName(apiKeyHeaderName);
    }

    public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(
        TRequest request,
        ClientInterceptorContext<TRequest, TResponse> context,
        AsyncUnaryCallContinuation<TRequest, TResponse> continuation)
    {
        var metadata = new Metadata();
        if (context.Options.Headers is not null)
        {
            foreach (var entry in context.Options.Headers)
            {
                if (entry.IsBinary)
                    metadata.Add(entry.Key, entry.ValueBytes);
                else
                    metadata.Add(entry.Key, entry.Value);
            }
        }

        metadata.Add(_apiKeyHeaderName, _apiKey);
        var options = context.Options.WithHeaders(metadata);
        return continuation(request, new ClientInterceptorContext<TRequest, TResponse>(
            context.Method,
            context.Host,
            options));
    }

    private static string NormalizeHeaderName(string? headerName)
    {
        string normalized = string.IsNullOrWhiteSpace(headerName)
            ? DefaultApiKeyHeaderName
            : headerName.Trim();

        return normalized.ToLowerInvariant();
    }
}
