using CSharpDB.Observability;
using Grpc.Core;
using Grpc.Core.Interceptors;

namespace CSharpDB.Daemon.Grpc;

/// <summary>
/// Marks database work initiated by a gRPC call while preserving the inbound
/// server activity established by ASP.NET Core.
/// </summary>
public sealed class CSharpDbOperationScopeGrpcInterceptor : Interceptor
{
    private readonly bool _loggingEnabled;

    public CSharpDbOperationScopeGrpcInterceptor(CSharpDbObservabilityOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        _loggingEnabled = options.Enabled && options.Logging?.Enabled == true;
    }

    public override Task<TResponse> UnaryServerHandler<TRequest, TResponse>(
        TRequest request,
        ServerCallContext context,
        UnaryServerMethod<TRequest, TResponse> continuation)
    {
        return _loggingEnabled
            ? InvokeWithScopeAsync(request, context, continuation)
            : continuation(request, context);
    }

    private static async Task<TResponse> InvokeWithScopeAsync<TRequest, TResponse>(
        TRequest request,
        ServerCallContext context,
        UnaryServerMethod<TRequest, TResponse> continuation)
        where TRequest : class
        where TResponse : class
    {
        IDisposable? scope = null;
        try
        {
            scope = CSharpDbOperationScope.EnterTransport(
                CSharpDbTransport.Grpc,
                OpaqueDiagnosticsId.Create());
        }
        catch
        {
            // Diagnostics context must never affect RPC execution.
        }

        try
        {
            return await continuation(request, context).ConfigureAwait(false);
        }
        finally
        {
            try
            {
                scope?.Dispose();
            }
            catch
            {
                // Diagnostics context must never affect RPC completion.
            }
        }
    }
}
