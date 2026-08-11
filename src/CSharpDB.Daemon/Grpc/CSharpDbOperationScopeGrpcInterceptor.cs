using CSharpDB.Api.Diagnostics;
using CSharpDB.Observability;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Microsoft.Extensions.DependencyInjection;

namespace CSharpDB.Daemon.Grpc;

/// <summary>
/// Marks database work initiated by a gRPC call while preserving the inbound
/// server activity established by ASP.NET Core.
/// </summary>
public sealed class CSharpDbOperationScopeGrpcInterceptor : Interceptor
{
    private readonly bool _enabled;
    private readonly CSharpDbHostRequestDiagnostics? _requestDiagnostics;

    public CSharpDbOperationScopeGrpcInterceptor(CSharpDbObservabilityOptions options)
        : this(options, null!)
    {
    }

    [ActivatorUtilitiesConstructor]
    public CSharpDbOperationScopeGrpcInterceptor(
        CSharpDbObservabilityOptions options,
        IServiceProvider serviceProvider)
    {
        ArgumentNullException.ThrowIfNull(options);
        _enabled = options.Enabled;
        if (_enabled && serviceProvider is not null)
        {
            try
            {
                _requestDiagnostics = serviceProvider
                    .GetService<CSharpDbHostRequestDiagnostics>();
            }
            catch
            {
                // Diagnostics registration must never prevent host startup.
            }
        }
    }

    public override Task<TResponse> UnaryServerHandler<TRequest, TResponse>(
        TRequest request,
        ServerCallContext context,
        UnaryServerMethod<TRequest, TResponse> continuation)
    {
        return _enabled &&
               !CSharpDbOperationScope.IsDiagnosticsSuppressed &&
               !IsDiagnosticsMethod(context?.Method)
            ? InvokeWithScopeAsync(request, context!, continuation)
            : continuation(request, context!);
    }

    private async Task<TResponse> InvokeWithScopeAsync<TRequest, TResponse>(
        TRequest request,
        ServerCallContext context,
        UnaryServerMethod<TRequest, TResponse> continuation)
        where TRequest : class
        where TResponse : class
    {
        IDisposable? scope = null;
        IDisposable? requestLease = null;
        try
        {
            OpaqueDiagnosticsId sessionId = OpaqueDiagnosticsId.Create();
            requestLease = _requestDiagnostics?.TryBeginRequest(
                sessionId,
                CSharpDbTransport.Grpc,
                CSharpDbOperationScope.Current?.OperationId);
            scope = CSharpDbOperationScope.EnterTransport(
                CSharpDbTransport.Grpc,
                sessionId);
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
                requestLease?.Dispose();
            }
            catch
            {
                // Diagnostics state must never affect RPC completion.
            }

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

    internal static bool IsDiagnosticsMethod(string? method)
    {
        if (string.IsNullOrEmpty(method))
            return false;

        int separator = method.LastIndexOf('/');
        ReadOnlySpan<char> name = separator >= 0
            ? method.AsSpan(separator + 1)
            : method.AsSpan();
        return name.SequenceEqual("GetRuntimeDiagnostics") ||
               name.SequenceEqual("GetStorageDiagnostics") ||
               name.SequenceEqual("GetWalDiagnostics") ||
               name.SequenceEqual("GetActiveQueries") ||
               name.SequenceEqual("GetRecentQueries") ||
               name.SequenceEqual("GetQueryPlanDiagnostics") ||
               name.SequenceEqual("GetSessions") ||
               name.SequenceEqual("GetActiveMaintenanceOperations") ||
               name.SequenceEqual("GetRecentMaintenanceOperations") ||
               name.SequenceEqual("GetQueryDetail");
    }
}
