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
               !IsInfrastructureMethod(context?.Method)
            ? InvokeWithScopeAsync(request, context!, continuation)
            : continuation(request, context!);
    }

    public override Task<TResponse> ClientStreamingServerHandler<TRequest, TResponse>(
        IAsyncStreamReader<TRequest> requestStream,
        ServerCallContext context,
        ClientStreamingServerMethod<TRequest, TResponse> continuation)
    {
        return _enabled &&
               !CSharpDbOperationScope.IsDiagnosticsSuppressed &&
               !IsInfrastructureMethod(context?.Method)
            ? InvokeWithScopeAsync(requestStream, context!, continuation)
            : continuation(requestStream, context!);
    }

    public override Task ServerStreamingServerHandler<TRequest, TResponse>(
        TRequest request,
        IServerStreamWriter<TResponse> responseStream,
        ServerCallContext context,
        ServerStreamingServerMethod<TRequest, TResponse> continuation)
    {
        return _enabled &&
               !CSharpDbOperationScope.IsDiagnosticsSuppressed &&
               !IsInfrastructureMethod(context?.Method)
            ? InvokeWithScopeAsync(
                request,
                responseStream,
                context!,
                continuation)
            : continuation(request, responseStream, context!);
    }

    public override Task DuplexStreamingServerHandler<TRequest, TResponse>(
        IAsyncStreamReader<TRequest> requestStream,
        IServerStreamWriter<TResponse> responseStream,
        ServerCallContext context,
        DuplexStreamingServerMethod<TRequest, TResponse> continuation)
    {
        return _enabled &&
               !CSharpDbOperationScope.IsDiagnosticsSuppressed &&
               !IsInfrastructureMethod(context?.Method)
            ? InvokeWithScopeAsync(
                requestStream,
                responseStream,
                context!,
                continuation)
            : continuation(requestStream, responseStream, context!);
    }

    private async Task<TResponse> InvokeWithScopeAsync<TRequest, TResponse>(
        TRequest request,
        ServerCallContext context,
        UnaryServerMethod<TRequest, TResponse> continuation)
        where TRequest : class
        where TResponse : class
    {
        ScopeLease lease = TryEnterScope();

        try
        {
            return await continuation(request, context).ConfigureAwait(false);
        }
        finally
        {
            lease.Dispose();
        }
    }

    private async Task<TResponse> InvokeWithScopeAsync<TRequest, TResponse>(
        IAsyncStreamReader<TRequest> requestStream,
        ServerCallContext context,
        ClientStreamingServerMethod<TRequest, TResponse> continuation)
        where TRequest : class
        where TResponse : class
    {
        ScopeLease lease = TryEnterScope();
        try
        {
            return await continuation(requestStream, context)
                .ConfigureAwait(false);
        }
        finally
        {
            lease.Dispose();
        }
    }

    private async Task InvokeWithScopeAsync<TRequest, TResponse>(
        TRequest request,
        IServerStreamWriter<TResponse> responseStream,
        ServerCallContext context,
        ServerStreamingServerMethod<TRequest, TResponse> continuation)
        where TRequest : class
        where TResponse : class
    {
        ScopeLease lease = TryEnterScope();
        try
        {
            await continuation(request, responseStream, context)
                .ConfigureAwait(false);
        }
        finally
        {
            lease.Dispose();
        }
    }

    private async Task InvokeWithScopeAsync<TRequest, TResponse>(
        IAsyncStreamReader<TRequest> requestStream,
        IServerStreamWriter<TResponse> responseStream,
        ServerCallContext context,
        DuplexStreamingServerMethod<TRequest, TResponse> continuation)
        where TRequest : class
        where TResponse : class
    {
        ScopeLease lease = TryEnterScope();
        try
        {
            await continuation(requestStream, responseStream, context)
                .ConfigureAwait(false);
        }
        finally
        {
            lease.Dispose();
        }
    }

    private ScopeLease TryEnterScope()
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

        return new ScopeLease(scope, requestLease);
    }

    internal static bool IsDiagnosticsMethod(string? method)
        => CSharpDbGrpcMethodPolicy.IsDiagnosticsMethod(method);

    private static bool IsInfrastructureMethod(string? method)
        => CSharpDbGrpcMethodPolicy.IsHealthMethod(method) ||
           CSharpDbGrpcMethodPolicy.IsDiagnosticsMethod(method);

    private readonly struct ScopeLease(
        IDisposable? scope,
        IDisposable? requestLease) : IDisposable
    {
        public void Dispose()
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
}
