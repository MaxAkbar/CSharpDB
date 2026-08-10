using System.Diagnostics;
using System.Net;
using System.Runtime.ExceptionServices;
using CSharpDB.Client;
using CSharpDB.Observability;
using CSharpDB.Primitives;
using Microsoft.AspNetCore.Mvc;
using ObservabilityTransport = CSharpDB.Observability.CSharpDbTransport;

namespace CSharpDB.Api.Middleware;

public sealed class ExceptionHandlingMiddleware
{
    private readonly RequestDelegate _next;
    private readonly ILogger<ExceptionHandlingMiddleware> _logger;
    private readonly bool _typedLoggerBridgeAvailable;

    public ExceptionHandlingMiddleware(
        RequestDelegate next,
        ILogger<ExceptionHandlingMiddleware> logger,
        CSharpDbDiagnosticLoggerBridge? diagnosticLoggerBridge = null)
    {
        _next = next ?? throw new ArgumentNullException(nameof(next));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _typedLoggerBridgeAvailable = diagnosticLoggerBridge is not null;
    }

    public async Task InvokeAsync(HttpContext context)
    {
        try
        {
            await _next(context);
        }
        catch (OperationCanceledException) when (context.RequestAborted.IsCancellationRequested)
        {
            // The caller is gone. Do not turn a routine disconnect into a
            // server failure or attempt to write through a canceled request.
        }
        catch (OperationCanceledException ex)
        {
            await HandleErrorAsync(
                context,
                ex,
                HttpStatusCode.RequestTimeout,
                SafeErrorKind.OperationCanceled,
                unexpected: false);
        }
        catch (TimeoutException ex)
        {
            await HandleErrorAsync(
                context,
                ex,
                HttpStatusCode.GatewayTimeout,
                SafeErrorKind.TimedOut,
                unexpected: false);
        }
        catch (BadHttpRequestException ex)
        {
            await HandleErrorAsync(
                context,
                ex,
                (HttpStatusCode)ex.StatusCode,
                SafeErrorKind.InvalidHttpRequest,
                unexpected: false);
        }
        catch (ArgumentException ex)
        {
            await HandleErrorAsync(
                context,
                ex,
                HttpStatusCode.BadRequest,
                SafeErrorKind.InvalidArgument,
                unexpected: false);
        }
        catch (CSharpDbClientConfigurationException ex)
        {
            await HandleErrorAsync(
                context,
                ex,
                HttpStatusCode.BadRequest,
                SafeErrorKind.ClientConfiguration,
                unexpected: false);
        }
        catch (CSharpDbException ex)
        {
            SafeApiErrorDescriptor descriptor = SafeApiErrorPolicy.For(ex.Code);
            await HandleErrorAsync(
                context,
                ex,
                descriptor.Status,
                descriptor.Kind,
                descriptor.IsUnexpected);
        }
        catch (CSharpDbClientException ex)
        {
            await HandleErrorAsync(
                context,
                ex,
                HttpStatusCode.InternalServerError,
                SafeErrorKind.ClientTransport,
                unexpected: true);
        }
        catch (Exception ex)
        {
            await HandleErrorAsync(
                context,
                ex,
                HttpStatusCode.InternalServerError,
                SafeErrorKind.Unexpected,
                unexpected: true);
        }
    }

    private async Task HandleErrorAsync(
        HttpContext context,
        Exception exception,
        HttpStatusCode status,
        SafeErrorKind errorKind,
        bool unexpected)
    {
        SafeErrorProjection safeError = SafeErrorProjector.Project(exception, errorKind);
        (string traceId, DiagnosticsTraceId? diagnosticTraceId) = GetTraceCorrelation();
        CSharpDbLogEventDefinition<CSharpDbApiErrorEvent> definition =
            PublishApiError(unexpected, safeError, diagnosticTraceId);
        if (!_typedLoggerBridgeAvailable)
            LogApiErrorFallback(definition, safeError, traceId, unexpected);

        if (context.Response.HasStarted)
            ExceptionDispatchInfo.Capture(exception).Throw();

        await WriteErrorResponse(context, status, safeError, traceId);
    }

    private static async Task WriteErrorResponse(
        HttpContext context,
        HttpStatusCode status,
        SafeErrorProjection safeError,
        string traceId)
    {
        context.Response.Clear();
        context.Response.StatusCode = (int)status;
        context.Response.ContentType = "application/problem+json";

        var problem = new ProblemDetails
        {
            Status = (int)status,
            Title = status.ToString(),
            Detail = safeError.PublicDetail,
        };
        problem.Extensions["errorCode"] = safeError.Code;
        problem.Extensions["errorType"] = safeError.ErrorType;
        problem.Extensions["traceId"] = traceId;

        await context.Response.WriteAsJsonAsync(
            problem,
            options: null,
            contentType: "application/problem+json",
            cancellationToken: CancellationToken.None);
    }

    private static CSharpDbLogEventDefinition<CSharpDbApiErrorEvent> PublishApiError(
        bool unexpected,
        SafeErrorProjection safeError,
        DiagnosticsTraceId? traceId)
    {
        CSharpDbLogEventDefinition<CSharpDbApiErrorEvent> definition = unexpected
            ? CSharpDbLogEvents.ApiUnhandledError
            : CSharpDbLogEvents.ApiRequestRejected;
        ObservabilityTransport transport = CSharpDbOperationScope.CurrentTransport;
        if (transport is not (ObservabilityTransport.Http or ObservabilityTransport.Grpc))
            transport = ObservabilityTransport.Http;

        CSharpDbDiagnostics.EventPublisher.Publish(
            definition,
            () => new CSharpDbApiErrorEvent(
                DateTimeOffset.UtcNow,
                transport,
                traceId,
                safeError));
        return definition;
    }

    private void LogApiErrorFallback(
        CSharpDbLogEventDefinition<CSharpDbApiErrorEvent> definition,
        SafeErrorProjection safeError,
        string traceId,
        bool unexpected)
    {
        try
        {
            _logger.Log(
                unexpected ? LogLevel.Error : LogLevel.Warning,
                new EventId(definition.EventId, definition.Name),
                definition.MessageTemplate,
                safeError.Code,
                safeError.ErrorType,
                traceId);
        }
        catch
        {
            // The compatibility fallback has the same no-throw contract as
            // typed DiagnosticListener delivery.
        }
    }

    private static (string ResponseTraceId, DiagnosticsTraceId? DiagnosticTraceId)
        GetTraceCorrelation()
    {
        if (Activity.Current is { } activity && activity.TraceId != default)
        {
            DiagnosticsTraceId traceId =
                DiagnosticsTraceId.FromActivityTraceId(activity.TraceId);
            return (traceId.Value, traceId);
        }

        var fallback = new DiagnosticsTraceId(
            CSharpDbDiagnostics.CreateOpaqueIdentifier());
        return (fallback.Value, fallback);
    }
}
