using System.Diagnostics;
using System.Net;
using System.Runtime.ExceptionServices;
using CSharpDB.Client;
using CSharpDB.Observability;
using CSharpDB.Primitives;
using Microsoft.AspNetCore.Mvc;

namespace CSharpDB.Api.Middleware;

public sealed class ExceptionHandlingMiddleware
{
    private readonly RequestDelegate _next;
    private readonly ILogger<ExceptionHandlingMiddleware> _logger;

    public ExceptionHandlingMiddleware(RequestDelegate next, ILogger<ExceptionHandlingMiddleware> logger)
    {
        _next = next;
        _logger = logger;
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
        string traceId = GetTraceId();
        int eventId = unexpected
            ? CSharpDbLogEventIds.ApiUnhandledError
            : CSharpDbLogEventIds.ApiRequestRejected;

        if (unexpected)
        {
            _logger.LogError(
                new EventId(eventId, nameof(CSharpDbLogEventIds.ApiUnhandledError)),
                "CSharpDB API request failed with {ErrorCode} ({ErrorType}); trace {TraceId}",
                safeError.Code,
                safeError.ErrorType,
                traceId);
        }
        else
        {
            _logger.LogWarning(
                new EventId(eventId, nameof(CSharpDbLogEventIds.ApiRequestRejected)),
                "CSharpDB API request was rejected with {ErrorCode} ({ErrorType}); trace {TraceId}",
                safeError.Code,
                safeError.ErrorType,
                traceId);
        }

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

    private static string GetTraceId()
    {
        if (Activity.Current is { } activity && activity.TraceId != default)
            return activity.TraceId.ToHexString();

        return CSharpDbDiagnostics.CreateOpaqueIdentifier();
    }
}
