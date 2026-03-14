using System.Net;
using CSharpDB.Client;
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
        catch (ArgumentException ex)
        {
            _logger.LogWarning(ex, "Validation error");
            await WriteErrorResponse(context, HttpStatusCode.BadRequest, ex.Message);
        }
        catch (CSharpDbClientConfigurationException ex)
        {
            _logger.LogWarning(ex, "Client configuration error");
            await WriteErrorResponse(context, HttpStatusCode.BadRequest, ex.Message);
        }
        catch (CSharpDbException ex)
        {
            _logger.LogWarning(ex, "Database error: {ErrorCode}", ex.Code);
            await WriteErrorResponse(context, MapErrorCode(ex.Code), ex.Message);
        }
        catch (CSharpDbClientException ex)
        {
            _logger.LogError(ex, "Client error");
            await WriteErrorResponse(context, HttpStatusCode.InternalServerError, ex.Message);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Unhandled exception");
            var detail = context.RequestServices.GetService<IHostEnvironment>()?.IsDevelopment() == true
                ? $"{ex.GetType().Name}: {ex.Message}"
                : "An unexpected error occurred.";
            await WriteErrorResponse(context, HttpStatusCode.InternalServerError, detail);
        }
    }

    private static HttpStatusCode MapErrorCode(ErrorCode code) => code switch
    {
        ErrorCode.TableNotFound => HttpStatusCode.NotFound,
        ErrorCode.ColumnNotFound => HttpStatusCode.NotFound,
        ErrorCode.TriggerNotFound => HttpStatusCode.NotFound,
        ErrorCode.TableAlreadyExists => HttpStatusCode.Conflict,
        ErrorCode.TriggerAlreadyExists => HttpStatusCode.Conflict,
        ErrorCode.DuplicateKey => HttpStatusCode.Conflict,
        ErrorCode.ConstraintViolation => HttpStatusCode.UnprocessableEntity,
        ErrorCode.SyntaxError => HttpStatusCode.BadRequest,
        ErrorCode.TypeMismatch => HttpStatusCode.BadRequest,
        ErrorCode.Busy => HttpStatusCode.ServiceUnavailable,
        _ => HttpStatusCode.InternalServerError,
    };

    private static async Task WriteErrorResponse(HttpContext context, HttpStatusCode status, string detail)
    {
        context.Response.StatusCode = (int)status;
        context.Response.ContentType = "application/problem+json";

        var problem = new ProblemDetails
        {
            Status = (int)status,
            Title = status.ToString(),
            Detail = detail,
        };

        await context.Response.WriteAsJsonAsync(problem);
    }
}
