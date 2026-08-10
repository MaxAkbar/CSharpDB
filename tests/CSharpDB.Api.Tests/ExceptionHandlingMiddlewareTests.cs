using System.Diagnostics;
using System.Net;
using System.Text.Json;
using CSharpDB.Api.Middleware;
using CSharpDB.Client;
using CSharpDB.Observability;
using CSharpDB.Primitives;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.Extensions.Logging;

namespace CSharpDB.Api.Tests;

public sealed class ExceptionHandlingMiddlewareTests
{
    private const string SafeTraceId = "0123456789abcdef0123456789abcdef";
    private const string TraceIdentifierCanary =
        "BearerCapabilitySecretCustomer42DatabaseCredential";
    private const string Secret =
        "Password=CanarySecret;Data Source=C:\\private\\database.db;SELECT 'CanarySecret'";

    [Theory]
    [InlineData(ErrorCase.BadHttpRequest, HttpStatusCode.BadRequest, "invalid_http_request", "invalid_http_request")]
    [InlineData(ErrorCase.Argument, HttpStatusCode.BadRequest, "invalid_argument", "invalid_argument")]
    [InlineData(ErrorCase.ClientConfiguration, HttpStatusCode.BadRequest, "client_configuration", "client_configuration")]
    [InlineData(ErrorCase.DatabaseNotFound, HttpStatusCode.NotFound, "csharpdb.not_found", "database_not_found")]
    [InlineData(ErrorCase.DatabaseResourceLimit, HttpStatusCode.RequestEntityTooLarge, "csharpdb.resource_limit", "database_resource_limit")]
    [InlineData(ErrorCase.ClientTransport, HttpStatusCode.InternalServerError, "client_transport", "client_transport")]
    [InlineData(ErrorCase.Unexpected, HttpStatusCode.InternalServerError, "unexpected_error", "unexpected")]
    public async Task Middleware_ProjectsSafeProblemAndStructuredLog(
        ErrorCase errorCase,
        HttpStatusCode expectedStatus,
        string expectedCode,
        string expectedType)
    {
        Exception exception = CreateException(errorCase);
        var logger = new CapturingLogger<ExceptionHandlingMiddleware>();
        var middleware = new ExceptionHandlingMiddleware(
            _ => Task.FromException(exception),
            logger);
        var context = new DefaultHttpContext
        {
            TraceIdentifier = SafeTraceId,
        };
        context.Response.Body = new MemoryStream();
        context.Response.ContentLength = 1_024;
        context.Response.Headers["X-Downstream-Error"] = Secret;

        await middleware.InvokeAsync(context);

        context.Response.Body.Position = 0;
        using JsonDocument problem = await JsonDocument.ParseAsync(
            context.Response.Body,
            cancellationToken: TestContext.Current.CancellationToken);
        CapturedLog log = Assert.Single(logger.Entries);
        string payload = problem.RootElement.GetRawText();

        Assert.Equal((int)expectedStatus, context.Response.StatusCode);
        Assert.Equal("application/problem+json", context.Response.ContentType);
        Assert.Null(context.Response.ContentLength);
        Assert.False(context.Response.Headers.ContainsKey("X-Downstream-Error"));
        Assert.Equal(expectedCode, problem.RootElement.GetProperty("errorCode").GetString());
        Assert.Equal(expectedType, problem.RootElement.GetProperty("errorType").GetString());
        string traceId = Assert.IsType<string>(
            problem.RootElement.GetProperty("traceId").GetString());
        Assert.True(CSharpDbDiagnostics.IsValidOpaqueIdentifier(traceId));
        Assert.NotEqual(SafeTraceId, traceId);
        Assert.Contains(traceId, log.Message, StringComparison.Ordinal);
        Assert.Null(log.Exception);
        Assert.DoesNotContain("CanarySecret", payload, StringComparison.Ordinal);
        Assert.DoesNotContain("private", payload, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("SELECT", payload, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("CanarySecret", log.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("private", log.Message, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("SELECT", log.Message, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(nameof(Customer42ApiKeyCanaryException), payload, StringComparison.Ordinal);
        Assert.DoesNotContain(nameof(Customer42ApiKeyCanaryException), log.Message, StringComparison.Ordinal);
    }

    public static IEnumerable<object[]> DatabaseErrorCases()
    {
        yield return DbCase(ErrorCode.Unknown, HttpStatusCode.InternalServerError, "csharpdb.operation_failed", LogLevel.Error);
        yield return DbCase(ErrorCode.IoError, HttpStatusCode.InternalServerError, "csharpdb.io", LogLevel.Error);
        yield return DbCase(ErrorCode.CorruptDatabase, HttpStatusCode.InternalServerError, "csharpdb.corrupt", LogLevel.Error);
        yield return DbCase(ErrorCode.TableNotFound, HttpStatusCode.NotFound, "csharpdb.not_found", LogLevel.Warning);
        yield return DbCase(ErrorCode.TableAlreadyExists, HttpStatusCode.Conflict, "csharpdb.already_exists", LogLevel.Warning);
        yield return DbCase(ErrorCode.ColumnNotFound, HttpStatusCode.NotFound, "csharpdb.not_found", LogLevel.Warning);
        yield return DbCase(ErrorCode.TypeMismatch, HttpStatusCode.BadRequest, "csharpdb.type_mismatch", LogLevel.Warning);
        yield return DbCase(ErrorCode.SyntaxError, HttpStatusCode.BadRequest, "csharpdb.syntax", LogLevel.Warning);
        yield return DbCase(ErrorCode.ConstraintViolation, HttpStatusCode.UnprocessableEntity, "csharpdb.constraint", LogLevel.Warning);
        yield return DbCase(ErrorCode.JournalError, HttpStatusCode.InternalServerError, "csharpdb.io", LogLevel.Error);
        yield return DbCase(ErrorCode.DuplicateKey, HttpStatusCode.Conflict, "csharpdb.conflict", LogLevel.Warning);
        yield return DbCase(ErrorCode.TriggerNotFound, HttpStatusCode.NotFound, "csharpdb.not_found", LogLevel.Warning);
        yield return DbCase(ErrorCode.TriggerAlreadyExists, HttpStatusCode.Conflict, "csharpdb.already_exists", LogLevel.Warning);
        yield return DbCase(ErrorCode.WalError, HttpStatusCode.InternalServerError, "csharpdb.io", LogLevel.Error);
        yield return DbCase(ErrorCode.Busy, HttpStatusCode.ServiceUnavailable, "csharpdb.busy", LogLevel.Warning);
        yield return DbCase(ErrorCode.TransactionConflict, HttpStatusCode.Conflict, "csharpdb.conflict", LogLevel.Warning);
        yield return DbCase(ErrorCode.ResourceLimitExceeded, HttpStatusCode.RequestEntityTooLarge, "csharpdb.resource_limit", LogLevel.Warning);
    }

    [Theory]
    [MemberData(nameof(DatabaseErrorCases))]
    public async Task Middleware_MapsEveryDatabaseCodeConsistently(
        ErrorCode code,
        HttpStatusCode expectedStatus,
        string expectedSafeCode,
        LogLevel expectedLevel)
    {
        var logger = new CapturingLogger<ExceptionHandlingMiddleware>();
        var middleware = new ExceptionHandlingMiddleware(
            _ => Task.FromException(new CSharpDbException(code, Secret)),
            logger);
        var context = CreateContext();

        await middleware.InvokeAsync(context);

        using JsonDocument problem = await ReadProblemAsync(context);
        CapturedLog log = Assert.Single(logger.Entries);
        Assert.Equal((int)expectedStatus, context.Response.StatusCode);
        Assert.Equal(expectedSafeCode, problem.RootElement.GetProperty("errorCode").GetString());
        Assert.Equal(expectedLevel, log.Level);
        Assert.Equal(
            expectedLevel == LogLevel.Error
                ? CSharpDB.Observability.CSharpDbLogEventIds.ApiUnhandledError
                : CSharpDB.Observability.CSharpDbLogEventIds.ApiRequestRejected,
            log.EventId.Id);
        Assert.Null(log.Exception);
    }

    [Fact]
    public async Task Middleware_UsesActivityTraceIdForResponseAndLogCorrelation()
    {
        using var activity = new Activity("api-test");
        activity.SetIdFormat(ActivityIdFormat.W3C);
        activity.Start();
        var logger = new CapturingLogger<ExceptionHandlingMiddleware>();
        var middleware = new ExceptionHandlingMiddleware(
            _ => Task.FromException(new InvalidOperationException(Secret)),
            logger);
        var context = CreateContext();

        await middleware.InvokeAsync(context);

        using JsonDocument problem = await ReadProblemAsync(context);
        string traceId = activity.TraceId.ToHexString();
        Assert.Equal(traceId, problem.RootElement.GetProperty("traceId").GetString());
        Assert.Contains(traceId, Assert.Single(logger.Entries).Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Middleware_ReplacesUnsafeTraceIdentifierWithOpaqueFallback()
    {
        var logger = new CapturingLogger<ExceptionHandlingMiddleware>();
        var middleware = new ExceptionHandlingMiddleware(
            _ => Task.FromException(new InvalidOperationException(Secret)),
            logger);
        var context = CreateContext();
        context.TraceIdentifier = TraceIdentifierCanary;

        await middleware.InvokeAsync(context);

        using JsonDocument problem = await ReadProblemAsync(context);
        string traceId = Assert.IsType<string>(
            problem.RootElement.GetProperty("traceId").GetString());
        CapturedLog log = Assert.Single(logger.Entries);
        string payload = problem.RootElement.GetRawText();

        Assert.True(CSharpDbDiagnostics.IsValidOpaqueIdentifier(traceId));
        Assert.Contains(traceId, log.Message, StringComparison.Ordinal);
        Assert.DoesNotContain(TraceIdentifierCanary, payload, StringComparison.Ordinal);
        Assert.DoesNotContain(TraceIdentifierCanary, log.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Middleware_DoesNotLogOrWriteAfterClientCancellation()
    {
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        var logger = new CapturingLogger<ExceptionHandlingMiddleware>();
        var middleware = new ExceptionHandlingMiddleware(
            _ => Task.FromException(new OperationCanceledException(cancellation.Token)),
            logger);
        var context = CreateContext();
        context.RequestAborted = cancellation.Token;

        await middleware.InvokeAsync(context);

        Assert.Empty(logger.Entries);
        Assert.Equal(0, context.Response.Body.Length);
    }

    [Theory]
    [InlineData(false, HttpStatusCode.RequestTimeout, "operation_canceled")]
    [InlineData(true, HttpStatusCode.GatewayTimeout, "operation_timed_out")]
    public async Task Middleware_ProjectsServerCancellationAndTimeout(
        bool timeout,
        HttpStatusCode expectedStatus,
        string expectedCode)
    {
        Exception exception = timeout ? new TimeoutException(Secret) : new OperationCanceledException(Secret);
        var middleware = new ExceptionHandlingMiddleware(
            _ => Task.FromException(exception),
            new CapturingLogger<ExceptionHandlingMiddleware>());
        var context = CreateContext();

        await middleware.InvokeAsync(context);

        using JsonDocument problem = await ReadProblemAsync(context);
        Assert.Equal((int)expectedStatus, context.Response.StatusCode);
        Assert.Equal(expectedCode, problem.RootElement.GetProperty("errorCode").GetString());
    }

    [Fact]
    public async Task Middleware_RethrowsOriginalFailureWhenResponseHasStarted()
    {
        var expected = new InvalidOperationException(Secret);
        var logger = new CapturingLogger<ExceptionHandlingMiddleware>();
        var middleware = new ExceptionHandlingMiddleware(
            _ => Task.FromException(expected),
            logger);
        var context = new DefaultHttpContext();
        context.Features.Set<IHttpResponseFeature>(new StartedResponseFeature());

        Exception actual = await Assert.ThrowsAsync<InvalidOperationException>(
            () => middleware.InvokeAsync(context));

        Assert.Same(expected, actual);
        Assert.Single(logger.Entries);
    }

    private static object[] DbCase(
        ErrorCode code,
        HttpStatusCode status,
        string safeCode,
        LogLevel level)
        => [code, status, safeCode, level];

    private static DefaultHttpContext CreateContext()
    {
        var context = new DefaultHttpContext
        {
            TraceIdentifier = SafeTraceId,
        };
        context.Response.Body = new MemoryStream();
        return context;
    }

    private static async Task<JsonDocument> ReadProblemAsync(DefaultHttpContext context)
    {
        context.Response.Body.Position = 0;
        return await JsonDocument.ParseAsync(
            context.Response.Body,
            cancellationToken: TestContext.Current.CancellationToken);
    }

    private static Exception CreateException(ErrorCase errorCase) => errorCase switch
    {
        ErrorCase.BadHttpRequest => new BadHttpRequestException(Secret, StatusCodes.Status400BadRequest),
        ErrorCase.Argument => new ArgumentException(Secret),
        ErrorCase.ClientConfiguration => new CSharpDbClientConfigurationException(Secret),
        ErrorCase.DatabaseNotFound => new CSharpDbException(ErrorCode.TableNotFound, Secret),
        ErrorCase.DatabaseResourceLimit => new CSharpDbException(ErrorCode.ResourceLimitExceeded, Secret),
        ErrorCase.ClientTransport => new CSharpDbClientException(Secret),
        _ => CreateCanaryException(),
    };

    private static Exception CreateCanaryException()
    {
        var exception = new Customer42ApiKeyCanaryException(
            Secret,
            new IOException("C:\\private\\inner-secret.db"));
        exception.Data["sql"] = "SELECT 'CanarySecret'";
        return exception;
    }

    public enum ErrorCase
    {
        BadHttpRequest,
        Argument,
        ClientConfiguration,
        DatabaseNotFound,
        DatabaseResourceLimit,
        ClientTransport,
        Unexpected,
    }

    private sealed class CapturingLogger<T> : ILogger<T>
    {
        public List<CapturedLog> Entries { get; } = [];

        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull
            => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
            => Entries.Add(new CapturedLog(
                logLevel,
                eventId,
                formatter(state, exception),
                exception));
    }

    private sealed record CapturedLog(
        LogLevel Level,
        EventId EventId,
        string Message,
        Exception? Exception);

    private sealed class Customer42ApiKeyCanaryException(
        string message,
        Exception innerException)
        : Exception(message, innerException);

    private sealed class StartedResponseFeature : IHttpResponseFeature
    {
        public int StatusCode { get; set; } = StatusCodes.Status200OK;
        public string? ReasonPhrase { get; set; }
        public IHeaderDictionary Headers { get; set; } = new HeaderDictionary();
        public Stream Body { get; set; } = new MemoryStream();
        public bool HasStarted => true;

        public void OnStarting(Func<object, Task> callback, object state)
        {
        }

        public void OnCompleted(Func<object, Task> callback, object state)
        {
        }
    }
}
