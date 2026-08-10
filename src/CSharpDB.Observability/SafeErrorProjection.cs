using System.Text.Json.Serialization;

namespace CSharpDB.Observability;

public enum SafeErrorKind
{
    Unexpected = 0,
    InvalidHttpRequest,
    InvalidArgument,
    ClientConfiguration,
    ClientTransport,
    OperationCanceled,
    TimedOut,
    AccessDenied,
    DatabaseNotFound,
    DatabaseAlreadyExists,
    DatabaseConflict,
    DatabaseConstraint,
    DatabaseSyntax,
    DatabaseTypeMismatch,
    DatabaseBusy,
    DatabaseResourceLimit,
    DatabaseCorrupt,
    DatabaseIo,
    DatabaseOperation,
}

/// <summary>
/// A reviewed error projection that never contains an exception message,
/// stack trace, exception data, SQL, values, credentials, or paths.
/// </summary>
public sealed record SafeErrorProjection
{
    [JsonConstructor]
    public SafeErrorProjection(string code, string errorType, string publicDetail)
    {
        if (!SafeErrorProjector.IsApprovedProjection(code, errorType, publicDetail))
        {
            throw new ArgumentException(
                "The error projection must use a reviewed code, type, and public detail.");
        }

        Code = code;
        ErrorType = errorType;
        PublicDetail = publicDetail;
    }

    public string Code { get; }
    public string ErrorType { get; }
    public string PublicDetail { get; }
}

public static class SafeErrorProjector
{
    public static SafeErrorProjection Project(Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);

        SafeErrorKind kind = exception switch
        {
            OperationCanceledException => SafeErrorKind.OperationCanceled,
            TimeoutException => SafeErrorKind.TimedOut,
            ArgumentException => SafeErrorKind.InvalidArgument,
            UnauthorizedAccessException => SafeErrorKind.AccessDenied,
            IOException => SafeErrorKind.DatabaseIo,
            _ => SafeErrorKind.Unexpected,
        };

        return Project(exception, kind);
    }

    public static SafeErrorProjection Project(Exception exception, SafeErrorKind kind)
    {
        ArgumentNullException.ThrowIfNull(exception);

        return Project(kind);
    }

    public static SafeErrorProjection Project(SafeErrorKind kind)
    {
        (string code, string errorType, string detail) = kind switch
        {
            SafeErrorKind.InvalidHttpRequest =>
                ("invalid_http_request", "invalid_http_request", "The HTTP request is invalid."),
            SafeErrorKind.InvalidArgument =>
                ("invalid_argument", "invalid_argument", "The request is invalid."),
            SafeErrorKind.ClientConfiguration =>
                ("client_configuration", "client_configuration", "The client configuration is invalid."),
            SafeErrorKind.ClientTransport =>
                ("client_transport", "client_transport", "The database client could not complete the request."),
            SafeErrorKind.OperationCanceled =>
                ("operation_canceled", "operation_canceled", "The operation was canceled."),
            SafeErrorKind.TimedOut =>
                ("operation_timed_out", "operation_timed_out", "The operation timed out."),
            SafeErrorKind.AccessDenied =>
                ("access_denied", "access_denied", "Access was denied."),
            SafeErrorKind.DatabaseNotFound =>
                ("csharpdb.not_found", "database_not_found", "The requested database object was not found."),
            SafeErrorKind.DatabaseAlreadyExists =>
                ("csharpdb.already_exists", "database_already_exists", "The database object already exists."),
            SafeErrorKind.DatabaseConflict =>
                ("csharpdb.conflict", "database_conflict", "The request conflicts with existing database state."),
            SafeErrorKind.DatabaseConstraint =>
                ("csharpdb.constraint", "database_constraint", "The request violates a database constraint."),
            SafeErrorKind.DatabaseSyntax =>
                ("csharpdb.syntax", "database_syntax", "The SQL request is invalid."),
            SafeErrorKind.DatabaseTypeMismatch =>
                ("csharpdb.type_mismatch", "database_type_mismatch", "The request contains an incompatible value type."),
            SafeErrorKind.DatabaseBusy =>
                ("csharpdb.busy", "database_busy", "The database is temporarily busy."),
            SafeErrorKind.DatabaseResourceLimit =>
                ("csharpdb.resource_limit", "database_resource_limit", "The request exceeded a configured resource limit."),
            SafeErrorKind.DatabaseCorrupt =>
                ("csharpdb.corrupt", "database_corrupt", "The database could not safely complete the request."),
            SafeErrorKind.DatabaseIo =>
                ("csharpdb.io", "database_io", "The database could not complete the storage operation."),
            SafeErrorKind.DatabaseOperation =>
                ("csharpdb.operation_failed", "database_operation", "The database could not complete the request."),
            _ =>
                ("unexpected_error", "unexpected", "An unexpected error occurred."),
        };

        return new SafeErrorProjection(code, errorType, detail);
    }

    internal static bool IsApprovedProjection(
        string? code,
        string? errorType,
        string? publicDetail)
    {
        if (code is null || errorType is null || publicDetail is null)
            return false;

        return code switch
        {
            "invalid_http_request" => Matches(
                errorType,
                publicDetail,
                "invalid_http_request",
                "The HTTP request is invalid."),
            "invalid_argument" => Matches(
                errorType,
                publicDetail,
                "invalid_argument",
                "The request is invalid."),
            "client_configuration" => Matches(
                errorType,
                publicDetail,
                "client_configuration",
                "The client configuration is invalid."),
            "client_transport" => Matches(
                errorType,
                publicDetail,
                "client_transport",
                "The database client could not complete the request."),
            "operation_canceled" => Matches(
                errorType,
                publicDetail,
                "operation_canceled",
                "The operation was canceled."),
            "operation_timed_out" => Matches(
                errorType,
                publicDetail,
                "operation_timed_out",
                "The operation timed out."),
            "access_denied" => Matches(
                errorType,
                publicDetail,
                "access_denied",
                "Access was denied."),
            "csharpdb.not_found" => Matches(
                errorType,
                publicDetail,
                "database_not_found",
                "The requested database object was not found."),
            "csharpdb.already_exists" => Matches(
                errorType,
                publicDetail,
                "database_already_exists",
                "The database object already exists."),
            "csharpdb.conflict" => Matches(
                errorType,
                publicDetail,
                "database_conflict",
                "The request conflicts with existing database state."),
            "csharpdb.constraint" => Matches(
                errorType,
                publicDetail,
                "database_constraint",
                "The request violates a database constraint."),
            "csharpdb.syntax" => Matches(
                errorType,
                publicDetail,
                "database_syntax",
                "The SQL request is invalid."),
            "csharpdb.type_mismatch" => Matches(
                errorType,
                publicDetail,
                "database_type_mismatch",
                "The request contains an incompatible value type."),
            "csharpdb.busy" => Matches(
                errorType,
                publicDetail,
                "database_busy",
                "The database is temporarily busy."),
            "csharpdb.resource_limit" => Matches(
                errorType,
                publicDetail,
                "database_resource_limit",
                "The request exceeded a configured resource limit."),
            "csharpdb.corrupt" => Matches(
                errorType,
                publicDetail,
                "database_corrupt",
                "The database could not safely complete the request."),
            "csharpdb.io" => Matches(
                errorType,
                publicDetail,
                "database_io",
                "The database could not complete the storage operation."),
            "csharpdb.operation_failed" => Matches(
                errorType,
                publicDetail,
                "database_operation",
                "The database could not complete the request."),
            "unexpected_error" => Matches(
                errorType,
                publicDetail,
                "unexpected",
                "An unexpected error occurred."),
            _ => false,
        };
    }

    private static bool Matches(
        string actualType,
        string actualDetail,
        string expectedType,
        string expectedDetail)
        => string.Equals(actualType, expectedType, StringComparison.Ordinal) &&
           string.Equals(actualDetail, expectedDetail, StringComparison.Ordinal);
}
