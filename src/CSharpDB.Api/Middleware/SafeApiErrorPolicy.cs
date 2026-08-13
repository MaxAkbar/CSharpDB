using System.Net;
using CSharpDB.Observability;
using CSharpDB.Primitives;

namespace CSharpDB.Api.Middleware;

internal readonly record struct SafeApiErrorDescriptor(
    HttpStatusCode Status,
    SafeErrorKind Kind,
    bool IsUnexpected);

internal static class SafeApiErrorPolicy
{
    internal static SafeApiErrorDescriptor For(ErrorCode code) => code switch
    {
        ErrorCode.TableNotFound or
        ErrorCode.ColumnNotFound or
        ErrorCode.TriggerNotFound =>
            Expected(HttpStatusCode.NotFound, SafeErrorKind.DatabaseNotFound),

        ErrorCode.TableAlreadyExists or
        ErrorCode.TriggerAlreadyExists =>
            Expected(HttpStatusCode.Conflict, SafeErrorKind.DatabaseAlreadyExists),

        ErrorCode.DuplicateKey or
        ErrorCode.TransactionConflict =>
            Expected(HttpStatusCode.Conflict, SafeErrorKind.DatabaseConflict),

        ErrorCode.ConstraintViolation =>
            Expected(HttpStatusCode.UnprocessableEntity, SafeErrorKind.DatabaseConstraint),
        ErrorCode.SyntaxError =>
            Expected(HttpStatusCode.BadRequest, SafeErrorKind.DatabaseSyntax),
        ErrorCode.TypeMismatch =>
            Expected(HttpStatusCode.BadRequest, SafeErrorKind.DatabaseTypeMismatch),
        ErrorCode.Busy =>
            Expected(HttpStatusCode.ServiceUnavailable, SafeErrorKind.DatabaseBusy),
        ErrorCode.ResourceLimitExceeded =>
            Expected(HttpStatusCode.RequestEntityTooLarge, SafeErrorKind.DatabaseResourceLimit),

        ErrorCode.CorruptDatabase =>
            Unexpected(SafeErrorKind.DatabaseCorrupt),
        ErrorCode.IoError or
        ErrorCode.JournalError or
        ErrorCode.WalError =>
            Unexpected(SafeErrorKind.DatabaseIo),
        _ => Unexpected(SafeErrorKind.DatabaseOperation),
    };

    internal static SafeErrorProjection ProjectResult(ErrorCode? code)
        => SafeErrorProjector.Project(
            code is { } value
                ? For(value).Kind
                : SafeErrorKind.DatabaseOperation);

    private static SafeApiErrorDescriptor Expected(HttpStatusCode status, SafeErrorKind kind)
        => new(status, kind, IsUnexpected: false);

    private static SafeApiErrorDescriptor Unexpected(SafeErrorKind kind)
        => new(HttpStatusCode.InternalServerError, kind, IsUnexpected: true);
}
