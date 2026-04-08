namespace CSharpDB.Primitives;

public enum ErrorCode
{
    Unknown = 0,
    IoError,
    CorruptDatabase,
    TableNotFound,
    TableAlreadyExists,
    ColumnNotFound,
    TypeMismatch,
    SyntaxError,
    ConstraintViolation,
    JournalError,
    DuplicateKey,
    TriggerNotFound,
    TriggerAlreadyExists,
    WalError,
    Busy,
    TransactionConflict,
}

public class CSharpDbException : Exception
{
    public ErrorCode Code { get; }

    public CSharpDbException(ErrorCode code, string message)
        : base(message)
    {
        Code = code;
    }

    public CSharpDbException(ErrorCode code, string message, Exception innerException)
        : base(message, innerException)
    {
        Code = code;
    }
}

public sealed class CSharpDbConflictException : CSharpDbException
{
    public CSharpDbConflictException(string message)
        : base(ErrorCode.TransactionConflict, message)
    {
    }

    public CSharpDbConflictException(string message, Exception innerException)
        : base(ErrorCode.TransactionConflict, message, innerException)
    {
    }
}
