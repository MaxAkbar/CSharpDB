namespace CSharpDB.Migration.SqlServer;

internal enum SqlServerMigrationErrorCode
{
    General,
    InspectionLimit,
}

/// <summary>
/// A SQL Server analysis error whose public message is safe to present to
/// callers. Provider messages, SQL text, endpoints, database names, and
/// credentials are deliberately excluded from <see cref="Exception.Message"/>.
/// Provider exceptions are not retained because <see cref="Exception.ToString"/>
/// is also part of the public error surface.
/// </summary>
public class SqlServerMigrationException : Exception
{
    public SqlServerMigrationException(string message)
        : base(message)
    {
    }

    internal SqlServerMigrationException(
        string message,
        SqlServerMigrationErrorCode errorCode)
        : base(message)
    {
        ErrorCode = errorCode;
    }

    internal SqlServerMigrationErrorCode ErrorCode { get; } =
        SqlServerMigrationErrorCode.General;
}

/// <summary>
/// A retained-capture failure caused by a configured or fixed safety bound.
/// The message remains provider-neutral and contains no source value,
/// identifier, endpoint, SQL text, or connection material.
/// </summary>
public sealed class SqlServerRetainedCaptureLimitException :
    SqlServerMigrationException
{
    public SqlServerRetainedCaptureLimitException(string message)
        : base(message)
    {
    }
}
