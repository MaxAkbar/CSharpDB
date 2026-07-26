namespace CSharpDB.Migration.MySql;

internal enum MySqlMigrationErrorCode
{
    General,
    InspectionLimit,
}

/// <summary>
/// A MySQL analysis error whose public message omits provider messages, SQL,
/// endpoints, database names, and credentials.
/// </summary>
public class MySqlMigrationException : Exception
{
    public MySqlMigrationException(string message)
        : base(message)
    {
    }

    internal MySqlMigrationException(
        string message,
        MySqlMigrationErrorCode errorCode)
        : base(message)
    {
        ErrorCode = errorCode;
    }

    internal MySqlMigrationErrorCode ErrorCode { get; } =
        MySqlMigrationErrorCode.General;
}

/// <summary>
/// A retained-capture failure caused by a configured or fixed safety bound.
/// Its message excludes source values, identifiers, SQL text, endpoints, and
/// connection material.
/// </summary>
public sealed class MySqlRetainedCaptureLimitException :
    MySqlMigrationException
{
    public MySqlRetainedCaptureLimitException(string message)
        : base(message)
    {
    }
}
