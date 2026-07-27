namespace CSharpDB.Migration.Access;

public enum AccessMigrationErrorCode
{
    UnsupportedPlatform,
    ProviderUnavailable,
    InvalidSource,
    SourceLeaseUnavailable,
    InspectionLimit,
    CatalogReadFailed,
    CaptureFailed,
    InvalidRetainedPackage,
}

/// <summary>
/// A value-free Microsoft Access migration error. Provider messages, local
/// paths, source values, and connection details are deliberately excluded
/// from the public exception graph.
/// </summary>
public class AccessMigrationException : Exception
{
    public AccessMigrationException(
        AccessMigrationErrorCode errorCode,
        string message)
        : base(message)
    {
        ErrorCode = errorCode;
    }

    public AccessMigrationErrorCode ErrorCode { get; }
}

public sealed class AccessRetainedCaptureLimitException
    : AccessMigrationException
{
    public AccessRetainedCaptureLimitException(string message)
        : base(
            AccessMigrationErrorCode.InspectionLimit,
            message)
    {
    }

}
