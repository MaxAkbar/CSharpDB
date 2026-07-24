namespace CSharpDB.Migration.LiteDb;

/// <summary>
/// A stable adapter-level failure raised while LiteDB metadata or BSON values
/// are being inspected.
/// </summary>
public sealed class LiteDbMigrationException : Exception
{
    public LiteDbMigrationException(string message)
        : base(message)
    {
    }

    public LiteDbMigrationException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
