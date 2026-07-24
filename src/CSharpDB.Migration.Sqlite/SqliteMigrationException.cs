namespace CSharpDB.Migration.Sqlite;

/// <summary>
/// A value-free SQLite migration error that is safe to present to callers.
/// Provider messages, SQL text, and local paths are deliberately excluded.
/// </summary>
public sealed class SqliteMigrationException : Exception
{
    public SqliteMigrationException(string message)
        : base(message)
    {
    }

    public SqliteMigrationException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
