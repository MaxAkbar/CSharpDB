namespace CSharpDB.Migration.SqlServer;

/// <summary>
/// A SQL Server analysis error whose public message is safe to present to
/// callers. Provider messages, SQL text, endpoints, database names, and
/// credentials are deliberately excluded from <see cref="Exception.Message"/>.
/// Provider exceptions are not retained because <see cref="Exception.ToString"/>
/// is also part of the public error surface.
/// </summary>
public sealed class SqlServerMigrationException : Exception
{
    public SqlServerMigrationException(string message)
        : base(message)
    {
    }
}
