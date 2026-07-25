namespace CSharpDB.Migration.MySql;

/// <summary>
/// A MySQL analysis error whose public message omits provider messages, SQL,
/// endpoints, database names, and credentials.
/// </summary>
public sealed class MySqlMigrationException : Exception
{
    public MySqlMigrationException(string message)
        : base(message)
    {
    }
}
